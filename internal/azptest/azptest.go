// Package azptest is an in-process AZP server for the SDK's tests: it
// runs the real handshake and record layer (the server half kept in
// internal/azp) over TCP and WebSocket and hands every client message to
// a handler the test scripts, so faults that a real engine produces only
// by accident (a dropped answer, a GoAway, an expired credential, a
// "gone" subscription) can be produced on demand.
package azptest

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/azora-db/azora-go/internal/azp"
)

// Prologue is the handshake prologue of the realtime protocol.
const Prologue = "azora-rt/1"

// Handler receives every message a client sends after Hello, on the
// session's reader goroutine. Pings are answered before it is called
// (unless Server.NoPong is set).
type Handler func(s *Session, m azp.Msg)

// Server is a scripted AZP server.
type Server struct {
	// Key is the server's static key.
	Key azp.KeyPair
	// NoPong stops the automatic answer to client pings.
	NoPong atomic.Bool
	// Silent stops the server from reading: connections stay open but
	// nothing is answered (a black hole).
	Silent atomic.Bool

	ln  net.Listener
	ws  *httptest.Server
	ctx context.Context
	end context.CancelFunc
	wg  sync.WaitGroup

	mu       sync.Mutex
	handler  Handler
	hello    func(*azp.Hello) *azp.Error
	welcome  azp.Welcome
	sessions map[*Session]struct{}
	accepted int
	hellos   []azp.Hello
}

// Session is one client connection past Hello.
type Session struct {
	// Hello is what the client sent; N numbers sessions from 1 in the
	// order they were accepted.
	Hello azp.Hello
	N     int

	srv  *Server
	conn *azp.Conn
}

// New starts a server on a loopback port; it is closed when the test
// ends. The default handler ignores every message.
func New(t testing.TB) *Server {
	t.Helper()
	key, err := azp.GenerateKey(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{Key: key, ln: ln, sessions: map[*Session]struct{}{},
		welcome: azp.Welcome{SessionID: "test", HeartbeatMS: 15000, MaxMessage: 16 << 20, Principal: "key:test",
			Scopes: []string{"read", "write", "ddl"}, MaxSubscriptions: 100}}
	s.ctx, s.end = context.WithCancel(context.Background())
	s.ws = httptest.NewServer(http.HandlerFunc(s.serveWS))
	s.wg.Add(1)
	go s.accept()
	t.Cleanup(s.Close)
	return s
}

// Close stops the server and drops every connection.
func (s *Server) Close() {
	s.end()
	s.ln.Close()
	s.DropAll()
	s.ws.CloseClientConnections()
	s.ws.Close()
	s.wg.Wait()
}

// Addr returns the TCP address.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// WebSocketURL returns the ws:// URL of the realtime endpoint.
func (s *Server) WebSocketURL() string { return "ws" + strings.TrimPrefix(s.ws.URL, "http") + "/rt/v1" }

// ServerKey returns the public key in the form connection strings use.
func (s *Server) ServerKey() string { return azp.EncodePublicKey(s.Key.Public) }

// Fingerprint returns the public key's fingerprint.
func (s *Server) Fingerprint() string { return azp.Fingerprint(s.Key.Public) }

// URL returns a TCP connection string for database db, pinning the key.
func (s *Server) URL(db string) string {
	return "azora://" + s.Addr() + "/" + db + "?key=" + s.ServerKey()
}

// Handle sets the handler for messages after Hello.
func (s *Server) Handle(h Handler) {
	s.mu.Lock()
	s.handler = h
	s.mu.Unlock()
}

// OnHello sets a function that may refuse a Hello: a non-nil Error is
// sent instead of Welcome (and the connection closes if it is fatal).
func (s *Server) OnHello(f func(*azp.Hello) *azp.Error) {
	s.mu.Lock()
	s.hello = f
	s.mu.Unlock()
}

// SetHeartbeat sets the heartbeat interval announced in Welcome.
func (s *Server) SetHeartbeat(d time.Duration) {
	s.mu.Lock()
	s.welcome.HeartbeatMS = uint32(d.Milliseconds())
	s.mu.Unlock()
}

// Accepted returns how many sessions got past Hello so far.
func (s *Server) Accepted() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accepted
}

// Hellos returns every Hello received, accepted or not.
func (s *Server) Hellos() []azp.Hello {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]azp.Hello(nil), s.hellos...)
}

// Sessions returns the live sessions.
func (s *Server) Sessions() []*Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Session, 0, len(s.sessions))
	for ss := range s.sessions {
		out = append(out, ss)
	}
	return out
}

// DropAll closes every live connection abruptly.
func (s *Server) DropAll() {
	for _, ss := range s.Sessions() {
		ss.Drop()
	}
}

// Broadcast sends m to every live session.
func (s *Server) Broadcast(m azp.Msg) {
	for _, ss := range s.Sessions() {
		ss.Send(m)
	}
}

func (s *Server) accept() {
	defer s.wg.Done()
	for {
		nc, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.serve(azp.NewStreamTransport(nc, 0))
		}()
	}
}

func (s *Server) serveWS(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"azp.v1"}})
	if err != nil {
		return
	}
	s.wg.Add(1)
	defer s.wg.Done()
	s.serve(azp.NewWSTransport(c, r.RemoteAddr, 0))
}

// serve runs one connection: handshake, Hello, then the handler loop.
func (s *Server) serve(t azp.Transport) {
	conn, err := azp.Server(s.ctx, t, azp.ServerConfig{Keys: azp.StaticKeys{Key: s.Key}, AllowPlaintext: true,
		Prologue: []byte(Prologue), HandshakeTimeout: 5 * time.Second})
	if err != nil {
		return
	}
	defer conn.Close()
	stop := context.AfterFunc(s.ctx, func() { conn.Close() })
	defer stop()
	raw, err := conn.Recv(s.ctx)
	if err != nil {
		return
	}
	m, err := azp.Decode(raw)
	hello, ok := m.(*azp.Hello)
	if err != nil || !ok {
		return
	}
	s.mu.Lock()
	s.hellos = append(s.hellos, *hello)
	check, welcome := s.hello, s.welcome
	s.mu.Unlock()
	ss := &Session{Hello: *hello, srv: s, conn: conn}
	if check != nil {
		if e := check(hello); e != nil {
			ss.Send(e)
			if e.Fatal {
				return
			}
		}
	}
	s.mu.Lock()
	s.accepted++
	ss.N = s.accepted
	s.sessions[ss] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.sessions, ss)
		s.mu.Unlock()
	}()
	welcome.ServerTimeMS = time.Now().UnixMilli()
	if ss.Send(&welcome) != nil {
		return
	}
	for {
		for s.Silent.Load() {
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
		raw, err := conn.Recv(s.ctx)
		if err != nil {
			return
		}
		if s.Silent.Load() {
			continue
		}
		m, err := azp.Decode(raw)
		if err != nil {
			ss.Send(&azp.Error{Code: azp.CodeInvalid, Message: "malformed message", Fatal: true})
			return
		}
		if p, ok := m.(*azp.Ping); ok && !s.NoPong.Load() {
			ss.Send(&azp.Pong{Nonce: p.Nonce})
		}
		s.mu.Lock()
		h := s.handler
		s.mu.Unlock()
		if h != nil {
			h(ss, m)
		}
	}
}

// Send sends m to the client.
func (ss *Session) Send(m azp.Msg) error {
	b, err := azp.Encode(m)
	if err != nil {
		return err
	}
	return ss.conn.Send(ss.srv.ctx, b)
}

// Drop closes the connection abruptly, as a network failure would.
func (ss *Session) Drop() { ss.conn.Close() }

// Mode returns the channel's security mode.
func (ss *Session) Mode() azp.Mode { return ss.conn.Mode() }

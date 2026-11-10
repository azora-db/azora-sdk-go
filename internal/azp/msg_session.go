package azp

// Hello opens a session (first message after the handshake).
type Hello struct {
	Version  uint16
	Database string
	// Credential is an API key or a client token.
	Credential string
	// Client names the SDK and its version ("azora-go/1.0").
	Client string
}

func (*Hello) Type() MsgType { return TypeHello }
func (m *Hello) encode(e *encoder) {
	e.u(uint64(m.Version))
	e.s(m.Database)
	e.s(m.Credential)
	e.s(m.Client)
}
func (m *Hello) decode(d *decoder) {
	v := d.u()
	if v > 0xffff {
		d.fail("version %d", v)
	}
	m.Version, m.Database, m.Credential, m.Client = uint16(v), d.s(), d.s(), d.s()
}

// Welcome accepts a session.
type Welcome struct {
	SessionID    string
	ServerTimeMS int64
	HeartbeatMS  uint32
	MaxMessage   uint32
	Principal    string   // who the credential identifies
	Scopes       []string // what it may do
	// MaxSubscriptions is this connection's subscription limit.
	MaxSubscriptions uint32
}

func (*Welcome) Type() MsgType { return TypeWelcome }
func (m *Welcome) encode(e *encoder) {
	e.s(m.SessionID)
	e.i(m.ServerTimeMS)
	e.u(uint64(m.HeartbeatMS))
	e.u(uint64(m.MaxMessage))
	e.s(m.Principal)
	e.strs(m.Scopes)
	e.u(uint64(m.MaxSubscriptions))
}
func (m *Welcome) decode(d *decoder) {
	m.SessionID, m.ServerTimeMS, m.HeartbeatMS, m.MaxMessage = d.s(), d.i(), d.u32(), d.u32()
	m.Principal, m.Scopes, m.MaxSubscriptions = d.s(), d.strs(), d.u32()
}

// Error codes.
const (
	CodeInvalid          = "invalid"
	CodeUnauthenticated  = "unauthenticated"
	CodePermissionDenied = "permission_denied"
	CodeNotFound         = "not_found"
	CodeResourceLimit    = "resource_exhausted"
	CodeGone             = "gone" // resync required
	CodeUnavailable      = "unavailable"
	CodeConflict         = "conflict"
	CodeInternal         = "internal"
	CodeCanceled         = "canceled"
)

// Error reports a failed request (ReqID > 0) or a session error (ReqID 0,
// Fatal: the server closes the connection after it).
type Error struct {
	ReqID        uint32
	Code         string
	Message      string
	Fatal        bool
	RetryAfterMS uint32
}

func (*Error) Type() MsgType { return TypeError }
func (m *Error) encode(e *encoder) {
	e.u(uint64(m.ReqID))
	e.s(m.Code)
	e.s(m.Message)
	e.flag(m.Fatal)
	e.u(uint64(m.RetryAfterMS))
}
func (m *Error) decode(d *decoder) {
	m.ReqID, m.Code, m.Message, m.Fatal, m.RetryAfterMS = d.u32(), d.s(), d.s(), d.flag(), d.u32()
}

// Error implements error, so a request's Error can be returned as one.
func (m *Error) Error() string { return m.Code + ": " + m.Message }

// Ping keeps the session alive and measures round trips; the peer answers
// with a Pong carrying the same Nonce.
type Ping struct{ Nonce uint64 }

func (*Ping) Type() MsgType       { return TypePing }
func (m *Ping) encode(e *encoder) { e.u(m.Nonce) }
func (m *Ping) decode(d *decoder) { m.Nonce = d.u() }

// Pong answers a Ping.
type Pong struct{ Nonce uint64 }

func (*Pong) Type() MsgType       { return TypePong }
func (m *Pong) encode(e *encoder) { e.u(m.Nonce) }
func (m *Pong) decode(d *decoder) { m.Nonce = d.u() }

// OK acknowledges a request that returns nothing.
type OK struct{ ReqID uint32 }

func (*OK) Type() MsgType       { return TypeOK }
func (m *OK) encode(e *encoder) { e.u(uint64(m.ReqID)) }
func (m *OK) decode(d *decoder) { m.ReqID = d.u32() }

// Auth replaces the session's credential (for example a refreshed token).
type Auth struct {
	ReqID      uint32
	Credential string
}

func (*Auth) Type() MsgType       { return TypeAuth }
func (m *Auth) encode(e *encoder) { e.u(uint64(m.ReqID)); e.s(m.Credential) }
func (m *Auth) decode(d *decoder) { m.ReqID, m.Credential = d.u32(), d.s() }

// GoAway tells the client the server is going away (shutdown, drain):
// reconnect after RetryAfterMS and resume.
type GoAway struct {
	Reason       string
	RetryAfterMS uint32
}

func (*GoAway) Type() MsgType       { return TypeGoAway }
func (m *GoAway) encode(e *encoder) { e.s(m.Reason); e.u(uint64(m.RetryAfterMS)) }
func (m *GoAway) decode(d *decoder) { m.Reason, m.RetryAfterMS = d.s(), d.u32() }

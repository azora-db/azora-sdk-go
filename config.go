package azora

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/azora-db/azora-go/internal/azp"
)

// Default values for Config fields left zero.
const (
	DefaultPort           = "7480"
	DefaultWebSocketPath  = "/rt/v1"
	DefaultReconnectMin   = 100 * time.Millisecond
	DefaultReconnectMax   = 30 * time.Second
	DefaultRequestTimeout = 30 * time.Second
	DefaultClientName     = "azora-go/" + Version
	// MaxPoolSize is the largest Config.PoolSize.
	MaxPoolSize = 64
)

// Version is the SDK's version, sent to the server in the client name.
const Version = "0.1.0"

// prologue binds handshakes to the realtime protocol; it equals the
// server's.
const prologue = "azora-rt/1"

// subprotocol is the WebSocket subprotocol of the realtime endpoint.
const subprotocol = "azp.v1"

// Config configures a DB. Addr (or WebSocketURL), Database, a way to
// authenticate the server (ServerKey, Fingerprint, or AllowPlaintext for
// tests) and a credential are required. ParseURL fills the first three
// from a connection string; Connect does both steps at once.
type Config struct {
	// Addr is the server's raw TCP realtime address (host:port).
	Addr string
	// Database names the database the connection is bound to.
	Database string

	// ServerKey pins the server's static public key (base64, as printed by
	// `azora keys show` and by `azora serve` on start): the Noise IK
	// handshake, one round trip, and the connection fails unless the
	// server holds the matching private key.
	ServerKey string
	// Fingerprint ("azp1-...") selects the Noise XX handshake: the server
	// sends its key and the client checks it against the fingerprint. With
	// ServerKey also set, it is an extra check of the pinned key.
	Fingerprint string
	// AllowPlaintext requests an unencrypted channel. For tests and
	// loopback only: it applies only when neither ServerKey nor Fingerprint
	// is set, and the server and the database must allow it too.
	AllowPlaintext bool

	// WebSocketURL ("wss://host/rt/v1") connects over WebSocket instead of
	// TCP; Addr is then ignored. Frames are encrypted end to end inside the
	// WebSocket, so a TLS-terminating proxy only sees ciphertext.
	WebSocketURL string
	// HTTPClient is used for the WebSocket upgrade and for storage
	// requests (default: one built on Dial).
	HTTPClient *http.Client

	// Credential is an API key ("azk_...") or a client token ("azt1...").
	Credential string
	// CredentialFunc returns a fresh credential (for example a newly
	// minted short-lived token). When set it is called on every
	// (re)connect of every pooled connection and whenever the server
	// reports that the credential expired, and its result replaces
	// Credential. It must be safe for concurrent use.
	CredentialFunc func(ctx context.Context) (string, error)
	// ClientName identifies the application in the server's logs (default
	// "azora-go/<version>").
	ClientName string

	// PoolSize is the number of connections (default 1, at most
	// MaxPoolSize). The server runs at most 8 requests of one connection
	// at a time and rate-limits each connection separately, so a pool
	// raises the throughput of concurrent requests. Requests go to the
	// connection with the fewest in flight; subscriptions share the first
	// connection.
	PoolSize int

	// StorageURL is the server's HTTP address ("https://files.example.com"),
	// used by Storage. With WebSocketURL it defaults to that URL's origin.
	StorageURL string

	// Dial opens TCP connections (default: a net.Dialer). It is also used
	// for WebSocket and storage connections when HTTPClient is nil.
	Dial func(ctx context.Context, network, addr string) (net.Conn, error)
	// ReconnectMin and ReconnectMax bound the reconnect backoff
	// (exponential with full jitter; defaults 100ms and 30s).
	ReconnectMin, ReconnectMax time.Duration
	// RequestTimeout bounds a call whose context has no deadline,
	// including the time spent waiting for a connection (default 30s;
	// negative disables it). It also bounds each connection attempt.
	RequestTimeout time.Duration

	// Logger receives connection events at debug level and failed
	// credential refreshes as warnings (default slog.Default()).
	Logger *slog.Logger
	// Hooks observe the connection; they run on the client's goroutines
	// and must not block.
	Hooks Hooks
}

// Hooks are optional callbacks. They are called from the client's internal
// goroutines and must return quickly.
type Hooks struct {
	// OnConnect runs after every successful connection (handshake and
	// Hello done), once per pooled connection.
	OnConnect func(Welcome)
	// OnDisconnect runs when a connection that OnConnect reported ends,
	// with the reason (ErrClosed when the DB was closed).
	OnDisconnect func(error)
	// OnResync runs when a subscription lost its resume point (schema
	// change, retention, recovery) and takes a new snapshot; its next
	// Update has Reset set.
	OnResync func(subID uint32, reason string)
	// OnError reports errors that no caller receives: failed reconnects,
	// session errors sent by the server, failed credential refreshes.
	OnError func(error)
}

// Welcome describes an established session.
type Welcome struct {
	SessionID        string
	ServerTime       time.Time
	Heartbeat        time.Duration
	MaxMessage       int
	Principal        string
	Scopes           []string
	MaxSubscriptions int
	// Mode is the channel's security: "IK", "XX" or "plaintext".
	Mode string
	// ServerKey is the server's public key (base64url, "" in plaintext)
	// and Fingerprint its fingerprint.
	ServerKey   string
	Fingerprint string
}

// ParseURL parses a connection string into a Config:
//
//	azora://host[:port]/<database>?key=<base64 public key>
//	azora://host/<database>?fp=azp1-xxxx-...              (first contact, XX)
//	azoras+ws://host[:port][/path]?db=<database>&key=...  (WebSocket over TLS)
//	azora+ws://host[:port][/path]?db=<database>&key=...   (plain WebSocket)
//	wss://host[:port][/path]?db=<database>&key=...        (same as azoras+ws)
//	ws://host[:port][/path]?db=<database>&key=...         (same as azora+ws)
//
// The TCP port defaults to 7480 and the WebSocket path to /rt/v1. Query
// parameters: key (server public key), fp (fingerprint), db (database;
// for azora:// the path may name it instead), plaintext=1 (tests only).
// Credentials are not read from URLs: set Config.Credential or pass
// WithAPIKey / WithToken to Connect.
func ParseURL(s string) (Config, error) {
	cfg, err := parseURL(s)
	if err != nil {
		return cfg, err
	}
	if _, err := cfg.security(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// parseURL is ParseURL without the check that the URL says how to
// authenticate the server (Connect's options may still add that).
func parseURL(s string) (Config, error) {
	var cfg Config
	u, err := url.Parse(strings.TrimSpace(s))
	if err != nil {
		return cfg, fmt.Errorf("azora: parse url: %w", err)
	}
	if u.User != nil {
		return cfg, errors.New("azora: parse url: user info is not supported (set Config.Credential)")
	}
	if u.Host == "" || u.Hostname() == "" {
		return cfg, errors.New("azora: parse url: missing host")
	}
	if u.Fragment != "" {
		return cfg, errors.New("azora: parse url: unexpected fragment")
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return cfg, fmt.Errorf("azora: parse url: %w", err)
	}
	for k, vs := range q {
		if len(vs) != 1 {
			return cfg, fmt.Errorf("azora: parse url: parameter %q given %d times", k, len(vs))
		}
		switch k {
		case "key":
			cfg.ServerKey = vs[0]
		case "fp":
			cfg.Fingerprint = vs[0]
		case "db":
			cfg.Database = vs[0]
		case "plaintext":
			switch vs[0] {
			case "1", "true":
				cfg.AllowPlaintext = true
			case "0", "false":
			default:
				return cfg, fmt.Errorf("azora: parse url: plaintext=%q (want 1 or 0)", vs[0])
			}
		default:
			return cfg, fmt.Errorf("azora: parse url: unknown parameter %q", k)
		}
	}
	if u.Port() != "" && !validPort(u.Port()) {
		return cfg, fmt.Errorf("azora: parse url: bad port %q", u.Port())
	}
	switch scheme := strings.ToLower(u.Scheme); scheme {
	case "azora":
		path := strings.Trim(u.Path, "/")
		if path != "" {
			if cfg.Database != "" && cfg.Database != path {
				return cfg, errors.New("azora: parse url: database given in both the path and db=")
			}
			if strings.Contains(path, "/") {
				return cfg, fmt.Errorf("azora: parse url: path %q is not a database name", u.Path)
			}
			cfg.Database = path
		}
		port := u.Port()
		if port == "" {
			port = DefaultPort
		}
		cfg.Addr = net.JoinHostPort(u.Hostname(), port)
	case "azoras+ws", "azora+ws", "wss", "ws":
		ws := "wss"
		if scheme == "azora+ws" || scheme == "ws" {
			ws = "ws"
		}
		path := u.EscapedPath()
		if path == "" || path == "/" {
			path = DefaultWebSocketPath
		}
		cfg.WebSocketURL = ws + "://" + u.Host + path
	default:
		return cfg, fmt.Errorf("azora: parse url: unsupported scheme %q (azora, azoras+ws, azora+ws, wss, ws)", u.Scheme)
	}
	if cfg.Database == "" {
		return cfg, errors.New("azora: parse url: missing database")
	}
	return cfg, nil
}

func validPort(p string) bool {
	if len(p) == 0 || len(p) > 5 {
		return false
	}
	n := 0
	for i := 0; i < len(p); i++ {
		if p[i] < '0' || p[i] > '9' {
			return false
		}
		n = n*10 + int(p[i]-'0')
	}
	return n > 0 && n < 65536
}

// DecodeServerKey parses a server public key: unpadded base64url (the form
// `azora keys show` prints), or standard base64 with or without padding.
func DecodeServerKey(s string) ([32]byte, error) {
	s = strings.TrimSpace(s)
	if k, err := azp.DecodePublicKey(s); err == nil {
		return k, nil
	}
	var out [32]byte
	// A '+' in a URL query decodes as a space.
	t := strings.ReplaceAll(s, " ", "+")
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding} {
		b, err := enc.DecodeString(t)
		if err == nil && len(b) == len(out) {
			copy(out[:], b)
			return out, nil
		}
	}
	return out, errors.New("azora: server key: not a base64 32-byte key")
}

// KeyFingerprint returns the fingerprint ("azp1-...") of a server public
// key given in any form DecodeServerKey accepts: what Config.Fingerprint
// and the fp= URL parameter are compared with.
func KeyFingerprint(serverKey string) (string, error) {
	k, err := DecodeServerKey(serverKey)
	if err != nil {
		return "", err
	}
	return azp.Fingerprint(k), nil
}

// normalizeFingerprint lowercases a fingerprint and checks its shape.
func normalizeFingerprint(fp string) (string, error) {
	fp = strings.ToLower(strings.TrimSpace(fp))
	if !strings.HasPrefix(fp, "azp1-") || len(fp) != len("azp1-")+24+5 {
		return "", fmt.Errorf("azora: fingerprint %q: want azp1- followed by six 4-character groups", fp)
	}
	return fp, nil
}

// security builds the handshake configuration.
func (cfg *Config) security() (azp.ClientConfig, error) {
	cc := azp.ClientConfig{Prologue: []byte(prologue)}
	if cfg.Fingerprint != "" {
		fp, err := normalizeFingerprint(cfg.Fingerprint)
		if err != nil {
			return cc, err
		}
		cc.VerifyServer = func(pub [32]byte) error {
			if got := azp.Fingerprint(pub); got != fp {
				return fmt.Errorf("server key fingerprint %s does not match the configured %s", got, fp)
			}
			return nil
		}
	}
	if cfg.ServerKey != "" {
		k, err := DecodeServerKey(cfg.ServerKey)
		if err != nil {
			return cc, err
		}
		cc.ServerKey = &k
	}
	switch {
	case cc.ServerKey != nil, cc.VerifyServer != nil:
	case cfg.AllowPlaintext:
		cc.AllowPlaintext = true
	default:
		return cc, errors.New("azora: set ServerKey (pinned key) or Fingerprint; AllowPlaintext is for tests only")
	}
	return cc, nil
}

// withDefaults validates cfg and fills defaults.
func (cfg Config) withDefaults() (Config, azp.ClientConfig, error) {
	if cfg.Database == "" {
		return cfg, azp.ClientConfig{}, errors.New("azora: Config.Database is required")
	}
	if cfg.Addr == "" && cfg.WebSocketURL == "" {
		return cfg, azp.ClientConfig{}, errors.New("azora: Config.Addr or Config.WebSocketURL is required")
	}
	if cfg.WebSocketURL != "" {
		u, err := url.Parse(cfg.WebSocketURL)
		if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.Host == "" {
			return cfg, azp.ClientConfig{}, fmt.Errorf("azora: WebSocketURL %q: want ws:// or wss://", cfg.WebSocketURL)
		}
		if cfg.StorageURL == "" {
			cfg.StorageURL = "http" + strings.TrimPrefix(u.Scheme, "ws") + "://" + u.Host
		}
	}
	if cfg.StorageURL != "" {
		u, err := url.Parse(cfg.StorageURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return cfg, azp.ClientConfig{}, fmt.Errorf("azora: StorageURL %q: want http:// or https://", cfg.StorageURL)
		}
		cfg.StorageURL = strings.TrimRight(cfg.StorageURL, "/")
	}
	if cfg.Credential == "" && cfg.CredentialFunc == nil {
		return cfg, azp.ClientConfig{}, errors.New("azora: a credential is required: Config.Credential or Config.CredentialFunc (WithAPIKey, WithToken, WithCredentialFunc)")
	}
	if cfg.PoolSize < 0 || cfg.PoolSize > MaxPoolSize {
		return cfg, azp.ClientConfig{}, fmt.Errorf("azora: PoolSize %d out of range (1 to %d)", cfg.PoolSize, MaxPoolSize)
	}
	cc, err := cfg.security()
	if err != nil {
		return cfg, cc, err
	}
	if cfg.PoolSize == 0 {
		cfg.PoolSize = 1
	}
	if cfg.ReconnectMin <= 0 {
		cfg.ReconnectMin = DefaultReconnectMin
	}
	if cfg.ReconnectMax <= 0 {
		cfg.ReconnectMax = DefaultReconnectMax
	}
	if cfg.ReconnectMax < cfg.ReconnectMin {
		cfg.ReconnectMax = cfg.ReconnectMin
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = DefaultRequestTimeout
	}
	if cfg.ClientName == "" {
		cfg.ClientName = DefaultClientName
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Dial == nil {
		d := &net.Dialer{KeepAlive: 30 * time.Second}
		cfg.Dial = d.DialContext
	}
	return cfg, cc, nil
}

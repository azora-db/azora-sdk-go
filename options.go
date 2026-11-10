package azora

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// Option adjusts the Config that Connect builds from a connection string.
type Option func(*Config)

// WithAPIKey authenticates with an API key ("azk_...", created with
// `azora meta key create`). API keys are for servers and tools.
func WithAPIKey(key string) Option { return func(c *Config) { c.Credential = key } }

// WithToken authenticates with a client token ("azt1...", minted by your
// backend for one user). Use WithCredentialFunc instead when the token
// expires during the life of the connection.
func WithToken(token string) Option { return func(c *Config) { c.Credential = token } }

// WithCredentialFunc authenticates with whatever f returns, calling it on
// every (re)connect and whenever the server reports that the credential
// expired: the way to use short-lived tokens. See Config.CredentialFunc.
func WithCredentialFunc(f func(ctx context.Context) (string, error)) Option {
	return func(c *Config) { c.CredentialFunc = f }
}

// WithPool opens n connections and spreads requests across them (see
// Config.PoolSize).
func WithPool(n int) Option { return func(c *Config) { c.PoolSize = n } }

// WithStorageURL sets the server's HTTP address for Storage (see
// Config.StorageURL).
func WithStorageURL(u string) Option { return func(c *Config) { c.StorageURL = u } }

// WithLogger sets the logger for connection events.
func WithLogger(l *slog.Logger) Option { return func(c *Config) { c.Logger = l } }

// WithHooks sets the connection callbacks.
func WithHooks(h Hooks) Option { return func(c *Config) { c.Hooks = h } }

// WithRequestTimeout bounds calls whose context has no deadline (see
// Config.RequestTimeout; negative disables the bound).
func WithRequestTimeout(d time.Duration) Option { return func(c *Config) { c.RequestTimeout = d } }

// WithReconnectBackoff bounds the delay between reconnect attempts.
func WithReconnectBackoff(min, max time.Duration) Option {
	return func(c *Config) { c.ReconnectMin, c.ReconnectMax = min, max }
}

// WithClientName names the application in the server's logs.
func WithClientName(name string) Option { return func(c *Config) { c.ClientName = name } }

// WithServerKey pins the server's public key (overriding key= of the
// connection string).
func WithServerKey(key string) Option { return func(c *Config) { c.ServerKey = key } }

// WithFingerprint checks the server's key against a fingerprint
// (overriding fp= of the connection string).
func WithFingerprint(fp string) Option { return func(c *Config) { c.Fingerprint = fp } }

// WithHTTPClient sets the HTTP client used for the WebSocket upgrade and
// for storage requests.
func WithHTTPClient(h *http.Client) Option { return func(c *Config) { c.HTTPClient = h } }

// WithDialer sets the function that opens network connections.
func WithDialer(dial func(ctx context.Context, network, addr string) (net.Conn, error)) Option {
	return func(c *Config) { c.Dial = dial }
}

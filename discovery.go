package azora

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// Environment variables read by ConnectEnv.
const (
	// EnvURL names the database's public URL.
	EnvURL = "AZORA_URL"
	// EnvToken names the private credential: an API key or a client
	// token. EnvAPIKey is read when it is not set.
	EnvToken  = "AZORA_TOKEN"
	EnvAPIKey = "AZORA_API_KEY"
)

// discoveryPath is where a server publishes its public key and realtime
// endpoint, below its base URL.
const discoveryPath = "/.well-known/azora.json"

// ConnectEnv opens a DB from the environment: AZORA_URL, the database's
// public URL, and AZORA_TOKEN, the private credential (an API key or a
// client token). `azora connect <database> --key <name>` on the server
// prints both.
//
//	db, err := azora.ConnectEnv(ctx)
func ConnectEnv(ctx context.Context, opts ...Option) (*DB, error) {
	u := strings.TrimSpace(os.Getenv(EnvURL))
	if u == "" {
		return nil, fmt.Errorf("azora: %s is not set: it is the database's public URL, printed by `azora connect <database>` on the server", EnvURL)
	}
	token := strings.TrimSpace(os.Getenv(EnvToken))
	if token == "" {
		token = strings.TrimSpace(os.Getenv(EnvAPIKey))
	}
	cfg, err := parseURL(u)
	if err != nil {
		return nil, err
	}
	cfg.Credential = token
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.Credential == "" && cfg.CredentialFunc == nil {
		return nil, fmt.Errorf("azora: %s is not set: it is the private credential (an API key or a client token)", EnvToken)
	}
	return Open(ctx, cfg)
}

// needsDiscovery reports whether the server's key has to be read from
// the discovery document: the configuration names one and says nothing
// else about how to authenticate the server.
func (cfg *Config) needsDiscovery() bool {
	return cfg.DiscoveryURL != "" && cfg.ServerKey == "" && cfg.Fingerprint == "" && !cfg.AllowPlaintext
}

// isLoopbackHost reports whether host is this machine.
func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// discoveryAllowed says whether a server key read from discoveryURL can
// be trusted: over HTTPS the certificate vouches for the answer, and on
// loopback nobody is in between. Over plain HTTP to another machine
// anyone on the path could answer with a key of their own.
func discoveryAllowed(discoveryURL string) error {
	u, err := url.Parse(discoveryURL)
	if err != nil || u.Host == "" {
		return fmt.Errorf("azora: DiscoveryURL %q: want an http(s) URL", discoveryURL)
	}
	if u.Scheme == "https" || u.Scheme == "http" && isLoopbackHost(u.Hostname()) {
		return nil
	}
	return fmt.Errorf("azora: the URL has no server key and %s is plain HTTP to another machine, so nothing would prove who answers: "+
		"add ?key=<server key> to the URL (`azora connect <database>` prints it) or use https", u.Host)
}

// discovered is what a discovery document yields.
type discovered struct {
	serverKey string
	webSocket string   // path of the realtime endpoint below the base URL
	features  []string // optional features the server supports ("units")
}

// discover reads the server's discovery document.
func discover(ctx context.Context, hc *http.Client, discoveryURL string) (discovered, error) {
	var out discovered
	if err := discoveryAllowed(discoveryURL); err != nil {
		return out, err
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, discoveryURL, nil)
	if err != nil {
		return out, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := hc.Do(req)
	if err != nil {
		return out, fmt.Errorf("azora: discovery: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return out, fmt.Errorf("azora: discovery: %w", err)
	}
	if to := resp.Request.URL.String(); to != discoveryURL {
		// Followed a redirect: the answer is only as good as where it
		// came from.
		if err := discoveryAllowed(to); err != nil {
			return out, fmt.Errorf("azora: discovery: %s redirected to %s, which is not https", discoveryURL, resp.Request.URL.Host)
		}
	}
	if resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("azora: discovery: %s answered %s: is this an Azora server's address? (or add ?key=<server key> to the URL)", discoveryURL, resp.Status)
	}
	var doc struct {
		Azora     int      `json:"azora"`
		ServerKey string   `json:"server_key"`
		WebSocket string   `json:"websocket"`
		Features  []string `json:"features"`
	}
	if err := json.Unmarshal(body, &doc); err != nil || doc.Azora != 1 || doc.ServerKey == "" {
		return out, fmt.Errorf("azora: discovery: %s did not answer an Azora discovery document", discoveryURL)
	}
	if _, err := DecodeServerKey(doc.ServerKey); err != nil {
		return out, fmt.Errorf("azora: discovery: %w", err)
	}
	if doc.WebSocket != "" && (!strings.HasPrefix(doc.WebSocket, "/") || strings.ContainsAny(doc.WebSocket, "?#")) {
		return out, errors.New("azora: discovery: bad websocket path")
	}
	return discovered{serverKey: doc.ServerKey, webSocket: doc.WebSocket, features: doc.Features}, nil
}

// applyDiscovery reads the discovery document into the DB's
// configuration and builds the handshake settings from it.
func (db *DB) applyDiscovery(ctx context.Context) error {
	d, err := discover(ctx, db.http, db.cfg.DiscoveryURL)
	if err != nil {
		return err
	}
	cfg := db.cfg
	cfg.ServerKey = d.serverKey
	sec, err := cfg.security()
	if err != nil {
		return err
	}
	db.secMu.Lock()
	db.sec = sec
	db.features = d.features
	if d.webSocket != "" && db.wsBase != "" {
		db.wsURL = db.wsBase + d.webSocket
	}
	db.secMu.Unlock()
	return nil
}

// supports reports whether the server lists feature in its discovery
// document. Without a discovery document (the key was given) nothing is
// known and every feature is assumed: the server answers what it does
// not support with an error.
func (db *DB) supports(feature string) bool {
	db.secMu.Lock()
	defer db.secMu.Unlock()
	if !db.discovered {
		return true
	}
	for _, f := range db.features {
		if f == feature {
			return true
		}
	}
	return false
}

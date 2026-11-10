// Package devengine runs a real Azora engine (the `azora` binary) on a
// temporary data root: the SDK's integration tests and the examples' dev
// server are built on it.
//
// The engine's command line takes the data root lock, so commands (create
// a database, mint a key or a token) only work while the server is
// stopped: do the setup before Serve, or between Stop and Serve.
package devengine

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"
)

// EnvBin names the environment variable that points at the engine binary.
const EnvBin = "AZORA_BIN"

// FindBinary returns the engine binary: $AZORA_BIN, else
// ../azora-engine/bin/azora next to the SDK's module root (found by
// walking up from the working directory), else `azora` on PATH.
func FindBinary() (string, error) {
	if p := os.Getenv(EnvBin); p != "" {
		if _, err := os.Stat(p); err != nil {
			return "", fmt.Errorf("%s=%s: %w", EnvBin, p, err)
		}
		return p, nil
	}
	if dir, err := os.Getwd(); err == nil {
		for {
			if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
				p := filepath.Join(filepath.Dir(dir), "azora-engine", "bin", "azora")
				if _, err := os.Stat(p); err == nil {
					return p, nil
				}
			}
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}
	if p, err := exec.LookPath("azora"); err == nil {
		return p, nil
	}
	return "", fmt.Errorf("azora engine binary not found: set %s or build ../azora-engine/bin/azora", EnvBin)
}

// Options configure New.
type Options struct {
	// Bin is the engine binary ("" = FindBinary).
	Bin string
	// Dir is where the config and the data root live; it must exist.
	Dir string
	// Config adds or overrides sections of azora.json, merged one level
	// deep: {"realtime": {"message_rate": 100000}}.
	Config map[string]map[string]any
}

// Engine is an engine binary bound to a data root, with its server
// process when one is running.
type Engine struct {
	Bin        string
	Dir        string
	ConfigPath string
	// HTTPAddr and RTAddr are the listen addresses Serve uses (fixed when
	// the Engine is created, so a restart comes back on the same ports).
	HTTPAddr, RTAddr string
	// ServerKey and Fingerprint are the realtime server's public key, as
	// printed by the server when it starts.
	ServerKey, Fingerprint string

	mu     sync.Mutex
	cmd    *exec.Cmd
	exited chan struct{}
	stderr *syncBuffer
}

// New writes the config, initializes the data root and picks free ports.
// The server is not started.
func New(o Options) (*Engine, error) {
	bin := o.Bin
	if bin == "" {
		var err error
		if bin, err = FindBinary(); err != nil {
			return nil, err
		}
	}
	e := &Engine{Bin: bin, Dir: o.Dir, ConfigPath: filepath.Join(o.Dir, "azora.json")}
	cfg := map[string]any{
		"data_root": filepath.Join(o.Dir, "root"),
		"realtime":  map[string]any{"allow_plaintext": true},
		"storage":   map[string]any{"cors_origins": "*"},
	}
	for section, kv := range o.Config {
		m, _ := cfg[section].(map[string]any)
		if m == nil {
			m = map[string]any{}
			cfg[section] = m
		}
		for k, v := range kv {
			m[k] = v
		}
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(e.ConfigPath, data, 0o600); err != nil {
		return nil, err
	}
	if _, err := e.CLI("init", "--json"); err != nil {
		return nil, err
	}
	if e.HTTPAddr, err = freeAddr(); err != nil {
		return nil, err
	}
	if e.RTAddr, err = freeAddr(); err != nil {
		return nil, err
	}
	return e, nil
}

// freeAddr returns a loopback address with a port that was free a moment
// ago.
func freeAddr() (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer ln.Close()
	return ln.Addr().String(), nil
}

// CLI runs an engine command against the data root and returns its
// standard output. It fails while the server runs (the data root is
// locked).
func (e *Engine) CLI(args ...string) (string, error) {
	cmd := exec.Command(e.Bin, append([]string{"--config", e.ConfigPath}, args...)...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("azora %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(errb.String()))
	}
	return out.String(), nil
}

// CreateDB creates a database.
func (e *Engine) CreateDB(name string) error {
	_, err := e.CLI("db", "create", name, "--json")
	return err
}

// Exec runs SQL statements on a database, one command per statement.
func (e *Engine) Exec(db string, statements ...string) error {
	for _, s := range statements {
		if _, err := e.CLI("db", "exec", db, s); err != nil {
			return err
		}
	}
	return nil
}

// Publish publishes tables for realtime ("files" publishes stored files).
func (e *Engine) Publish(db string, tables ...string) error {
	_, err := e.CLI(append([]string{"realtime", "publish", db}, tables...)...)
	return err
}

// CreateKey creates an API key with the given scopes (read, write, ddl,
// admin) and returns its secret.
func (e *Engine) CreateKey(db, name string, scopes ...string) (string, error) {
	out, err := e.CLI("meta", "key", "create", db, "--name", name, "--scope", strings.Join(scopes, ","), "--json")
	if err != nil {
		return "", err
	}
	var res struct {
		Secret string `json:"secret"`
	}
	if err := json.Unmarshal([]byte(out), &res); err != nil || res.Secret == "" {
		return "", fmt.Errorf("azora meta key create: unexpected output %q", out)
	}
	return res.Secret, nil
}

// Token describes a client token to mint.
type Token struct {
	Sub     string
	Scopes  []string      // default read
	TTL     time.Duration // default 1h
	Tables  []string
	Filters []string // "table: filter"
}

// CreateToken mints a client token.
func (e *Engine) CreateToken(db string, t Token) (string, error) {
	args := []string{"realtime", "token", "create", db, "--sub", t.Sub, "--json"}
	if len(t.Scopes) > 0 {
		args = append(args, "--scope", strings.Join(t.Scopes, ","))
	}
	if t.TTL > 0 {
		args = append(args, "--ttl", t.TTL.String())
	}
	for _, tb := range t.Tables {
		args = append(args, "--table", tb)
	}
	for _, f := range t.Filters {
		args = append(args, "--filter", f)
	}
	out, err := e.CLI(args...)
	if err != nil {
		return "", err
	}
	var res struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal([]byte(out), &res); err != nil || res.Token == "" {
		return "", fmt.Errorf("azora realtime token create: unexpected output %q", out)
	}
	return res.Token, nil
}

var keyLine = regexp.MustCompile(`server key (\S+) \(fingerprint (azp1-[a-z2-7-]+)\)`)

// Serve starts `azora serve` and waits until it accepts connections.
func (e *Engine) Serve() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.cmd != nil {
		return errors.New("devengine: the server is already running")
	}
	cmd := exec.Command(e.Bin, "--config", e.ConfigPath, "serve", "--addr", e.HTTPAddr, "--rt-addr", e.RTAddr,
		"--public-url", "http://"+e.HTTPAddr)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr := &syncBuffer{}
	cmd.Stderr = stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} // a Ctrl+C in the terminal is ours to forward
	if err := cmd.Start(); err != nil {
		return err
	}
	exited := make(chan struct{})
	ready := make(chan error, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		reported := false
		for sc.Scan() {
			line := sc.Text()
			if m := keyLine.FindStringSubmatch(line); m != nil {
				e.ServerKey, e.Fingerprint = m[1], m[2]
			}
			if !reported && strings.HasPrefix(line, "Serving storage on") {
				reported = true
				ready <- nil
			}
		}
		io.Copy(io.Discard, stdout)
		cmd.Wait()
		if !reported {
			ready <- fmt.Errorf("devengine: azora serve exited before it was ready: %s", strings.TrimSpace(stderr.String()))
		}
		close(exited)
	}()
	select {
	case err := <-ready:
		if err != nil {
			return err
		}
	case <-time.After(30 * time.Second):
		cmd.Process.Kill()
		return fmt.Errorf("devengine: azora serve was not ready after 30s: %s", strings.TrimSpace(stderr.String()))
	}
	if e.ServerKey == "" {
		cmd.Process.Kill()
		<-exited
		return errors.New("devengine: azora serve did not print its server key")
	}
	e.cmd, e.exited, e.stderr = cmd, exited, stderr
	return nil
}

// syncBuffer is a bytes.Buffer safe for a writer and a reader on
// different goroutines (exec copies stderr on its own).
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// Stderr returns what the running (or last) server wrote to standard
// error: its log.
func (e *Engine) Stderr() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stderr == nil {
		return ""
	}
	return e.stderr.String()
}

// Running reports whether the server process is up.
func (e *Engine) Running() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.cmd != nil
}

// Stop shuts the server down gracefully (SIGINT), killing it if it has
// not exited after 15 seconds. It is a no-op when the server is not
// running.
func (e *Engine) Stop() error { return e.end(syscall.SIGINT) }

// Kill ends the server at once (SIGKILL), as a crash would.
func (e *Engine) Kill() error { return e.end(syscall.SIGKILL) }

func (e *Engine) end(sig syscall.Signal) error {
	e.mu.Lock()
	cmd, exited := e.cmd, e.exited
	e.cmd, e.exited = nil, nil // stderr is kept for Stderr
	e.mu.Unlock()
	if cmd == nil {
		return nil
	}
	cmd.Process.Signal(sig)
	select {
	case <-exited:
	case <-time.After(15 * time.Second):
		cmd.Process.Kill()
		<-exited
		return errors.New("devengine: azora serve did not stop within 15s and was killed")
	}
	return nil
}

// Restart stops the server and starts it again on the same data root and
// ports.
func (e *Engine) Restart() error {
	if err := e.Stop(); err != nil {
		return err
	}
	return e.Serve()
}

// ConnectURL returns the database's public URL (what `azora connect`
// prints): the HTTP address, the database, the server's key.
func (e *Engine) ConnectURL(db string) string {
	return "http://" + e.HTTPAddr + "/" + db + "?key=" + e.ServerKey
}

// URL returns the TCP connection string of a database, pinning the
// server's key.
func (e *Engine) URL(db string) string {
	return "azora://" + e.RTAddr + "/" + db + "?key=" + e.ServerKey
}

// WebSocketURL returns the WebSocket connection string of a database,
// pinning the server's key.
func (e *Engine) WebSocketURL(db string) string {
	return "azora+ws://" + e.HTTPAddr + "/rt/v1?db=" + db + "&key=" + e.ServerKey
}

// HTTPURL returns the server's HTTP origin (storage API).
func (e *Engine) HTTPURL() string { return "http://" + e.HTTPAddr }

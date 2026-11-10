package azora_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	azora "github.com/azora-db/azora-go"
)

const someKey = "vqh3QC4QcPU4fY9MRogg_GZwZ2Ahuh4M_h40-FME3UI"

func TestParsePublicURL(t *testing.T) {
	cfg, err := azora.ParseURL("https://db.example.com/shop?key=" + someKey)
	require.NoError(t, err)
	require.Equal(t, "shop", cfg.Database)
	require.Equal(t, "wss://db.example.com/rt/v1", cfg.WebSocketURL)
	require.Equal(t, "https://db.example.com", cfg.StorageURL)
	require.Equal(t, "https://db.example.com/.well-known/azora.json", cfg.DiscoveryURL)
	require.Equal(t, someKey, cfg.ServerKey)
	require.Empty(t, cfg.Addr)
	require.Empty(t, cfg.Credential, "a URL never carries the credential")

	// A path before the database is where the server is mounted.
	cfg, err = azora.ParseURL("https://example.com:8443/azora/v1/shop/")
	require.NoError(t, err)
	require.Equal(t, "shop", cfg.Database)
	require.Equal(t, "wss://example.com:8443/azora/v1/rt/v1", cfg.WebSocketURL)
	require.Equal(t, "https://example.com:8443/azora/v1", cfg.StorageURL)
	require.Equal(t, "https://example.com:8443/azora/v1/.well-known/azora.json", cfg.DiscoveryURL)

	// Plain HTTP: fine with a pinned key, and without one on loopback only.
	cfg, err = azora.ParseURL("http://10.0.0.5:8080/shop?key=" + someKey)
	require.NoError(t, err)
	require.Equal(t, "ws://10.0.0.5:8080/rt/v1", cfg.WebSocketURL)
	for _, u := range []string{"http://127.0.0.1:8080/shop", "http://localhost:8080/shop", "http://[::1]:8080/shop", "https://db.example.com/shop"} {
		_, err := azora.ParseURL(u)
		require.NoError(t, err, u)
	}
	_, err = azora.ParseURL("http://10.0.0.5:8080/shop")
	require.ErrorContains(t, err, "add ?key=")
	_, err = azora.ParseURL("http://db.example.com/shop")
	require.ErrorContains(t, err, "add ?key=")
	_, err = azora.ParseURL("http://10.0.0.5:8080/shop?fp=azp1-7uih-nrqh-26rp-caj3-az5k-zdaw")
	require.NoError(t, err, "a fingerprint also says who the server is")

	for u, msg := range map[string]string{
		"https://db.example.com":                 "must end with the database name",
		"https://db.example.com/":                "must end with the database name",
		"https://db.example.com/shop?db=other":   "both the path and db=",
		"https://db.example.com/shop?token=x":    "unknown parameter",
		"https://user:pw@db.example.com/shop":    "user info",
		"ftp://db.example.com/shop":              "unsupported scheme",
		"https://db.example.com/shop?key=notkey": "server key",
	} {
		_, err := azora.ParseURL(u)
		require.ErrorContains(t, err, msg, u)
	}
}

func TestDiscoveryRefusals(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var body string
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/.well-known/azora.json" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(status)
		w.Write([]byte(body))
	}))
	defer srv.Close()
	connect := func() error {
		_, err := azora.Connect(ctx, srv.URL+"/shop", azora.WithToken("azk_x"))
		return err
	}
	body = `<html>not azora</html>`
	require.ErrorContains(t, connect(), "did not answer an Azora discovery document")
	body = `{"azora": 2, "server_key": "` + someKey + `"}`
	require.ErrorContains(t, connect(), "did not answer an Azora discovery document", "a version this SDK does not know")
	body = `{"azora": 1, "server_key": "short"}`
	require.ErrorContains(t, connect(), "server key")
	body = `{"azora": 1, "server_key": "` + someKey + `", "websocket": "https://evil.example/rt"}`
	require.ErrorContains(t, connect(), "websocket path")
	status = http.StatusNotFound
	require.ErrorContains(t, connect(), "is this an Azora server's address")

	// No network call at all for plain HTTP to another machine.
	_, err := azora.Connect(ctx, "http://192.0.2.1:8080/shop", azora.WithToken("azk_x"))
	require.ErrorContains(t, err, "add ?key=")
	// A credential is still required.
	_, err = azora.Connect(ctx, srv.URL+"/shop")
	require.ErrorContains(t, err, "credential")
}

func TestEngineConnectByPublicURL(t *testing.T) {
	f := engine(t)
	c := ctx(t)

	// The URL `azora connect` prints: WebSocket, the key pinned, storage
	// at the same address.
	db := f.connectURL(t, f.ConnectURL(testDB), azora.WithToken(f.Key))
	w, ok := db.Welcome()
	require.True(t, ok)
	require.Equal(t, "IK", w.Mode)
	require.Equal(t, f.ServerKey, w.ServerKey)
	n, err := azora.Scalar[int64](c, db, "SELECT 40 + 2")
	require.NoError(t, err)
	require.EqualValues(t, 42, n)
	obj, err := db.Storage().Upload(c, strings.NewReader("one url"), azora.UploadOptions{Name: "one.txt"})
	require.NoError(t, err, "storage needs no address of its own")
	var back strings.Builder
	_, err = db.Storage().DownloadTo(c, obj.ID, &back)
	require.NoError(t, err)
	require.Equal(t, "one url", back.String())
	require.NoError(t, db.Storage().Delete(c, obj.ID))

	// Without the key: read from the server (allowed here: loopback).
	bare := "http://" + f.HTTPAddr + "/" + testDB
	db2 := f.connectURL(t, bare, azora.WithToken(f.ReadKey))
	w, ok = db2.Welcome()
	require.True(t, ok)
	require.Equal(t, "IK", w.Mode, "the discovered key is pinned like a given one")
	require.Equal(t, f.ServerKey, w.ServerKey)
	_, err = db2.Query(c, "SELECT 1")
	require.NoError(t, err)

	// The same URL, another token: a different client altogether.
	db3 := f.connectURL(t, bare, azora.WithToken(f.Restricted))
	_, err = db3.Query(c, "SELECT 1")
	require.True(t, azora.IsPermissionDenied(err), "%v", err)

	// A wrong token is refused with the server's reason.
	_, err = azora.Connect(c, bare, azora.WithToken("azk_wrong"))
	require.True(t, azora.IsUnauthenticated(err), "%v", err)
	// A wrong pinned key fails the handshake.
	_, err = azora.Connect(c, bare+"?key="+someKey, azora.WithToken(f.Key))
	require.Error(t, err)
}

func TestEngineConnectEnv(t *testing.T) {
	f := engine(t)
	c := ctx(t)
	t.Setenv(azora.EnvURL, "")
	t.Setenv(azora.EnvToken, "")
	t.Setenv(azora.EnvAPIKey, "")
	_, err := azora.ConnectEnv(c)
	require.ErrorContains(t, err, "AZORA_URL is not set")
	t.Setenv(azora.EnvURL, f.ConnectURL(testDB))
	_, err = azora.ConnectEnv(c)
	require.ErrorContains(t, err, "AZORA_TOKEN is not set")

	t.Setenv(azora.EnvToken, f.Key)
	db, err := azora.ConnectEnv(c)
	require.NoError(t, err)
	defer db.Close()
	require.Equal(t, testDB, db.Database())
	_, err = db.Exec(c, "DELETE FROM plain WHERE id = -1")
	require.NoError(t, err)

	// The older variable still works, and an option overrides both.
	t.Setenv(azora.EnvToken, "")
	t.Setenv(azora.EnvAPIKey, f.ReadKey)
	ro, err := azora.ConnectEnv(c)
	require.NoError(t, err)
	defer ro.Close()
	_, err = ro.Exec(c, "DELETE FROM plain WHERE id = -1")
	require.True(t, azora.IsPermissionDenied(err), "%v", err)
	rw, err := azora.ConnectEnv(c, azora.WithToken(f.Key))
	require.NoError(t, err)
	defer rw.Close()
	_, err = rw.Exec(c, "DELETE FROM plain WHERE id = -1")
	require.NoError(t, err)
}

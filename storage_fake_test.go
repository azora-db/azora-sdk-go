package azora_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	azora "github.com/azora-db/azora-go"
	"github.com/azora-db/azora-go/internal/azptest"
)

// fakeStorage is a small stand-in for the storage HTTP API with faults
// that can be switched on: the real API is exercised by the integration
// tests.
type fakeStorage struct {
	t   testing.TB
	srv *httptest.Server

	mu       sync.Mutex
	requests []string // "METHOD path?query"
	auth     []string
	objects  []azora.Object
	session  []byte // bytes of the one upload session
	declared int64
	// Faults.
	throttle   int // answer this many requests with 429 first
	cutAppends int // accept only half of this many appends, then break the connection
	wrongFirst bool
}

func newFakeStorage(t testing.TB) *fakeStorage {
	f := &fakeStorage{t: t}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeStorage) fail(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	fmt.Fprintf(w, `{"error":{"code":%q,"message":%q,"limit":"max_object_size","max":5}}`, code, msg)
}

func (f *fakeStorage) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, r.Method+" "+r.URL.RequestURI())
	f.auth = append(f.auth, r.Header.Get("Authorization"))
	if f.throttle > 0 {
		f.throttle--
		w.Header().Set("Retry-After", "0")
		if f.throttle%2 == 0 {
			w.Header().Set("Retry-After", "1")
		}
		f.fail(w, http.StatusTooManyRequests, "throttled", "slow down")
		return
	}
	const base = "/storage/v1/b/app"
	path := r.URL.Path
	switch {
	case path == base+"/objects" && r.Method == http.MethodGet:
		after, _ := strconv.Atoi(r.URL.Query().Get("after"))
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit == 0 {
			limit = 2
		}
		page := azora.ObjectPage{Objects: []azora.Object{}}
		for i := after; i < len(f.objects) && len(page.Objects) < limit; i++ {
			page.Objects = append(page.Objects, f.objects[i])
		}
		if after+len(page.Objects) < len(f.objects) {
			page.Next = strconv.Itoa(after + len(page.Objects))
		}
		json.NewEncoder(w).Encode(page)
	case path == base+"/objects" && r.Method == http.MethodPost:
		body, _ := io.ReadAll(r.Body)
		if len(body) > 5 && r.URL.Query().Get("bucket") == "tiny" {
			f.fail(w, http.StatusRequestEntityTooLarge, "too_large", "over the limit")
			return
		}
		obj := azora.Object{ID: "obj_" + strconv.Itoa(len(f.objects)+1), Name: r.URL.Query().Get("name"), Size: int64(len(body)),
			ContentType: r.Header.Get("Content-Type"), SHA256: r.Header.Get("X-Azora-SHA256"), Bucket: r.URL.Query().Get("bucket"),
			Path: r.URL.Query().Get("path"), Metadata: map[string]string{"content-length": strconv.FormatInt(r.ContentLength, 10)}}
		f.objects = append(f.objects, obj)
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{"object": obj})
	case path == base+"/objects/missing":
		f.fail(w, http.StatusNotFound, "not_found", "object not found")
	case path == base+"/objects/proxy":
		w.WriteHeader(http.StatusBadGateway)
		io.WriteString(w, "<html>bad gateway</html>")
	case path == base+"/objects/obj_1/content":
		w.Header().Set("ETag", `W/"abc-gz"`)
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Last-Modified", "Tue, 10 Nov 2026 10:00:00 GMT")
		if rg := r.Header.Get("Range"); rg != "" {
			w.Header().Set("Content-Range", "echo "+rg)
			w.WriteHeader(http.StatusPartialContent)
		}
		io.WriteString(w, "content")
	case path == base+"/sessions" && r.Method == http.MethodPost:
		f.session = nil
		f.declared, _ = strconv.ParseInt(r.Header.Get("Upload-Length"), 10, 64)
		w.Header().Set("Location", f.srv.URL+"/storage/v1/s/app/sid")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{"session": map[string]any{"id": "sid", "url": f.srv.URL + "/storage/v1/s/app/sid",
			"size": f.declared, "offset": 0, "expires_at": time.Now().Add(time.Hour)}})
	case path == "/storage/v1/s/app/sid" && r.Method == http.MethodHead:
		w.Header().Set("Upload-Offset", strconv.Itoa(len(f.session)))
		if f.declared > 0 {
			w.Header().Set("Upload-Length", strconv.FormatInt(f.declared, 10))
		}
		w.Header().Set("Upload-Expires", time.Now().Add(time.Hour).UTC().Format(http.TimeFormat))
	case path == "/storage/v1/s/app/sid" && r.Method == http.MethodPatch:
		off, _ := strconv.Atoi(r.Header.Get("Upload-Offset"))
		if f.wrongFirst {
			f.wrongFirst = false
			f.session = append(f.session, "0123"...) // someone else's append got there first
		}
		if off != len(f.session) {
			w.Header().Set("Upload-Offset", strconv.Itoa(len(f.session)))
			f.fail(w, http.StatusConflict, "offset_mismatch", "append at the session's offset")
			return
		}
		if f.cutAppends > 0 {
			f.cutAppends--
			half := make([]byte, r.ContentLength/2)
			n, _ := io.ReadFull(r.Body, half)
			f.session = append(f.session, half[:n]...)
			panic(http.ErrAbortHandler) // the connection breaks; what arrived is kept
		}
		body, _ := io.ReadAll(r.Body)
		f.session = append(f.session, body...)
		w.Header().Set("Upload-Offset", strconv.Itoa(len(f.session)))
		w.WriteHeader(http.StatusNoContent)
	case path == "/storage/v1/s/app/sid/complete":
		if f.declared > 0 && int64(len(f.session)) != f.declared {
			w.Header().Set("Upload-Offset", strconv.Itoa(len(f.session)))
			f.fail(w, http.StatusConflict, "offset_mismatch", "incomplete")
			return
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{"object": azora.Object{ID: "obj_done", Size: int64(len(f.session))}})
	case path == "/storage/v1/s/app/gone":
		w.WriteHeader(http.StatusNotFound) // a HEAD answer has no body
	default:
		f.fail(w, http.StatusNotFound, "not_found", "no such route")
	}
}

// storageDB returns a DB (on a scripted realtime server) whose Storage
// talks to the fake storage API.
func storageDB(t testing.TB, f *fakeStorage) *azora.DB {
	return fakeDB(t, azptest.New(t), azora.WithStorageURL(f.srv.URL+"/"))
}

func TestStorageClient(t *testing.T) {
	f := newFakeStorage(t)
	db := storageDB(t, f)
	st := db.Storage()
	require.Same(t, st, db.Storage())

	// Upload: options travel in the query, the hash in a header, the
	// length is known for in-memory readers.
	obj, err := st.Upload(ctx(t), strings.NewReader("hello"), azora.UploadOptions{Bucket: "docs", Path: "a/b.txt", Name: "b.txt",
		ContentType: "text/plain", Overwrite: true, SHA256: strings.Repeat("ab", 32)})
	require.NoError(t, err)
	require.Equal(t, "obj_1", obj.ID)
	require.Equal(t, int64(5), obj.Size)
	require.Equal(t, strings.Repeat("ab", 32), obj.SHA256)
	require.Equal(t, "5", obj.Metadata["content-length"])
	require.Empty(t, obj.ContentType, "no Content-Type header: the declared type is in the query")
	f.mu.Lock()
	require.Equal(t, "POST /storage/v1/b/app/objects?bucket=docs&content_type=text%2Fplain&name=b.txt&overwrite=1&path=a%2Fb.txt", f.requests[0])
	require.Equal(t, "Bearer azk_test", f.auth[0])
	f.mu.Unlock()
	// A reader of unknown length is streamed.
	obj, err = st.Upload(ctx(t), io.MultiReader(strings.NewReader("ab"), strings.NewReader("cd")), azora.UploadOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(4), obj.Size)
	require.Equal(t, "-1", obj.Metadata["content-length"])
	obj, err = st.Upload(ctx(t), io.MultiReader(strings.NewReader("abcd")), azora.UploadOptions{Size: 4})
	require.NoError(t, err)
	require.Equal(t, "4", obj.Metadata["content-length"])

	// Errors are *azora.Error with the HTTP status and the limit.
	_, err = st.Upload(ctx(t), strings.NewReader("too large"), azora.UploadOptions{Bucket: "tiny"})
	var e *azora.Error
	require.ErrorAs(t, err, &e)
	require.Equal(t, azora.Error{Code: azora.CodeTooLarge, Message: "over the limit", HTTPStatus: 413, Limit: "max_object_size", Max: 5}, *e)
	_, err = st.Stat(ctx(t), "missing")
	require.True(t, azora.IsNotFound(err))
	require.ErrorIs(t, err, azora.ErrNotFound)
	_, err = st.Stat(ctx(t), "proxy") // not the API's JSON: a proxy's page
	require.ErrorAs(t, err, &e)
	require.Equal(t, 502, e.HTTPStatus)
	require.Equal(t, azora.CodeInternal, e.Code)

	// Listing pages; the iterator walks them all and can stop early.
	page, err := st.List(ctx(t), azora.ListOptions{Limit: 2, Bucket: "docs", Prefix: "a/", Unreferenced: true})
	require.NoError(t, err)
	require.Len(t, page.Objects, 2)
	require.Equal(t, "2", page.Next)
	f.mu.Lock()
	require.Equal(t, "GET /storage/v1/b/app/objects?bucket=docs&limit=2&prefix=a%2F&unreferenced=1", f.requests[len(f.requests)-1])
	f.mu.Unlock()
	var ids []string
	for o, err := range st.Objects(ctx(t), azora.ListOptions{}) {
		require.NoError(t, err)
		ids = append(ids, o.ID)
	}
	require.Equal(t, []string{"obj_1", "obj_2", "obj_3"}, ids)
	n := 0
	for range st.Objects(ctx(t), azora.ListOptions{}) {
		n++
		break
	}
	require.Equal(t, 1, n)

	// Downloads: ranges become a Range header; the hash comes from the ETag.
	d, err := st.Download(ctx(t), "obj_1")
	require.NoError(t, err)
	body, _ := io.ReadAll(d.Body)
	d.Body.Close()
	require.Equal(t, "content", string(body))
	require.Equal(t, "abc", d.SHA256)
	require.Equal(t, "text/plain", d.ContentType)
	require.False(t, d.Partial)
	require.Equal(t, 2026, d.LastModified.Year())
	for rg, o := range map[string]azora.DownloadOptions{"bytes=5-": {Offset: 5}, "bytes=5-14": {Offset: 5, Length: 10}, "bytes=0-2": {Length: 3}} {
		d, err = st.Download(ctx(t), "obj_1", o)
		require.NoError(t, err)
		d.Body.Close()
		require.True(t, d.Partial)
		require.Equal(t, "echo "+rg, d.ContentRange)
	}
	_, err = st.Download(ctx(t), "obj_1", azora.DownloadOptions{Offset: -1})
	require.ErrorContains(t, err, "negative range")
	var buf bytes.Buffer
	nw, err := st.DownloadTo(ctx(t), "obj_1", &buf)
	require.NoError(t, err)
	require.Equal(t, int64(7), nw)
	_, err = st.DownloadTo(ctx(t), "missing", &buf)
	require.True(t, azora.IsNotFound(err))

	// Throttled requests are retried after the server's hint; uploads,
	// whose body cannot be replayed, are not.
	f.mu.Lock()
	f.throttle = 2
	f.mu.Unlock()
	start := time.Now()
	_, err = st.List(ctx(t), azora.ListOptions{})
	require.NoError(t, err)
	require.GreaterOrEqual(t, time.Since(start), time.Second)
	f.mu.Lock()
	f.throttle = 1
	f.mu.Unlock()
	_, err = st.Upload(ctx(t), strings.NewReader("x"), azora.UploadOptions{})
	require.True(t, azora.IsCode(err, azora.CodeThrottled), "%v", err)
	require.True(t, azora.IsRetryable(err))
	f.mu.Lock()
	f.throttle = 10
	f.mu.Unlock()
	c, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = st.List(c, azora.ListOptions{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	f.mu.Lock()
	f.throttle = 0
	f.mu.Unlock()
}

func TestStorageURLResolution(t *testing.T) {
	srv := azptest.New(t)
	// Over TCP the HTTP address is unknown unless given.
	db := fakeDB(t, srv)
	st := db.Storage()
	_, err := st.Stat(ctx(t), "x")
	require.ErrorIs(t, err, azora.ErrNoStorageURL)
	_, err = st.Upload(ctx(t), strings.NewReader("x"), azora.UploadOptions{})
	require.ErrorIs(t, err, azora.ErrNoStorageURL)
	_, err = st.NewSession(ctx(t), azora.UploadOptions{})
	require.ErrorIs(t, err, azora.ErrNoStorageURL)
	for _, err := range st.Objects(ctx(t), azora.ListOptions{}) {
		require.ErrorIs(t, err, azora.ErrNoStorageURL)
	}

	// Over WebSocket it is the WebSocket URL's origin: the fake realtime
	// server answers 404 there, which shows the request arrived.
	ws, err := azora.Connect(ctx(t), srv.WebSocketURL()+"?db=app&key="+srv.ServerKey(), azora.WithAPIKey("k"), azora.WithLogger(quiet))
	require.NoError(t, err)
	defer ws.Close()
	_, err = ws.Storage().Stat(ctx(t), "x")
	var e *azora.Error
	require.ErrorAs(t, err, &e)
	require.NotZero(t, e.HTTPStatus)

	// A server that is not there is a transport error, not an *Error.
	off := fakeDB(t, srv, azora.WithStorageURL("http://127.0.0.1:1"))
	_, err = off.Storage().Stat(ctx(t), "x")
	require.Error(t, err)
	require.False(t, errors.As(err, &e))
	require.ErrorContains(t, err, "azora: storage")
}

func TestStorageResumableUpload(t *testing.T) {
	f := newFakeStorage(t)
	db := storageDB(t, f)
	st := db.Storage()
	data := make([]byte, 100_000)
	for i := range data {
		data[i] = byte(i % 251)
	}

	// Appends that break half-way: the upload asks the server where it is
	// and continues from there, sending nothing twice.
	f.mu.Lock()
	f.cutAppends = 3
	f.mu.Unlock()
	var progress []int64
	obj, err := st.UploadResumable(ctx(t), bytes.NewReader(data), int64(len(data)), azora.UploadOptions{Name: "big.bin"},
		azora.ResumableOptions{ChunkSize: 30_000, OnProgress: func(sent, total int64) {
			require.Equal(t, int64(len(data)), total)
			progress = append(progress, sent)
		}})
	require.NoError(t, err)
	require.Equal(t, "obj_done", obj.ID)
	f.mu.Lock()
	require.Equal(t, data, f.session, "the server holds exactly the file")
	f.mu.Unlock()
	require.Equal(t, int64(len(data)), progress[len(progress)-1])
	for i := 1; i < len(progress); i++ {
		require.GreaterOrEqual(t, progress[i], progress[i-1])
	}

	// An append the server refuses because its offset moved is corrected
	// from the answer; here the session then holds foreign bytes, which
	// the completion check of a real server would catch.
	sess, err := st.NewSession(ctx(t), azora.UploadOptions{})
	require.NoError(t, err)
	require.Equal(t, f.srv.URL+"/storage/v1/s/app/sid", sess.URL())
	require.Zero(t, sess.Offset())
	require.WithinDuration(t, time.Now().Add(time.Hour), sess.ExpiresAt(), time.Minute)
	f.mu.Lock()
	f.wrongFirst = true
	f.mu.Unlock()
	_, err = sess.Append(ctx(t), strings.NewReader("abcd"))
	require.True(t, azora.IsCode(err, azora.CodeOffsetMismatch), "%v", err)
	require.True(t, azora.IsConflict(err))
	require.Equal(t, int64(4), sess.Offset(), "the answer carried the server's offset")
	off, err := sess.Append(ctx(t), strings.NewReader("efgh"))
	require.NoError(t, err)
	require.Equal(t, int64(8), off)

	// Resuming in "another process": only the URL is needed.
	again, err := azora.ResumeSession(ctx(t), sess.URL(), nil)
	require.NoError(t, err)
	require.Equal(t, int64(8), again.Offset())
	content := []byte("0123efghijklmnop")
	obj, err = again.UploadFrom(ctx(t), bytes.NewReader(content), int64(len(content)), azora.ResumableOptions{ChunkSize: 3})
	require.NoError(t, err)
	require.Equal(t, int64(16), obj.Size)
	f.mu.Lock()
	require.Equal(t, content, f.session)
	f.mu.Unlock()

	// A declared size: completing early is refused and keeps the session.
	sess, err = st.NewSession(ctx(t), azora.UploadOptions{Size: 10})
	require.NoError(t, err)
	require.Equal(t, int64(10), sess.Size())
	_, err = sess.Append(ctx(t), strings.NewReader("12345"))
	require.NoError(t, err)
	_, err = sess.Complete(ctx(t))
	require.True(t, azora.IsCode(err, azora.CodeOffsetMismatch), "%v", err)
	require.Equal(t, int64(5), sess.Offset())
	// More bytes on the server than the file has.
	_, err = sess.UploadFrom(ctx(t), strings.NewReader("123"), 3, azora.ResumableOptions{})
	require.ErrorContains(t, err, "more than the 3 to upload")

	// A session that is gone.
	_, err = azora.ResumeSession(ctx(t), f.srv.URL+"/storage/v1/s/app/gone", nil)
	require.True(t, azora.IsNotFound(err), "%v", err)

	// An upload that never gets anywhere gives up, and hands the session
	// back so the caller can resume or abort it.
	f.mu.Lock()
	f.cutAppends = 1000
	f.mu.Unlock()
	_, err = st.UploadResumable(ctx(t), bytes.NewReader([]byte("x")), 1, azora.UploadOptions{}, azora.ResumableOptions{MaxAttempts: 2})
	var se *azora.SessionError
	require.ErrorAs(t, err, &se)
	require.ErrorContains(t, err, "stalled at byte 0 after 2 attempts")
	require.NotNil(t, se.Session)
	require.Zero(t, se.Session.Offset())
}

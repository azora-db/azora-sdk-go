package azora_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"image"
	"image/png"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	azora "github.com/azora-db/azora-go"
)

// Integration tests: the storage HTTP API of the real engine.

func TestEngineStorageRoundTrip(t *testing.T) {
	f := engine(t)
	db := f.connect(t, azora.WithStorageURL(f.HTTPURL()))
	st := db.Storage()
	prefix := "rt-" + azora.NewMutationID() + "/"

	content := []byte("hello, azora storage\n")
	sum := sha256.Sum256(content)
	obj, err := st.Upload(ctx(t), bytes.NewReader(content), azora.UploadOptions{Path: prefix + "hello.txt", Name: "hello.txt",
		SHA256: hex.EncodeToString(sum[:])})
	require.NoError(t, err)
	require.NotEmpty(t, obj.ID)
	require.Equal(t, "default", obj.Bucket)
	require.Equal(t, prefix+"hello.txt", obj.Path)
	require.Equal(t, "hello.txt", obj.Name)
	require.Equal(t, int64(len(content)), obj.Size)
	require.Equal(t, hex.EncodeToString(sum[:]), obj.SHA256)
	require.Equal(t, "text/plain; charset=utf-8", obj.ContentType, "detected by the server")
	require.WithinDuration(t, time.Now(), obj.CreatedAt, time.Minute)

	got, err := st.Stat(ctx(t), obj.ID)
	require.NoError(t, err)
	require.Equal(t, obj, got)

	// Download: whole, a range, a suffix.
	d, err := st.Download(ctx(t), obj.ID)
	require.NoError(t, err)
	body, err := io.ReadAll(d.Body)
	require.NoError(t, err)
	require.NoError(t, d.Body.Close())
	require.Equal(t, content, body)
	require.Equal(t, obj.SHA256, d.SHA256)
	require.Equal(t, obj.ContentType, d.ContentType)
	require.Equal(t, int64(len(content)), d.Size)
	require.False(t, d.Partial)
	d, err = st.Download(ctx(t), obj.ID, azora.DownloadOptions{Offset: 7, Length: 5})
	require.NoError(t, err)
	body, _ = io.ReadAll(d.Body)
	d.Body.Close()
	require.Equal(t, "azora", string(body))
	require.True(t, d.Partial)
	require.Equal(t, "bytes 7-11/21", d.ContentRange)
	require.Equal(t, int64(5), d.Size)
	d, err = st.Download(ctx(t), obj.ID, azora.DownloadOptions{Offset: 13})
	require.NoError(t, err)
	body, _ = io.ReadAll(d.Body)
	d.Body.Close()
	require.Equal(t, "storage\n", string(body))
	_, err = st.Download(ctx(t), obj.ID, azora.DownloadOptions{Offset: 1000})
	var e *azora.Error
	require.ErrorAs(t, err, &e)
	require.Equal(t, http.StatusRequestedRangeNotSatisfiable, e.HTTPStatus)
	var buf bytes.Buffer
	n, err := st.DownloadTo(ctx(t), obj.ID, &buf)
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, buf.Bytes())

	// A compressible file is stored and sent compressed; the client gets
	// the original bytes.
	text := []byte(strings.Repeat("all work and no play makes jack a dull boy\n", 5000))
	big, err := st.Upload(ctx(t), bytes.NewReader(text), azora.UploadOptions{Path: prefix + "big.txt", ContentType: "text/plain"})
	require.NoError(t, err)
	require.Equal(t, "gzip", big.Encoding)
	require.Less(t, big.StoredSize, big.Size)
	buf.Reset()
	n, err = st.DownloadTo(ctx(t), big.ID, &buf)
	require.NoError(t, err)
	require.Equal(t, int64(len(text)), n)
	require.Equal(t, text, buf.Bytes())
	d, err = st.Download(ctx(t), big.ID)
	require.NoError(t, err)
	d.Body.Close()
	require.Equal(t, big.SHA256, d.SHA256)

	// The path is unique unless Overwrite is set; the hash is checked.
	_, err = st.Upload(ctx(t), strings.NewReader("again"), azora.UploadOptions{Path: prefix + "hello.txt"})
	require.True(t, azora.IsConflict(err), "%v", err)
	require.True(t, azora.IsCode(err, azora.CodeAlreadyExists), "%v", err)
	over, err := st.Upload(ctx(t), strings.NewReader("again"), azora.UploadOptions{Path: prefix + "hello.txt", Overwrite: true, Name: "hello.txt"})
	require.NoError(t, err)
	require.Equal(t, int64(5), over.Size)
	_, err = st.Upload(ctx(t), strings.NewReader("x"), azora.UploadOptions{SHA256: strings.Repeat("0", 64)})
	require.True(t, azora.IsCode(err, azora.CodeChecksumMismatch), "%v", err)
	_, err = st.Upload(ctx(t), strings.NewReader("x"), azora.UploadOptions{Bucket: "no-such-bucket"})
	require.True(t, azora.IsNotFound(err), "%v", err)

	// A file from disk.
	file := filepath.Join(t.TempDir(), "photo.png")
	var pngData bytes.Buffer
	require.NoError(t, png.Encode(&pngData, image.NewRGBA(image.Rect(0, 0, 2, 3))))
	require.NoError(t, os.WriteFile(file, pngData.Bytes(), 0o600))
	img, err := st.UploadFile(ctx(t), file, azora.UploadOptions{Path: prefix + "photo.png"})
	require.NoError(t, err)
	require.Equal(t, "photo.png", img.Name)
	require.Equal(t, "image/png", img.ContentType)
	require.Equal(t, 2, img.Width)
	require.Equal(t, 3, img.Height)
	_, err = st.UploadFile(ctx(t), filepath.Join(t.TempDir(), "missing"), azora.UploadOptions{})
	require.Error(t, err)

	// Listing by prefix, paged.
	var names []string
	for o, err := range st.Objects(ctx(t), azora.ListOptions{Prefix: prefix, Limit: 2}) {
		require.NoError(t, err)
		names = append(names, strings.TrimPrefix(o.Path, prefix))
	}
	require.ElementsMatch(t, []string{"hello.txt", "big.txt", "photo.png"}, names)
	page, err := st.List(ctx(t), azora.ListOptions{Prefix: prefix, Limit: 2})
	require.NoError(t, err)
	require.Len(t, page.Objects, 2)
	require.NotEmpty(t, page.Next)
	page, err = st.List(ctx(t), azora.ListOptions{Prefix: prefix, Limit: 2, After: page.Next})
	require.NoError(t, err)
	require.Len(t, page.Objects, 1)
	require.Empty(t, page.Next)
	page, err = st.List(ctx(t), azora.ListOptions{Prefix: prefix + "nothing"})
	require.NoError(t, err)
	require.Empty(t, page.Objects)

	// Delete: gone for Stat, Download and a second Delete.
	for _, id := range []string{over.ID, big.ID, img.ID} {
		require.NoError(t, st.Delete(ctx(t), id))
	}
	_, err = st.Stat(ctx(t), big.ID)
	require.True(t, azora.IsNotFound(err), "%v", err)
	require.ErrorIs(t, err, azora.ErrNotFound)
	_, err = st.Download(ctx(t), big.ID)
	require.True(t, azora.IsNotFound(err), "%v", err)
	require.True(t, azora.IsNotFound(st.Delete(ctx(t), big.ID)))

	// Scopes: a read key cannot upload, a write key cannot read.
	reader := f.connectURL(t, f.URL(testDB), azora.WithAPIKey(f.ReadKey), azora.WithStorageURL(f.HTTPURL())).Storage()
	_, err = reader.Upload(ctx(t), strings.NewReader("x"), azora.UploadOptions{})
	require.True(t, azora.IsPermissionDenied(err), "%v", err)
	require.ErrorAs(t, err, &e)
	require.Equal(t, http.StatusForbidden, e.HTTPStatus)
	_, err = reader.List(ctx(t), azora.ListOptions{Limit: 1})
	require.NoError(t, err)
	writer := f.connectURL(t, f.URL(testDB), azora.WithAPIKey(f.WriteKey), azora.WithStorageURL(f.HTTPURL())).Storage()
	_, err = writer.List(ctx(t), azora.ListOptions{})
	require.True(t, azora.IsPermissionDenied(err), "%v", err)

	// Over WebSocket the storage address needs no option.
	wsdb := f.connectURL(t, f.WebSocketURL(testDB), azora.WithAPIKey(f.Key))
	_, err = wsdb.Storage().List(ctx(t), azora.ListOptions{Limit: 1})
	require.NoError(t, err)
}

func TestEngineStorageSignedURLs(t *testing.T) {
	f := engine(t)
	db := f.connect(t, azora.WithStorageURL(f.HTTPURL()))
	st := db.Storage()
	prefix := "signed-" + azora.NewMutationID() + "/"
	obj, err := st.Upload(ctx(t), strings.NewReader("signed content"), azora.UploadOptions{Path: prefix + "a.txt", Name: "a.txt"})
	require.NoError(t, err)

	// A signed download link works without credentials until it expires.
	link, err := st.SignURL(ctx(t), obj.ID, azora.SignOptions{TTL: time.Minute})
	require.NoError(t, err)
	require.WithinDuration(t, time.Now().Add(time.Minute), link.ExpiresAt, 5*time.Second)
	require.Equal(t, "signed content", httpGet(t, link.URL))
	dl, err := st.SignURL(ctx(t), obj.ID, azora.SignOptions{Download: true, Filename: "report.txt"})
	require.NoError(t, err)
	resp, err := http.Get(dl.URL)
	require.NoError(t, err)
	resp.Body.Close()
	require.Contains(t, resp.Header.Get("Content-Disposition"), "attachment")
	require.Contains(t, resp.Header.Get("Content-Disposition"), "report.txt")
	// The same over the realtime connection.
	rtLink, exp, err := db.SignURL(ctx(t), obj.ID, 0)
	require.NoError(t, err)
	require.True(t, exp.After(time.Now()))
	require.Equal(t, "signed content", httpGet(t, rtLink))
	_, _, err = db.SignURL(ctx(t), "obj_nope", time.Minute)
	require.Error(t, err)
	// A tampered link, and a link of a deleted object.
	resp, err = http.Get(strings.Replace(link.URL, "sig=", "sig=x", 1))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	_, err = st.SignURL(ctx(t), "obj_nope", azora.SignOptions{})
	require.Error(t, err)
	_, err = st.SignURL(ctx(t), obj.ID, azora.SignOptions{TTL: 1000 * time.Hour})
	require.True(t, azora.IsInvalid(err), "over the maximum lifetime: %v", err)

	// A signed upload link: the grant fixes the path, the size limit and
	// the types; the client only brings the content.
	up, err := st.SignUploadURL(ctx(t), azora.UploadGrant{Path: prefix + "inbox.txt", MaxSize: 64, AllowedTypes: "text/plain",
		Metadata: map[string]string{"by": "test"}, Keep: true}, time.Minute)
	require.NoError(t, err)
	require.Contains(t, up.URL, "/storage/v1/u/"+testDB)
	require.Contains(t, up.SessionURL, "/storage/v1/s/"+testDB)
	require.True(t, up.ExpiresAt.After(time.Now()))
	got, err := azora.UploadSigned(ctx(t), up.URL, strings.NewReader("from a browser"), azora.SignedUploadOptions{Name: "naïve name.txt", ContentType: "text/plain"})
	require.NoError(t, err)
	require.Equal(t, prefix+"inbox.txt", got.Path)
	require.Equal(t, "naïve name.txt", got.Name)
	require.Equal(t, map[string]string{"by": "test"}, got.Metadata)
	body := &bytes.Buffer{}
	_, err = st.DownloadTo(ctx(t), got.ID, body)
	require.NoError(t, err)
	require.Equal(t, "from a browser", body.String())
	// The grant's limits apply: the path is taken, the size, the type.
	_, err = azora.UploadSigned(ctx(t), up.URL, strings.NewReader("second"), azora.SignedUploadOptions{ContentType: "text/plain"})
	require.True(t, azora.IsConflict(err), "%v", err)
	limited, err := st.SignUploadURL(ctx(t), azora.UploadGrant{MaxSize: 8, AllowedTypes: "text/plain"}, time.Minute)
	require.NoError(t, err)
	_, err = azora.UploadSigned(ctx(t), limited.URL, strings.NewReader("more than eight bytes"), azora.SignedUploadOptions{ContentType: "text/plain"})
	var e *azora.Error
	require.ErrorAs(t, err, &e)
	require.Equal(t, azora.CodeTooLarge, e.Code)
	require.Equal(t, "grant_max_size", e.Limit)
	require.Equal(t, int64(8), e.Max)
	_, err = azora.UploadSigned(ctx(t), limited.URL, strings.NewReader("\x89PNG\r\n\x1a\n"), azora.SignedUploadOptions{ContentType: "image/png"})
	require.True(t, azora.IsCode(err, azora.CodeTypeNotAllowed), "%v", err)
	_, err = azora.UploadSigned(ctx(t), strings.Replace(limited.URL, "sig=", "sig=x", 1), strings.NewReader("x"), azora.SignedUploadOptions{})
	require.True(t, azora.IsCode(err, azora.CodeInvalidSignature), "%v", err)
	require.True(t, azora.IsPermissionDenied(err) || e != nil)

	// A signed upload that fills a row's file column.
	mustExec(t, db, "DROP TABLE IF EXISTS profiles")
	mustExec(t, db, "CREATE TABLE profiles (id INTEGER PRIMARY KEY, avatar FILE)")
	mustExec(t, db, "INSERT INTO profiles (id) VALUES (42)")
	rowUp, err := st.SignUploadURL(ctx(t), azora.UploadGrant{Table: "profiles", Column: "avatar", Key: "id", KeyValue: "42"}, time.Minute)
	require.NoError(t, err)
	avatar, err := azora.UploadSigned(ctx(t), rowUp.URL, strings.NewReader("avatar bytes"), azora.SignedUploadOptions{Name: "me.bin"})
	require.NoError(t, err)
	require.Equal(t, int64(1), avatar.Refs, "the row references the object")
	ref, err := azora.Scalar[string](ctx(t), db, "SELECT avatar FROM profiles WHERE id = 42")
	require.NoError(t, err)
	require.Equal(t, avatar.ID, ref)
	// A referenced object is only deleted with Force, which clears the column.
	err = st.Delete(ctx(t), avatar.ID)
	require.True(t, azora.IsCode(err, azora.CodeInUse), "%v", err)
	require.True(t, azora.IsConflict(err))
	require.NoError(t, st.Delete(ctx(t), avatar.ID, azora.DeleteOptions{Force: true}))
	var cleared *string
	require.NoError(t, db.Get(ctx(t), &cleared, "SELECT avatar FROM profiles WHERE id = 42"))
	require.Nil(t, cleared)
	mustExec(t, db, "DROP TABLE profiles")

	require.NoError(t, st.Delete(ctx(t), obj.ID))
	resp, err = http.Get(link.URL)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode, "deleting the object ends its links")
	require.NoError(t, st.Delete(ctx(t), got.ID))
}

func TestEngineStorageSessions(t *testing.T) {
	f := engine(t)
	db := f.connect(t, azora.WithStorageURL(f.HTTPURL()))
	st := db.Storage()
	data := make([]byte, 700_000)
	for i := range data {
		data[i] = byte(i*7 + i/251)
	}
	sum := sha256.Sum256(data)

	// By hand: open, append in pieces, look at the offset, complete.
	sess, err := st.NewSession(ctx(t), azora.UploadOptions{Name: "pieces.bin", Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:])})
	require.NoError(t, err)
	require.Contains(t, sess.URL(), "/storage/v1/s/"+testDB+"/")
	require.Equal(t, int64(len(data)), sess.Size())
	require.True(t, sess.ExpiresAt().After(time.Now().Add(time.Hour)))
	off, err := sess.Append(ctx(t), bytes.NewReader(data[:100_000]))
	require.NoError(t, err)
	require.Equal(t, int64(100_000), off)
	// Completing early is refused and keeps the session.
	_, err = sess.Complete(ctx(t))
	require.True(t, azora.IsCode(err, azora.CodeOffsetMismatch), "%v", err)
	// "Another process" resumes with the URL alone and sends the rest.
	again, err := azora.ResumeSession(ctx(t), sess.URL(), nil)
	require.NoError(t, err)
	require.Equal(t, int64(100_000), again.Offset())
	require.Equal(t, int64(len(data)), again.Size())
	// An append at the wrong offset is refused with the right one.
	stale, err := azora.ResumeSession(ctx(t), sess.URL(), nil)
	require.NoError(t, err)
	_, err = again.Append(ctx(t), bytes.NewReader(data[100_000:150_000]))
	require.NoError(t, err)
	_, err = stale.Append(ctx(t), bytes.NewReader(data[100_000:150_000]))
	require.True(t, azora.IsCode(err, azora.CodeOffsetMismatch), "%v", err)
	require.Equal(t, int64(150_000), stale.Offset())
	var steps int
	obj, err := again.UploadFrom(ctx(t), bytes.NewReader(data), int64(len(data)), azora.ResumableOptions{ChunkSize: 128 << 10,
		OnProgress: func(sent, total int64) { steps++ }})
	require.NoError(t, err)
	require.Equal(t, int64(len(data)), obj.Size)
	require.Equal(t, hex.EncodeToString(sum[:]), obj.SHA256)
	require.Equal(t, "pieces.bin", obj.Name)
	require.Greater(t, steps, 3)
	var buf bytes.Buffer
	_, err = st.DownloadTo(ctx(t), obj.ID, &buf)
	require.NoError(t, err)
	require.Equal(t, data, buf.Bytes())
	// The session is gone once completed.
	_, err = azora.ResumeSession(ctx(t), sess.URL(), nil)
	require.True(t, azora.IsNotFound(err), "%v", err)
	require.NoError(t, st.Delete(ctx(t), obj.ID))

	// In one call.
	obj, err = st.UploadResumable(ctx(t), bytes.NewReader(data), int64(len(data)), azora.UploadOptions{Name: "one-call.bin"},
		azora.ResumableOptions{ChunkSize: 200_000})
	require.NoError(t, err)
	require.Equal(t, hex.EncodeToString(sum[:]), obj.SHA256)
	require.NoError(t, st.Delete(ctx(t), obj.ID))

	// Abort discards the bytes.
	sess, err = st.NewSession(ctx(t), azora.UploadOptions{})
	require.NoError(t, err)
	_, err = sess.Append(ctx(t), strings.NewReader("discard me"))
	require.NoError(t, err)
	require.NoError(t, sess.Abort(ctx(t)))
	_, err = sess.Sync(ctx(t))
	require.True(t, azora.IsNotFound(err), "%v", err)
	require.True(t, azora.IsNotFound(sess.Abort(ctx(t))))

	// A session under a signed grant, as a client without credentials
	// would run it.
	up, err := st.SignUploadURL(ctx(t), azora.UploadGrant{MaxSize: 1 << 20, Keep: true}, time.Minute)
	require.NoError(t, err)
	signed, err := azora.OpenSignedSession(ctx(t), up.SessionURL, azora.SignedUploadOptions{Name: "signed.bin", Size: int64(len(data)),
		ContentType: "application/x-test"})
	require.NoError(t, err)
	obj, err = signed.UploadFrom(ctx(t), bytes.NewReader(data), int64(len(data)), azora.ResumableOptions{ChunkSize: 300_000})
	require.NoError(t, err)
	require.Equal(t, "signed.bin", obj.Name)
	require.Equal(t, "application/x-test", obj.ContentType)
	require.Equal(t, hex.EncodeToString(sum[:]), obj.SHA256)
	require.NoError(t, st.Delete(ctx(t), obj.ID))
	// The grant's size limit applies to sessions when they are opened.
	_, err = azora.OpenSignedSession(ctx(t), up.SessionURL, azora.SignedUploadOptions{Size: 2 << 20})
	require.True(t, azora.IsCode(err, azora.CodeTooLarge), "%v", err)
	_, err = azora.OpenSignedSession(ctx(t), strings.Replace(up.SessionURL, "sig=", "sig=x", 1), azora.SignedUploadOptions{})
	require.True(t, azora.IsCode(err, azora.CodeInvalidSignature), "%v", err)
}

// Stored files are rows too: a files subscription sees uploads and
// deletions as they happen.
func TestEngineStorageFilesSubscription(t *testing.T) {
	f := engine(t)
	db := f.connect(t, azora.WithStorageURL(f.HTTPURL()))
	st := db.Storage()
	prefix := "live-" + azora.NewMutationID() + "/"
	type file struct {
		ID   string
		Path string
		Name string
		Size int64
	}
	files, err := azora.Live[file](ctx(t), db, azora.FilesSpec().WithPathPrefix(prefix+"avatars/"))
	require.NoError(t, err)
	defer files.Close()
	require.NoError(t, files.Wait(ctx(t)))
	require.Zero(t, files.Len())

	a, err := st.Upload(ctx(t), strings.NewReader("png"), azora.UploadOptions{Path: prefix + "avatars/u1.png", Name: "u1.png"})
	require.NoError(t, err)
	other, err := st.Upload(ctx(t), strings.NewReader("doc"), azora.UploadOptions{Path: prefix + "docs/a.txt"})
	require.NoError(t, err)
	eventually(t, 10*time.Second, func() bool { return files.Len() == 1 }, "the upload appears")
	got, ok := files.Get(a.ID)
	require.True(t, ok)
	require.Equal(t, file{ID: a.ID, Path: prefix + "avatars/u1.png", Name: "u1.png", Size: 3}, got)
	res, err := files.Verify(ctx(t))
	require.NoError(t, err)
	require.True(t, res.Match, "%+v", res)

	require.NoError(t, st.Delete(ctx(t), a.ID))
	eventually(t, 10*time.Second, func() bool { return files.Len() == 0 }, "the deletion appears")
	require.NoError(t, st.Delete(ctx(t), other.ID))
}

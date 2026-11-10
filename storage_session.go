package azora

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// DefaultChunkSize is the size of the appends UploadFrom makes.
const DefaultChunkSize = 8 << 20

// UploadSession is a resumable upload: one file received by the server
// in any number of appends, over as many connections as it takes. Its
// URL is the only credential the appends, the completion and the abort
// need, so keep it private; store it to resume the upload in another
// process with ResumeSession.
//
// A session expires 24 hours (storage.upload_session_ttl) after it was
// opened, whatever its progress. The methods are safe for concurrent
// use, but appends are sequential by nature: one at a time.
type UploadSession struct {
	url  string
	id   string
	http *http.Client

	mu      sync.Mutex
	offset  int64
	size    int64
	expires time.Time
}

// sessionJSON is the server's description of a session.
type sessionJSON struct {
	ID        string    `json:"id"`
	URL       string    `json:"url"`
	Size      int64     `json:"size"`
	Offset    int64     `json:"offset"`
	ExpiresAt time.Time `json:"expires_at"`
}

func newSession(resp *http.Response, hc *http.Client) (*UploadSession, error) {
	var body struct {
		Session *sessionJSON `json:"session"`
	}
	loc := resp.Header.Get("Location")
	if err := decodeJSON(resp, &body); err != nil {
		return nil, err
	}
	if body.Session == nil || (body.Session.URL == "" && loc == "") {
		return nil, errors.New("azora: storage: the response holds no session")
	}
	s := body.Session
	if s.URL == "" {
		s.URL = loc
	}
	return &UploadSession{url: s.URL, id: s.ID, http: hc, offset: s.Offset, size: s.Size, expires: s.ExpiresAt}, nil
}

// NewSession opens a resumable upload with the DB's API key. Set
// UploadOptions.Size to the file's total size when known: the server
// then refuses an oversized file at once and a completion before every
// byte arrived.
func (s *Storage) NewSession(ctx context.Context, o UploadOptions) (*UploadSession, error) {
	hd := http.Header{}
	if o.SHA256 != "" {
		hd.Set("X-Azora-SHA256", o.SHA256)
	}
	if o.Size > 0 {
		hd.Set("Upload-Length", strconv.FormatInt(o.Size, 10))
	}
	resp, err := s.do(ctx, request{method: http.MethodPost, path: "/sessions", query: o.query(), header: hd}, http.StatusCreated)
	if err != nil {
		return nil, err
	}
	return newSession(resp, s.http)
}

// OpenSignedSession opens a resumable upload under a signed grant: pass
// the SessionURL of a SignedUpload. No credential is needed.
func OpenSignedSession(ctx context.Context, sessionURL string, o SignedUploadOptions) (*UploadSession, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sessionURL, nil)
	if err != nil {
		return nil, fmt.Errorf("azora: storage: %w", err)
	}
	setFileHeaders(req.Header, o)
	if o.ContentType != "" {
		req.Header.Set("X-Azora-Content-Type", o.ContentType)
	}
	if o.Size > 0 {
		req.Header.Set("Upload-Length", strconv.FormatInt(o.Size, 10))
	}
	resp, err := o.client().Do(req)
	if err != nil {
		return nil, transportErr(ctx, err)
	}
	if resp.StatusCode != http.StatusCreated {
		return nil, responseError(resp)
	}
	return newSession(resp, o.client())
}

// ResumeSession reattaches to a session by its URL (UploadSession.URL),
// asking the server how many bytes it holds: continue with UploadFrom or
// Append. hc may be nil (http.DefaultClient). A session that expired or
// was completed is a not_found error.
func ResumeSession(ctx context.Context, sessionURL string, hc *http.Client) (*UploadSession, error) {
	if hc == nil {
		hc = http.DefaultClient
	}
	u := &UploadSession{url: sessionURL, http: hc}
	if _, err := u.Sync(ctx); err != nil {
		return nil, err
	}
	return u, nil
}

// URL returns the session's URL: the capability to append to, complete
// or abort it.
func (u *UploadSession) URL() string { return u.url }

// Offset returns the number of bytes the server holds, as last reported:
// where the next append starts.
func (u *UploadSession) Offset() int64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.offset
}

// Size returns the total size declared when the session was opened (0:
// none), and ExpiresAt when the session ends.
func (u *UploadSession) Size() int64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.size
}

// ExpiresAt returns when the server discards the session.
func (u *UploadSession) ExpiresAt() time.Time {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.expires
}

// note records the session state a response reports in its headers.
func (u *UploadSession) note(hd http.Header) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if n, err := strconv.ParseInt(hd.Get("Upload-Offset"), 10, 64); err == nil && n >= 0 {
		u.offset = n
	}
	if n, err := strconv.ParseInt(hd.Get("Upload-Length"), 10, 64); err == nil && n > 0 {
		u.size = n
	}
	if t, err := http.ParseTime(hd.Get("Upload-Expires")); err == nil {
		u.expires = t
	}
}

// Sync asks the server for the session's offset (after a failed append,
// the server keeps the bytes that arrived).
func (u *UploadSession) Sync(ctx context.Context) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, u.url, nil)
	if err != nil {
		return 0, fmt.Errorf("azora: storage: %w", err)
	}
	resp, err := u.http.Do(req)
	if err != nil {
		return 0, transportErr(ctx, err)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, responseError(resp)
	}
	resp.Body.Close()
	u.note(resp.Header)
	return u.Offset(), nil
}

// Append sends the content of r as the next bytes of the file and
// returns the new offset. When it fails, part of r may have been stored:
// call Sync to learn the offset to continue from (an offset_mismatch
// error already carries it: Offset is up to date).
func (u *UploadSession) Append(ctx context.Context, r io.Reader) (int64, error) {
	return u.appendAt(ctx, u.Offset(), r, bodyLength(r, 0))
}

func (u *UploadSession) appendAt(ctx context.Context, offset int64, r io.Reader, length int64) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, u.url, io.NopCloser(r))
	if err != nil {
		return 0, fmt.Errorf("azora: storage: %w", err)
	}
	req.ContentLength = length
	req.Header.Set("Upload-Offset", strconv.FormatInt(offset, 10))
	req.Header.Set("Content-Type", "application/offset+octet-stream")
	resp, err := u.http.Do(req)
	if err != nil {
		return u.Offset(), transportErr(ctx, err)
	}
	u.note(resp.Header)
	if resp.StatusCode != http.StatusNoContent {
		return u.Offset(), responseError(resp)
	}
	resp.Body.Close()
	return u.Offset(), nil
}

// Complete stores the uploaded bytes as an object and ends the session.
// With a declared size, completing before every byte arrived fails with
// offset_mismatch and keeps the session.
func (u *UploadSession) Complete(ctx context.Context) (*Object, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.url+"/complete", nil)
	if err != nil {
		return nil, fmt.Errorf("azora: storage: %w", err)
	}
	resp, err := u.http.Do(req)
	if err != nil {
		return nil, transportErr(ctx, err)
	}
	if resp.StatusCode != http.StatusCreated {
		u.note(resp.Header)
		return nil, responseError(resp)
	}
	return objectOf(resp)
}

// Abort discards the session and its bytes.
func (u *UploadSession) Abort(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u.url, nil)
	if err != nil {
		return fmt.Errorf("azora: storage: %w", err)
	}
	resp, err := u.http.Do(req)
	if err != nil {
		return transportErr(ctx, err)
	}
	if resp.StatusCode != http.StatusNoContent {
		return responseError(resp)
	}
	return resp.Body.Close()
}

// ResumableOptions tune UploadFrom and UploadResumable.
type ResumableOptions struct {
	// ChunkSize is the size of each append (default DefaultChunkSize).
	// After a failure at most one chunk is sent again.
	ChunkSize int64
	// MaxAttempts bounds consecutive failed appends without progress
	// before the upload gives up (default 5). Progress resets the count.
	MaxAttempts int
	// OnProgress, when set, is called after every append with the bytes
	// the server holds and the total.
	OnProgress func(sent, total int64)
}

// UploadFrom sends the bytes of r that the server does not hold yet
// (from the session's offset to size) in chunks, then completes the
// session and returns the stored object. A failed append (a dropped
// connection, a server restart) is recovered from: UploadFrom asks the
// server for its offset and continues from there, with backoff, until
// MaxAttempts appends in a row made no progress.
//
// If UploadFrom fails, the session is kept: call it again later, in this
// process or after ResumeSession in another, and it continues where the
// server is. r must be the same content every time.
func (u *UploadSession) UploadFrom(ctx context.Context, r io.ReaderAt, size int64, o ResumableOptions) (*Object, error) {
	chunk := o.ChunkSize
	if chunk <= 0 {
		chunk = DefaultChunkSize
	}
	attempts := o.MaxAttempts
	if attempts <= 0 {
		attempts = 5
	}
	offset, err := u.Sync(ctx)
	if err != nil {
		return nil, err
	}
	failures := 0
	for offset < size {
		n := min(chunk, size-offset)
		got, err := u.appendAt(ctx, offset, io.NewSectionReader(r, offset, n), n)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if !recoverable(err) {
				return nil, err
			}
			// Bytes that arrived before the failure are kept: ask where
			// the server is.
			if synced, serr := u.Sync(ctx); serr == nil {
				got = synced
			} else if !recoverable(serr) {
				return nil, serr
			}
			if got <= offset {
				if failures++; failures >= attempts {
					return nil, fmt.Errorf("azora: storage: upload stalled at byte %d after %d attempts: %w", offset, failures, err)
				}
				if err := sleepCtx(ctx, time.Duration(failures)*200*time.Millisecond); err != nil {
					return nil, err
				}
			}
		}
		if got > offset {
			failures = 0
		}
		offset = got
		if o.OnProgress != nil {
			o.OnProgress(offset, size)
		}
	}
	if offset > size {
		return nil, fmt.Errorf("azora: storage: the session holds %d bytes, more than the %d to upload", offset, size)
	}
	return u.Complete(ctx)
}

// recoverable reports append failures worth another attempt: transport
// errors, an offset the server corrected, and answers that ask to retry.
func recoverable(err error) bool {
	var e *Error
	if !errors.As(err, &e) {
		return true // the connection failed
	}
	switch e.Code {
	case CodeOffsetMismatch, CodeThrottled, CodeUnavailable, "busy", "timeout", "incomplete_body":
		return true
	}
	return e.HTTPStatus >= 500 && e.Code != "corrupt"
}

// UploadResumable stores size bytes of r through a resumable session:
// the way to send large files over connections that may break. It opens
// a session, uploads in chunks with recovery (see
// UploadSession.UploadFrom) and completes it. o.Size is set to size.
//
// To be able to continue after a crash of your own process, open the
// session yourself with NewSession, persist its URL, and use
// ResumeSession and UploadFrom.
func (s *Storage) UploadResumable(ctx context.Context, r io.ReaderAt, size int64, o UploadOptions, ro ResumableOptions) (*Object, error) {
	o.Size = size
	sess, err := s.NewSession(ctx, o)
	if err != nil {
		return nil, err
	}
	obj, err := sess.UploadFrom(ctx, r, size, ro)
	if err != nil {
		return nil, &SessionError{Session: sess, Err: err}
	}
	return obj, nil
}

// SessionError is returned by UploadResumable when the upload failed
// after its session was opened: Session can still be resumed (UploadFrom)
// or aborted.
type SessionError struct {
	Session *UploadSession
	Err     error
}

func (e *SessionError) Error() string { return e.Err.Error() }

// Unwrap returns the underlying error.
func (e *SessionError) Unwrap() error { return e.Err }

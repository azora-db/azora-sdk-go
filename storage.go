package azora

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Storage is the client of the server's file storage HTTP API for the
// DB's database. Get one with DB.Storage. It authenticates with the DB's
// credential, which must be an API key (client tokens cannot use the
// HTTP API: give those signed URLs, see DB.SignURL, UploadSigned and
// OpenSignedSession). Every method is safe for concurrent use.
//
// Unlike the realtime connection, HTTP requests are protected by TLS
// only (use an https:// storage URL in production); file content is not
// end to end encrypted.
type Storage struct {
	db   *DB
	base string // "" when unknown
	http *http.Client
}

// Storage returns the client of the database's file storage. Its HTTP
// address comes from WithStorageURL (Config.StorageURL) or, for WebSocket
// connections, from the WebSocket URL; without one every method returns
// ErrNoStorageURL.
func (db *DB) Storage() *Storage {
	db.storageOnce.Do(func() {
		db.storage = &Storage{db: db, base: db.cfg.StorageURL, http: db.http}
	})
	return db.storage
}

// Object describes a stored file.
type Object struct {
	ID          string            `json:"id"`
	Bucket      string            `json:"bucket"`
	Path        string            `json:"path,omitempty"`
	Name        string            `json:"name"`
	ContentType string            `json:"content_type"`
	Size        int64             `json:"size"`
	SHA256      string            `json:"sha256"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	// Width and Height are set for PNG, JPEG and GIF images.
	Width  int `json:"width,omitempty"`
	Height int `json:"height,omitempty"`
	// Refs counts the rows whose file columns reference the object.
	Refs       int64     `json:"refs"`
	AutoDelete bool      `json:"auto_delete"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
	// StoredSize, Encoding and Inline say how the content is stored
	// (compressed, inside the database).
	StoredSize int64  `json:"stored_size"`
	Encoding   string `json:"encoding"`
	Inline     bool   `json:"inline"`
}

// maxErrorBody bounds how much of an error response is read.
const maxErrorBody = 64 << 10

// storageRetries is how often a request the server throttled (429) or
// could not serve for now (503 with Retry-After) is sent again.
const storageRetries = 3

// request is one call of the storage API.
type request struct {
	method string
	path   string // below /storage/v1/b/{db}
	query  url.Values
	header http.Header
	json   any       // JSON body
	body   io.Reader // raw body (not replayable: never retried)
	length int64     // of body; 0 = unknown
}

// url builds the request URL.
func (s *Storage) url(path string, q url.Values) (string, error) {
	if s.base == "" {
		return "", ErrNoStorageURL
	}
	u := s.base + "/storage/v1/b/" + url.PathEscape(s.db.cfg.Database) + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	return u, nil
}

// bearer returns the credential for the Authorization header.
func (s *Storage) bearer() string {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	return s.db.cred
}

// do sends r with the API key and returns the response when its status
// is one of want; any other answer is returned as an *Error. Requests
// without a raw body are retried when the server asks to (429, and 503
// with Retry-After), after the delay it names.
func (s *Storage) do(ctx context.Context, r request, want ...int) (*http.Response, error) {
	u, err := s.url(r.path, r.query)
	if err != nil {
		return nil, err
	}
	var payload []byte
	if r.json != nil {
		if payload, err = json.Marshal(r.json); err != nil {
			return nil, fmt.Errorf("azora: storage: %w", err)
		}
	}
	for attempt := 0; ; attempt++ {
		var body io.Reader = r.body
		if payload != nil {
			body = bytes.NewReader(payload)
		}
		req, err := http.NewRequestWithContext(ctx, r.method, u, body)
		if err != nil {
			return nil, fmt.Errorf("azora: storage: %w", err)
		}
		for k, vs := range r.header {
			req.Header[k] = vs
		}
		if payload != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if r.body != nil && r.length > 0 {
			req.ContentLength = r.length
		}
		req.Header.Set("Authorization", "Bearer "+s.bearer())
		resp, err := s.http.Do(req)
		if err != nil {
			return nil, transportErr(ctx, err)
		}
		for _, st := range want {
			if resp.StatusCode == st {
				return resp, nil
			}
		}
		retryHint := resp.Header.Get("Retry-After") != ""
		e := responseError(resp)
		if r.body == nil && attempt < storageRetries && (resp.StatusCode == http.StatusTooManyRequests ||
			resp.StatusCode == http.StatusServiceUnavailable && retryHint) {
			if err := sleepCtx(ctx, max(e.RetryAfter, time.Duration(attempt+1)*50*time.Millisecond)); err != nil {
				return nil, err
			}
			continue
		}
		return nil, e
	}
}

// transportErr reports a failed HTTP exchange, as the context's error
// when that is why it failed.
func transportErr(ctx context.Context, err error) error {
	if cerr := ctx.Err(); cerr != nil {
		return cerr
	}
	return fmt.Errorf("azora: storage: %w", err)
}

// responseError turns an error response into an *Error and closes it.
func responseError(resp *http.Response) *Error {
	defer resp.Body.Close()
	e := &Error{HTTPStatus: resp.StatusCode}
	data, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	var body struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
			Limit   string `json:"limit"`
			Max     int64  `json:"max"`
		} `json:"error"`
	}
	if json.Unmarshal(data, &body) == nil && body.Error.Code != "" {
		e.Code, e.Message, e.Limit, e.Max = body.Error.Code, body.Error.Message, body.Error.Limit, body.Error.Max
	} else {
		// Not the storage API's answer: a proxy, or a HEAD request.
		e.Code, e.Message = codeOfStatus(resp.StatusCode), "HTTP "+resp.Status
	}
	if v := resp.Header.Get("Retry-After"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
			e.RetryAfter = time.Duration(secs) * time.Second
		}
	}
	return e
}

// codeOfStatus names an HTTP status that came without a JSON error.
func codeOfStatus(status int) string {
	switch status {
	case http.StatusBadRequest:
		return CodeInvalid
	case http.StatusUnauthorized:
		return CodeUnauthenticated
	case http.StatusForbidden:
		return CodeForbidden
	case http.StatusNotFound:
		return CodeNotFound
	case http.StatusConflict:
		return CodeConflict
	case http.StatusRequestEntityTooLarge:
		return CodeTooLarge
	case http.StatusTooManyRequests:
		return CodeThrottled
	case http.StatusServiceUnavailable:
		return CodeUnavailable
	}
	return CodeInternal
}

// decodeJSON reads a JSON response into v and closes it.
func decodeJSON(resp *http.Response, v any) error {
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		return fmt.Errorf("azora: storage: decode response: %w", err)
	}
	return nil
}

// objectOf reads a {"object": ...} response.
func objectOf(resp *http.Response) (*Object, error) {
	var body struct {
		Object *Object `json:"object"`
	}
	if err := decodeJSON(resp, &body); err != nil {
		return nil, err
	}
	if body.Object == nil {
		return nil, fmt.Errorf("azora: storage: the response holds no object")
	}
	return body.Object, nil
}

// Stat returns an object's description. A missing object is a not_found
// error (IsNotFound).
func (s *Storage) Stat(ctx context.Context, id string) (*Object, error) {
	resp, err := s.do(ctx, request{method: http.MethodGet, path: "/objects/" + url.PathEscape(id)}, http.StatusOK)
	if err != nil {
		return nil, err
	}
	return objectOf(resp)
}

// DeleteOptions tune Delete.
type DeleteOptions struct {
	// Force deletes an object that rows still reference, setting the
	// referencing columns to NULL. Without it such a delete fails with an
	// in_use error (IsConflict).
	Force bool
}

// Delete deletes an object. Signed links to it stop working at once.
func (s *Storage) Delete(ctx context.Context, id string, opts ...DeleteOptions) error {
	q := url.Values{}
	if len(opts) > 0 && opts[0].Force {
		q.Set("force", "1")
	}
	resp, err := s.do(ctx, request{method: http.MethodDelete, path: "/objects/" + url.PathEscape(id), query: q}, http.StatusNoContent)
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

// ListOptions select the objects List returns.
type ListOptions struct {
	// Bucket and Prefix (of the object's path) narrow the listing.
	Bucket, Prefix string
	// After is the cursor: objects with a larger id (ids grow with time).
	// Pass ObjectPage.Next to read the following page.
	After string
	// Limit is the page size (default 100, at most 1000).
	Limit int
	// Unreferenced keeps only objects that no file column references.
	Unreferenced bool
}

// ObjectPage is one page of a listing.
type ObjectPage struct {
	Objects []Object `json:"objects"`
	// Next is the After of the following page ("" on the last one).
	Next string `json:"next"`
}

// List returns one page of objects, in id (creation) order.
func (s *Storage) List(ctx context.Context, o ListOptions) (*ObjectPage, error) {
	q := url.Values{}
	if o.Bucket != "" {
		q.Set("bucket", o.Bucket)
	}
	if o.Prefix != "" {
		q.Set("prefix", o.Prefix)
	}
	if o.After != "" {
		q.Set("after", o.After)
	}
	if o.Limit > 0 {
		q.Set("limit", strconv.Itoa(o.Limit))
	}
	if o.Unreferenced {
		q.Set("unreferenced", "1")
	}
	resp, err := s.do(ctx, request{method: http.MethodGet, path: "/objects", query: q}, http.StatusOK)
	if err != nil {
		return nil, err
	}
	var page ObjectPage
	if err := decodeJSON(resp, &page); err != nil {
		return nil, err
	}
	return &page, nil
}

// Objects iterates over every object the options select, fetching pages
// as the loop advances. It ends after the last object, or with a non-nil
// error.
//
//	for obj, err := range st.Objects(ctx, azora.ListOptions{Bucket: "avatars"}) {
//		if err != nil { return err }
//		fmt.Println(obj.ID, obj.Name)
//	}
func (s *Storage) Objects(ctx context.Context, o ListOptions) iter.Seq2[Object, error] {
	return func(yield func(Object, error) bool) {
		for {
			page, err := s.List(ctx, o)
			if err != nil {
				yield(Object{}, err)
				return
			}
			for _, obj := range page.Objects {
				if !yield(obj, nil) {
					return
				}
			}
			if page.Next == "" {
				return
			}
			o.After = page.Next
		}
	}
}

// SignOptions tune SignURL.
type SignOptions struct {
	// TTL is the link's lifetime (0: the server's storage.signed_url_ttl,
	// 15 minutes by default; more than storage.max_signed_url_ttl is an
	// error). It is rounded up to whole seconds.
	TTL time.Duration
	// Download makes browsers save the file instead of showing it;
	// Filename overrides the name it is saved under.
	Download bool
	Filename string
}

// SignedURL is a signed download link.
type SignedURL struct {
	URL       string    `json:"url"`
	ExpiresAt time.Time `json:"expires_at"`
}

// SignURL returns a signed link that lets anyone download the object
// until it expires, without credentials. The link cannot be revoked
// except by deleting the object.
func (s *Storage) SignURL(ctx context.Context, id string, o SignOptions) (SignedURL, error) {
	var out SignedURL
	body := map[string]any{"id": id, "ttl_seconds": ttlSeconds(o.TTL), "download": o.Download, "filename": o.Filename}
	resp, err := s.do(ctx, request{method: http.MethodPost, path: "/sign", json: body}, http.StatusOK)
	if err != nil {
		return out, err
	}
	err = decodeJSON(resp, &out)
	return out, err
}

// ttlSeconds rounds a lifetime up to whole seconds.
func ttlSeconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Second - 1) / time.Second)
}

// Download is the content of an object, or of a range of it. Close Body
// when done.
type Download struct {
	Body io.ReadCloser
	// Size is the number of bytes Body will yield, or -1 when the server
	// sent the content compressed (Go's HTTP client then decompresses it
	// on the fly and the length is not known in advance; Stat tells it).
	Size        int64
	ContentType string
	// SHA256 is the content's hash (of the whole object, also for a
	// range).
	SHA256       string
	LastModified time.Time
	// Partial reports a range response; ContentRange is then its
	// "bytes start-end/total" header.
	Partial      bool
	ContentRange string
}

// DownloadOptions select a part of an object.
type DownloadOptions struct {
	// Offset is the first byte to read; Length the number of bytes (0:
	// to the end). Both zero reads the whole object.
	Offset, Length int64
}

// Download opens an object's content, optionally a byte range of it:
//
//	d, err := st.Download(ctx, id)
//	defer d.Body.Close()
//	io.Copy(w, d.Body)
//
// The server checks the content against its hash while sending it and
// aborts the transfer if the stored file is damaged, so a body read to
// io.EOF without error is complete.
func (s *Storage) Download(ctx context.Context, id string, opts ...DownloadOptions) (*Download, error) {
	hd := http.Header{}
	if len(opts) > 0 {
		o := opts[0]
		if o.Offset < 0 || o.Length < 0 {
			return nil, fmt.Errorf("azora: storage: negative range")
		}
		switch {
		case o.Length > 0:
			hd.Set("Range", fmt.Sprintf("bytes=%d-%d", o.Offset, o.Offset+o.Length-1))
		case o.Offset > 0:
			hd.Set("Range", fmt.Sprintf("bytes=%d-", o.Offset))
		}
	}
	resp, err := s.do(ctx, request{method: http.MethodGet, path: "/objects/" + url.PathEscape(id) + "/content", header: hd},
		http.StatusOK, http.StatusPartialContent)
	if err != nil {
		return nil, err
	}
	d := &Download{Body: resp.Body, Size: resp.ContentLength, ContentType: resp.Header.Get("Content-Type"),
		Partial: resp.StatusCode == http.StatusPartialContent, ContentRange: resp.Header.Get("Content-Range")}
	etag := strings.TrimPrefix(resp.Header.Get("ETag"), "W/")
	d.SHA256 = strings.TrimSuffix(strings.Trim(etag, `"`), "-gz")
	if t, err := http.ParseTime(resp.Header.Get("Last-Modified")); err == nil {
		d.LastModified = t
	}
	return d, nil
}

// DownloadTo writes an object's whole content to w and returns the
// number of bytes written.
func (s *Storage) DownloadTo(ctx context.Context, id string, w io.Writer) (int64, error) {
	d, err := s.Download(ctx, id)
	if err != nil {
		return 0, err
	}
	defer d.Body.Close()
	n, err := io.Copy(w, d.Body)
	if err != nil {
		return n, transportErr(ctx, err)
	}
	return n, nil
}

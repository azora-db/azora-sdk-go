package azora

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

// UploadOptions describe a file to store.
type UploadOptions struct {
	// Bucket to store the file in ("" = the default bucket).
	Bucket string
	// Path is the object's unique path within the bucket ("" = none: the
	// object is addressed by its id only). Uploading to a path that is
	// taken fails with already_exists (IsConflict) unless Overwrite is
	// set.
	Path      string
	Overwrite bool
	// Name is the file name shown in downloads.
	Name string
	// ContentType is the declared type ("" = detected by the server from
	// the content and the name).
	ContentType string
	// SHA256 (64 hex digits), when set, makes the server refuse content
	// with another hash (checksum_mismatch).
	SHA256 string
	// Size is the content's length when known (it becomes the request's
	// Content-Length, which lets the server refuse an oversized file
	// before reading it). 0 means unknown: the body is streamed.
	Size int64
}

func (o UploadOptions) query() url.Values {
	q := url.Values{}
	if o.Bucket != "" {
		q.Set("bucket", o.Bucket)
	}
	if o.Path != "" {
		q.Set("path", o.Path)
	}
	if o.Overwrite {
		q.Set("overwrite", "1")
	}
	if o.Name != "" {
		q.Set("name", o.Name)
	}
	if o.ContentType != "" {
		q.Set("content_type", o.ContentType)
	}
	return q
}

// Upload stores the content of r as a new object, streaming it (the
// content is never held in memory). The object is kept until deleted.
// For large files over unreliable networks use UploadResumable.
//
// An upload is not retried: r may have been partly consumed when it
// fails.
func (s *Storage) Upload(ctx context.Context, r io.Reader, o UploadOptions) (*Object, error) {
	hd := http.Header{}
	if o.SHA256 != "" {
		hd.Set("X-Azora-SHA256", o.SHA256)
	}
	// No Content-Type header: a declared type would switch the server's
	// detection off, and the declared one travels in the query.
	resp, err := s.do(ctx, request{method: http.MethodPost, path: "/objects", query: o.query(), header: hd,
		body: io.NopCloser(r), length: bodyLength(r, o.Size)}, http.StatusCreated)
	if err != nil {
		return nil, err
	}
	return objectOf(resp)
}

// bodyLength returns the length of an upload's content: the declared
// size, else what an in-memory reader (bytes.Reader, strings.Reader,
// bytes.Buffer) reports, else 0 (unknown: the body is sent chunked). The
// reader is wrapped before it reaches the HTTP client, which would
// otherwise close a caller's file, so the client cannot find out itself.
func bodyLength(r io.Reader, size int64) int64 {
	if size > 0 {
		return size
	}
	if l, ok := r.(interface{ Len() int }); ok {
		return int64(l.Len())
	}
	return 0
}

// UploadFile stores a file from disk. Name defaults to the file's base
// name and Size to its size.
func (s *Storage) UploadFile(ctx context.Context, path string, o UploadOptions) (*Object, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("azora: storage: %w", err)
	}
	defer f.Close()
	if o.Name == "" {
		o.Name = filepath.Base(path)
	}
	if o.Size == 0 {
		if st, err := f.Stat(); err == nil {
			o.Size = st.Size()
		}
	}
	return s.Upload(ctx, f, o)
}

// UploadGrant says what a signed upload URL allows. Whoever holds the
// URL can read the grant (it is signed, not encrypted): keep secrets out
// of Metadata.
type UploadGrant struct {
	Bucket    string `json:"bucket,omitempty"`
	Path      string `json:"path,omitempty"`
	Overwrite bool   `json:"overwrite,omitempty"`
	// AllowedTypes ("image/png,image/jpeg", "image/*") and MaxSize narrow
	// the bucket's and the database's limits.
	AllowedTypes string `json:"allowed_types,omitempty"`
	MaxSize      int64  `json:"max_size,omitempty"`
	// Keep keeps the object when no row references it.
	Keep     bool              `json:"keep,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
	// Table and Column, when set, receive the new object's id in the
	// upload's transaction: UPDATE Table SET Column = <id> WHERE Key =
	// KeyValue (Key defaults to rowid). Exactly one row must match, or
	// nothing is stored.
	Table    string `json:"table,omitempty"`
	Column   string `json:"column,omitempty"`
	Key      string `json:"key,omitempty"`
	KeyValue string `json:"key_value,omitempty"`
}

// SignedUpload is a signed upload link.
type SignedUpload struct {
	// URL accepts one file per request (POST or PUT; see UploadSigned).
	URL string `json:"url"`
	// SessionURL opens resumable upload sessions under the same grant
	// (see OpenSignedSession).
	SessionURL string    `json:"session_url"`
	ExpiresAt  time.Time `json:"expires_at"`
}

// SignUploadURL returns a signed link that lets a client without
// credentials (a browser, a mobile app) upload files under the grant
// until it expires (ttl 0: the server's default). The link is not
// single-use: keep the lifetime short, or give the grant a Path so that a
// second upload fails.
func (s *Storage) SignUploadURL(ctx context.Context, g UploadGrant, ttl time.Duration) (SignedUpload, error) {
	var out SignedUpload
	body := struct {
		UploadGrant
		TTLSeconds int64 `json:"ttl_seconds"`
	}{g, ttlSeconds(ttl)}
	resp, err := s.do(ctx, request{method: http.MethodPost, path: "/sign-upload", json: body}, http.StatusOK)
	if err != nil {
		return out, err
	}
	err = decodeJSON(resp, &out)
	return out, err
}

// SignedUploadOptions describe a file sent to a signed upload URL. The
// grant behind the URL decides the bucket, the path and the limits.
type SignedUploadOptions struct {
	// Name is the file name; ContentType the declared type ("" =
	// detected).
	Name        string
	ContentType string
	// SHA256 and Size are as in UploadOptions.
	SHA256 string
	Size   int64
	// HTTPClient sends the request (default http.DefaultClient).
	HTTPClient *http.Client
}

func (o SignedUploadOptions) client() *http.Client {
	if o.HTTPClient != nil {
		return o.HTTPClient
	}
	return http.DefaultClient
}

// UploadSigned stores the content of r through a signed upload URL (from
// SignUploadURL), without any credential: what a browser-style client
// does with a link its backend handed out.
func UploadSigned(ctx context.Context, signedURL string, r io.Reader, o SignedUploadOptions) (*Object, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, signedURL, io.NopCloser(r))
	if err != nil {
		return nil, fmt.Errorf("azora: storage: %w", err)
	}
	req.ContentLength = bodyLength(r, o.Size)
	setFileHeaders(req.Header, o)
	if o.ContentType != "" {
		req.Header.Set("Content-Type", o.ContentType)
	}
	resp, err := o.client().Do(req)
	if err != nil {
		return nil, transportErr(ctx, err)
	}
	if resp.StatusCode != http.StatusCreated {
		return nil, responseError(resp)
	}
	return objectOf(resp)
}

// setFileHeaders names and checks a file sent without an API key.
func setFileHeaders(hd http.Header, o SignedUploadOptions) {
	if o.Name != "" {
		hd.Set("X-Azora-Filename", url.PathEscape(o.Name))
	}
	if o.SHA256 != "" {
		hd.Set("X-Azora-SHA256", o.SHA256)
	}
}

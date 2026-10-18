package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// s3Signer signs the way S3 clients do: the object key is encoded once, not twice.
func s3Signer() *v4.Signer {
	return v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
}

func sha256hex(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

func newSigned(t *testing.T, method, rawURL string, body []byte, hdr map[string]string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, rawURL, bytes.NewReader(body))
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	hash := sha256hex(body)
	req.Header.Set("X-Amz-Content-Sha256", hash)
	if err := s3Signer().SignHTTP(context.Background(), aws.Credentials{AccessKeyID: testAK, SecretAccessKey: testSK},
		req, hash, "s3", "us-east-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	serverView(req, len(body))
	return req
}

// serverView makes a client-side request look like what the HTTP server hands to a handler:
// there Content-Length is also present as a header (httptest.NewRequest does not add it).
func serverView(req *http.Request, n int) {
	if n > 0 {
		req.Header.Set("Content-Length", strconv.Itoa(n))
	}
}

func runAuth(req *http.Request) (int, string, []byte) {
	var owner string
	var body []byte
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		owner, _ = r.Context().Value(ownerKey{}).(string)
		var err error
		body, err = io.ReadAll(r.Body)
		if err != nil {
			xmlErrorFor(w, err)
		}
	})
	rec := httptest.NewRecorder()
	AuthMiddleware(testCreds(), nil, slog.New(slog.NewJSONHandler(io.Discard, nil)), next).ServeHTTP(rec, req)
	return rec.Code, owner, body
}

func xmlErrorFor(w http.ResponseWriter, err error) {
	if errors.Is(err, errPayloadHash) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusInternalServerError)
}

// The canonical request must come out the same as in real clients for awkward but legal keys.
// Both bucket names in testCreds is "b".
func TestSigV4AcceptsAwkwardKeysAndQueries(t *testing.T) {
	tests := []struct{ name, url string }{
		{"space and unicode", "http://example.com/b/my%20file%20%C3%A9.txt"},
		{"plus and equals", "http://example.com/b/a%2Bb%3Dc"},
		{"nested", "http://example.com/b/dir/sub%20dir/file.txt"},
		{"double slash in key", "http://example.com/b/dir//file"},
		{"query sorted by name", "http://example.com/b?prefix=a&delimiter=%2F&list-type=2"},
		{"repeated query parameter", "http://example.com/b?tag=b&tag=a&x=1"},
		{"query with encoded space", "http://example.com/b?prefix=my%20dir%2F"},
		{"empty query value", "http://example.com/b?uploads"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if code, _, _ := runAuth(newSigned(t, http.MethodGet, tt.url, nil, nil)); code != http.StatusOK {
				t.Fatalf("a correctly signed request was rejected (%d)", code)
			}
		})
	}
}

// Header values: surrounding and repeated whitespace collapse, repeated headers are joined.
func TestSigV4CanonicalisesHeaderValues(t *testing.T) {
	req := newSigned(t, http.MethodPut, "http://example.com/b/k", []byte("x"),
		map[string]string{"X-Amz-Meta-Note": "  two   words  "})
	if code, _, _ := runAuth(req); code != http.StatusOK {
		t.Fatalf("spacing in a signed header broke the signature (%d)", code)
	}

	req = httptest.NewRequest(http.MethodGet, "http://example.com/b/k", nil)
	req.Header.Add("X-Amz-Meta-Multi", "one")
	req.Header.Add("X-Amz-Meta-Multi", "two")
	req.Header.Set("X-Amz-Content-Sha256", emptySHA256)
	if err := s3Signer().SignHTTP(context.Background(), aws.Credentials{AccessKeyID: testAK, SecretAccessKey: testSK},
		req, emptySHA256, "s3", "us-east-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	if code, _, _ := runAuth(req); code != http.StatusOK {
		t.Fatalf("a header sent twice is signed as one comma-joined value; got %d", code)
	}
}

func presign(t *testing.T, method, rawURL string, expires time.Duration, signAt time.Time) string {
	t.Helper()
	req := httptest.NewRequest(method, rawURL, nil)
	q := req.URL.Query()
	q.Set("X-Amz-Expires", expiresSeconds(expires))
	req.URL.RawQuery = q.Encode()
	signed, _, err := s3Signer().PresignHTTP(context.Background(), aws.Credentials{AccessKeyID: testAK, SecretAccessKey: testSK},
		req, "UNSIGNED-PAYLOAD", "s3", "us-east-1", signAt)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func expiresSeconds(d time.Duration) string { return strconv.Itoa(int(d.Seconds())) }

func presignedRequest(method, signedURL string) *http.Request {
	u, _ := url.Parse(signedURL)
	req := httptest.NewRequest(method, u.RequestURI(), nil)
	req.Host = u.Host
	return req
}

func TestPresignedURLs(t *testing.T) {
	t.Run("valid GET", func(t *testing.T) {
		signed := presign(t, http.MethodGet, "http://example.com/b/shared%20file.txt", 900*time.Second, time.Now())
		code, owner, _ := runAuth(presignedRequest(http.MethodGet, signed))
		if code != http.StatusOK || owner != "b" {
			t.Fatalf("code=%d owner=%q", code, owner)
		}
	})
	t.Run("valid PUT", func(t *testing.T) {
		signed := presign(t, http.MethodPut, "http://example.com/b/upload.bin", 900*time.Second, time.Now())
		if code, _, _ := runAuth(presignedRequest(http.MethodPut, signed)); code != http.StatusOK {
			t.Fatalf("code=%d", code)
		}
	})
	t.Run("expired", func(t *testing.T) {
		signed := presign(t, http.MethodGet, "http://example.com/b/k", 60*time.Second, time.Now().Add(-2*time.Hour))
		if code, _, _ := runAuth(presignedRequest(http.MethodGet, signed)); code != http.StatusForbidden {
			t.Fatalf("an expired link was accepted (%d)", code)
		}
	})
	t.Run("signed in the future", func(t *testing.T) {
		signed := presign(t, http.MethodGet, "http://example.com/b/k", 900*time.Second, time.Now().Add(2*time.Hour))
		if code, _, _ := runAuth(presignedRequest(http.MethodGet, signed)); code != http.StatusForbidden {
			t.Fatalf("a link from the future was accepted (%d)", code)
		}
	})
	t.Run("tampered path", func(t *testing.T) {
		signed := presign(t, http.MethodGet, "http://example.com/b/k", 900*time.Second, time.Now())
		if code, _, _ := runAuth(presignedRequest(http.MethodGet, strings.Replace(signed, "/b/k", "/b/other", 1))); code != http.StatusForbidden {
			t.Fatalf("a link for another key was accepted (%d)", code)
		}
	})
	t.Run("tampered expiry", func(t *testing.T) {
		signed := presign(t, http.MethodGet, "http://example.com/b/k", 60*time.Second, time.Now())
		if code, _, _ := runAuth(presignedRequest(http.MethodGet, strings.Replace(signed, "X-Amz-Expires=60", "X-Amz-Expires=604800", 1))); code != http.StatusForbidden {
			t.Fatalf("extending a link's life was accepted (%d)", code)
		}
	})
	t.Run("method is bound to the signature", func(t *testing.T) {
		signed := presign(t, http.MethodGet, "http://example.com/b/k", 900*time.Second, time.Now())
		if code, _, _ := runAuth(presignedRequest(http.MethodPut, signed)); code != http.StatusForbidden {
			t.Fatalf("a GET link was usable for PUT (%d)", code)
		}
	})
	t.Run("link for another bucket's key", func(t *testing.T) {
		signed := presign(t, http.MethodGet, "http://example.com/other-bucket/k", 900*time.Second, time.Now())
		if code, _, _ := runAuth(presignedRequest(http.MethodGet, signed)); code != http.StatusForbidden {
			t.Fatalf("a link crossed the bucket boundary (%d)", code)
		}
	})
}

// The signature covers the body only through x-amz-content-sha256. If that hash is not checked
// against the body, a captured request can be replayed with different content.
func TestSignedBodyMustMatchItsHash(t *testing.T) {
	good := []byte("the signed content")
	req := newSigned(t, http.MethodPut, "http://example.com/b/k", good, nil)

	t.Run("matching body is delivered", func(t *testing.T) {
		code, _, body := runAuth(req)
		if code != http.StatusOK || !bytes.Equal(body, good) {
			t.Fatalf("code=%d body=%q", code, body)
		}
	})
	t.Run("replayed with other content", func(t *testing.T) {
		replay := newSigned(t, http.MethodPut, "http://example.com/b/k", good, nil)
		replay.Body = io.NopCloser(strings.NewReader("evil replacement!!"))
		code, _, body := runAuth(replay)
		if code == http.StatusOK && string(body) == "evil replacement!!" {
			t.Fatal("tampered body accepted with the original signature")
		}
		if code != http.StatusBadRequest {
			t.Fatalf("code = %d, want 400 (hash mismatch surfaced while reading)", code)
		}
	})
	t.Run("hash of empty body but a body present", func(t *testing.T) {
		replay := newSigned(t, http.MethodGet, "http://example.com/b/k", nil, nil)
		replay.Body = io.NopCloser(strings.NewReader("smuggled"))
		replay.ContentLength = 8
		if code, _, body := runAuth(replay); code == http.StatusOK && len(body) > 0 {
			t.Fatal("a body was accepted under the empty-payload hash")
		}
	})
	t.Run("non-empty hash but no body", func(t *testing.T) {
		replay := newSigned(t, http.MethodPut, "http://example.com/b/k", good, nil)
		replay.Body = http.NoBody
		replay.ContentLength = 0
		if code, _, _ := runAuth(replay); code != http.StatusBadRequest {
			t.Fatalf("code = %d, want 400", code)
		}
	})
	t.Run("UNSIGNED-PAYLOAD is not checked", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPut, "http://example.com/b/k", strings.NewReader("anything"))
		r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
		if err := s3Signer().SignHTTP(context.Background(), aws.Credentials{AccessKeyID: testAK, SecretAccessKey: testSK},
			r, "UNSIGNED-PAYLOAD", "s3", "us-east-1", time.Now()); err != nil {
			t.Fatal(err)
		}
		serverView(r, len("anything"))
		if code, _, body := runAuth(r); code != http.StatusOK || string(body) != "anything" {
			t.Fatalf("code=%d body=%q", code, body)
		}
	})
}

type countingHandler struct{ n atomic.Int64 }

func (c *countingHandler) Enabled(context.Context, slog.Level) bool  { return true }
func (c *countingHandler) Handle(context.Context, slog.Record) error { c.n.Add(1); return nil }
func (c *countingHandler) WithAttrs([]slog.Attr) slog.Handler        { return c }
func (c *countingHandler) WithGroup(string) slog.Handler             { return c }

// Rejected requests are attacker-controlled: a flood must not turn into a log flood.
func TestAuthRejectionLogsAreRateLimited(t *testing.T) {
	h := &countingHandler{}
	log := slog.New(h)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	mw := AuthMiddleware(testCreds(), nil, log, next)

	start := time.Now()
	for i := 0; i < 2000; i++ {
		req := httptest.NewRequest(http.MethodGet, "/b/k", nil)
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=NOPE/20260101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00")
		mw.ServeHTTP(httptest.NewRecorder(), req)
	}
	seconds := int64(time.Since(start).Seconds()) + 1
	if got, max := h.n.Load(), seconds*authLogBurst+int64(authLogBurst); got > max {
		t.Fatalf("%d log entries for 2000 rejected requests in %ds, want at most %d", got, seconds, max)
	}
}

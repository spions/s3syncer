package proxy

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/spions/s3syncer/internal/config"
)

const (
	emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	testAK      = "AKTEST"
	testSK      = "supersecretkey"
)

func testCreds() Credentials {
	return BuildCredentials([]config.VirtualBucket{{
		Name: "b", AccessKeyID: testAK, SecretAccessKey: testSK,
	}})
}

func signV4(t *testing.T, at time.Time, secret string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "http://example.com/b/key.txt", nil)
	req.Header.Set("X-Amz-Content-Sha256", emptySHA256)
	err := v4.NewSigner().SignHTTP(context.Background(),
		aws.Credentials{AccessKeyID: testAK, SecretAccessKey: secret},
		req, emptySHA256, "s3", "us-east-1", at)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func serve(t *testing.T, req *http.Request, logBuf *bytes.Buffer) int {
	t.Helper()
	log := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	rec := httptest.NewRecorder()
	AuthMiddleware(testCreds(), nil, log, next).ServeHTTP(rec, req)
	return rec.Code
}

func TestAuthV4(t *testing.T) {
	tests := []struct {
		name   string
		at     time.Time
		secret string
		want   int
	}{
		{"valid", time.Now(), testSK, http.StatusOK},
		{"wrong secret", time.Now(), "other", http.StatusForbidden},
		{"too old", time.Now().Add(-time.Hour), testSK, http.StatusForbidden},
		{"too far in future", time.Now().Add(time.Hour), testSK, http.StatusForbidden},
		{"within skew", time.Now().Add(-10 * time.Minute), testSK, http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			if got := serve(t, signV4(t, tt.at, tt.secret), &logs); got != tt.want {
				t.Fatalf("status = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestAuthV4MissingDate(t *testing.T) {
	req := signV4(t, time.Now(), testSK)
	req.Header.Del("X-Amz-Date")
	var logs bytes.Buffer
	if got := serve(t, req, &logs); got != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", got)
	}
}

func TestAuthV2(t *testing.T) {
	newReq := func(date time.Time, secret string) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "http://example.com/b/key.txt", nil)
		req.Header.Set("Date", date.UTC().Format(http.TimeFormat))
		sig := computeSignatureV2(req, secret)
		req.Header.Set("Authorization", "AWS "+testAK+":"+sig)
		return req
	}
	var logs bytes.Buffer
	if got := serve(t, newReq(time.Now(), testSK), &logs); got != http.StatusOK {
		t.Fatalf("valid: status = %d, want 200", got)
	}
	if got := serve(t, newReq(time.Now(), "other"), &logs); got != http.StatusForbidden {
		t.Fatalf("wrong secret: status = %d, want 403", got)
	}
	if got := serve(t, newReq(time.Now().Add(-time.Hour), testSK), &logs); got != http.StatusForbidden {
		t.Fatalf("stale date: status = %d, want 403", got)
	}
}

// Rejected requests must never write signatures or auth headers to the log.
func TestAuthDoesNotLogSignatures(t *testing.T) {
	var logs bytes.Buffer

	bad := signV4(t, time.Now(), "other")
	clientSig := parseAuthHeader(bad.Header.Get("Authorization"))["Signature"]
	if got := serve(t, bad, &logs); got != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", got)
	}

	// The signature the server would accept for this request
	good := signV4(t, time.Now(), testSK)
	expectedSig := parseAuthHeader(good.Header.Get("Authorization"))["Signature"]

	v2 := httptest.NewRequest(http.MethodGet, "http://example.com/b/key.txt", nil)
	v2.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	v2.Header.Set("Authorization", "AWS "+testAK+":badsignature")
	serve(t, v2, &logs)

	out := logs.String()
	if !strings.Contains(out, "auth_rejected") {
		t.Fatal("expected auth_rejected entry in log")
	}
	for _, secret := range []string{clientSig, expectedSig, "badsignature", testSK} {
		if secret != "" && strings.Contains(out, secret) {
			t.Fatalf("log leaks sensitive value %q:\n%s", secret, out)
		}
	}
}

// An access key must only work for its own virtual bucket.
func TestAuthKeyIsBoundToItsBucket(t *testing.T) {
	creds := BuildCredentials([]config.VirtualBucket{
		{Name: "alpha", AccessKeyID: "AKALPHA", SecretAccessKey: "secret-alpha"},
		{Name: "beta", AccessKeyID: "AKBETA", SecretAccessKey: "secret-beta"},
	})
	var seenOwner string
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenOwner, _ = r.Context().Value(ownerKey{}).(string)
		w.WriteHeader(http.StatusOK)
	})
	h := AuthMiddleware(creds, nil, slog.New(slog.NewJSONHandler(io.Discard, nil)), next)

	signed := func(ak, sk, path string) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "http://example.com"+path, nil)
		req.Header.Set("X-Amz-Content-Sha256", emptySHA256)
		if err := v4.NewSigner().SignHTTP(context.Background(), aws.Credentials{AccessKeyID: ak, SecretAccessKey: sk},
			req, emptySHA256, "s3", "us-east-1", time.Now()); err != nil {
			t.Fatal(err)
		}
		return req
	}

	tests := []struct {
		name, path string
		want       int
		owner      string
	}{
		{"own bucket object", "/alpha/key.txt", http.StatusOK, "alpha"},
		{"own bucket listing", "/alpha", http.StatusOK, "alpha"},
		{"list buckets", "/", http.StatusOK, "alpha"},
		{"other bucket object", "/beta/key.txt", http.StatusForbidden, ""},
		{"other bucket listing", "/beta?list-type=2", http.StatusForbidden, ""},
		{"legacy resync endpoint", "/resync/beta", http.StatusForbidden, ""},
		{"bucket name as prefix of another", "/alphabet/key", http.StatusForbidden, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seenOwner = ""
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, signed("AKALPHA", "secret-alpha", tt.path))
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d", rec.Code, tt.want)
			}
			if seenOwner != tt.owner {
				t.Fatalf("owner in context = %q, want %q", seenOwner, tt.owner)
			}
		})
	}

	t.Run("V2 key is bound too", func(t *testing.T) {
		mk := func(path string) *http.Request {
			req := httptest.NewRequest(http.MethodGet, "http://example.com"+path, nil)
			req.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
			req.Header.Set("Authorization", "AWS AKALPHA:"+computeSignatureV2(req, "secret-alpha"))
			return req
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, mk("/alpha/k"))
		if rec.Code != http.StatusOK {
			t.Fatalf("own bucket: status = %d", rec.Code)
		}
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, mk("/beta/k"))
		if rec.Code != http.StatusForbidden {
			t.Fatalf("other bucket: status = %d, want 403", rec.Code)
		}
	})
}

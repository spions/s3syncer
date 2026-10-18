package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProtect(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })

	tests := []struct {
		name   string
		token  string
		remote string
		auth   string
		want   int
	}{
		{"token ok", "s3cret", "10.0.0.5:1234", "Bearer s3cret", http.StatusOK},
		{"token wrong", "s3cret", "127.0.0.1:1234", "Bearer nope", http.StatusUnauthorized},
		{"token missing", "s3cret", "127.0.0.1:1234", "", http.StatusUnauthorized},
		{"no token, ipv4 loopback", "", "127.0.0.1:1234", "", http.StatusOK},
		{"no token, ipv6 loopback", "", "[::1]:1234", "", http.StatusOK},
		{"no token, remote", "", "10.0.0.5:1234", "", http.StatusForbidden},
		{"no token, remote with bearer", "", "10.0.0.5:1234", "Bearer x", http.StatusForbidden},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/tasks/", nil)
			req.RemoteAddr = tt.remote
			if tt.auth != "" {
				req.Header.Set("Authorization", tt.auth)
			}
			rec := httptest.NewRecorder()
			Protect(tt.token, next).ServeHTTP(rec, req)
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d", rec.Code, tt.want)
			}
		})
	}
}

// A reverse proxy in the same pod connects from 127.0.0.1; without a token its clients must
// not get the API just because the TCP peer is loopback.
func TestProtectRefusesProxiedRequestsWithoutToken(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	for _, header := range []string{"X-Forwarded-For", "X-Real-Ip", "Forwarded"} {
		req := httptest.NewRequest(http.MethodGet, "/api/tasks/", nil)
		req.RemoteAddr = "127.0.0.1:5555"
		req.Header.Set(header, "203.0.113.7")
		rec := httptest.NewRecorder()
		Protect("", next).ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Errorf("%s: status = %d, want 403", header, rec.Code)
		}
	}
	// with a token the proxy headers are irrelevant: authentication decides
	req := httptest.NewRequest(http.MethodGet, "/api/tasks/", nil)
	req.RemoteAddr = "127.0.0.1:5555"
	req.Header.Set("X-Forwarded-For", "203.0.113.7")
	req.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	Protect("tok", next).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("token + proxy header: status = %d, want 200", rec.Code)
	}
}

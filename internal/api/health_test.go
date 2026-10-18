package api

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthzIgnoresDependencies(t *testing.T) {
	rec := httptest.NewRecorder()
	Healthz().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("liveness = %d, want 200", rec.Code)
	}
}

func TestReadyzReflectsQueueHealth(t *testing.T) {
	down := errors.New("dial tcp 10.0.0.9:6379: refused")
	var reported error
	h := Readyz(func() error { return down }, func(err error) { reported = err })

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness with a dead queue = %d, want 503", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "10.0.0.9") {
		t.Fatal("readiness response leaks the internal error")
	}
	if !errors.Is(reported, down) {
		t.Fatal("the failure must be reported to the log callback")
	}

	rec = httptest.NewRecorder()
	Readyz(func() error { return nil }, nil).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("readiness with a healthy queue = %d, want 200", rec.Code)
	}
}

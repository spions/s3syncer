package proxy

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRouteKeepsUnusualObjectKeysIntact(t *testing.T) {
	var gotPath string
	s3 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { gotPath = r.URL.Path })
	admin := http.NewServeMux()
	admin.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { gotPath = "admin:" + r.URL.Path })
	h := Route(admin, s3)

	for _, key := range []string{"/vb/dir//file", "/vb/a/./b", "/vb/a/../b", "/vb//lead", "/vb/trail//"} {
		gotPath = ""
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, key, nil))
		if rec.Code >= 300 && rec.Code < 400 {
			t.Errorf("%s was redirected (%d): the object would be unreachable", key, rec.Code)
		}
		if gotPath != key {
			t.Errorf("handler saw path %q, want %q unchanged", gotPath, key)
		}
	}
}

func TestRouteSendsOperationalEndpointsToAdmin(t *testing.T) {
	s3Hit := false
	s3 := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { s3Hit = true })
	adminHit := ""
	admin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { adminHit = r.URL.Path })
	h := Route(admin, s3)

	for _, p := range []string{"/metrics", "/healthz", "/readyz", "/api/tasks/", "/api/tasks/abc"} {
		adminHit, s3Hit = "", false
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, p, nil))
		if adminHit != p || s3Hit {
			t.Errorf("%s: admin=%q s3=%v", p, adminHit, s3Hit)
		}
	}
	// a bucket that merely starts like an admin path is still S3 traffic
	for _, p := range []string{"/apiary/key", "/metrics-bucket/key", "/healthz2"} {
		adminHit, s3Hit = "", false
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, p, nil))
		if adminHit != "" || !s3Hit {
			t.Errorf("%s was taken for an admin path", p)
		}
	}
}

package proxy

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/spions/s3syncer/internal/metrics"
)

func TestObserveCountsRequestsByMethodAndStatus(t *testing.T) {
	h := Observe(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/missing" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte("ok")) // implicit 200
	}))
	count := func(method, code string) float64 {
		return testutil.ToFloat64(metrics.HTTPRequests.WithLabelValues(method, code))
	}
	ok0, nf0 := count("GET", "200"), count("GET", "404")

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/a", nil))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/missing", nil))

	if got := count("GET", "200") - ok0; got != 1 {
		t.Errorf("200 responses counted %v times, want 1", got)
	}
	if got := count("GET", "404") - nf0; got != 1 {
		t.Errorf("404 responses counted %v times, want 1", got)
	}
	if g := testutil.ToFloat64(metrics.HTTPInflight); g != 0 {
		t.Errorf("in-flight gauge = %v after all requests finished", g)
	}
}

// Unusual verbs must not create a new time series each: label cardinality stays fixed.
func TestObserveBoundsMethodLabel(t *testing.T) {
	if got := methodLabel("PROPFIND"); got != "OTHER" {
		t.Errorf("methodLabel(PROPFIND) = %q, want OTHER", got)
	}
	if got := methodLabel(http.MethodPut); got != http.MethodPut {
		t.Errorf("methodLabel(PUT) = %q", got)
	}
}

// The recorder must not hide the real ResponseWriter from http.ResponseController
// (the upload idle timeout relies on reaching the connection through it).
func TestObserveKeepsResponseControllerWorking(t *testing.T) {
	var reached bool
	srv := httptest.NewServer(Observe(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = http.NewResponseController(w).SetReadDeadline(zeroTime()) == nil
	})))
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if !reached {
		t.Fatal("ResponseController cannot reach the connection through the metrics wrapper")
	}
}

func zeroTime() (t time.Time) { return }

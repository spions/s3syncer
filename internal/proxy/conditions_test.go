package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCheckConditions(t *testing.T) {
	mod := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	later := mod.Add(time.Hour).Format(http.TimeFormat)
	earlier := mod.Add(-time.Hour).Format(http.TimeFormat)
	same := mod.Format(http.TimeFormat)

	tests := []struct {
		name    string
		headers map[string]string
		want    int
	}{
		{"no conditions", nil, 0},
		{"If-None-Match equal", map[string]string{"If-None-Match": `"abc"`}, http.StatusNotModified},
		{"If-None-Match different", map[string]string{"If-None-Match": `"zzz"`}, 0},
		{"If-None-Match list contains it", map[string]string{"If-None-Match": `"x", "abc", "y"`}, http.StatusNotModified},
		{"If-None-Match star", map[string]string{"If-None-Match": `*`}, http.StatusNotModified},
		{"If-None-Match weak tag matches weakly", map[string]string{"If-None-Match": `W/"abc"`}, http.StatusNotModified},
		{"If-Match equal", map[string]string{"If-Match": `"abc"`}, 0},
		{"If-Match different", map[string]string{"If-Match": `"zzz"`}, http.StatusPreconditionFailed},
		{"If-Match weak never matches strongly", map[string]string{"If-Match": `W/"abc"`}, http.StatusPreconditionFailed},
		{"If-Match star", map[string]string{"If-Match": `*`}, 0},
		{"If-Modified-Since unchanged", map[string]string{"If-Modified-Since": later}, http.StatusNotModified},
		{"If-Modified-Since same instant", map[string]string{"If-Modified-Since": same}, http.StatusNotModified},
		{"If-Modified-Since modified", map[string]string{"If-Modified-Since": earlier}, 0},
		{"If-Unmodified-Since modified after", map[string]string{"If-Unmodified-Since": earlier}, http.StatusPreconditionFailed},
		{"If-Unmodified-Since unchanged", map[string]string{"If-Unmodified-Since": later}, 0},
		// RFC 7232: If-None-Match wins over If-Modified-Since; If-Match over If-Unmodified-Since
		{"ETag mismatch overrides If-Modified-Since", map[string]string{"If-None-Match": `"zzz"`, "If-Modified-Since": later}, 0},
		{"If-Match overrides If-Unmodified-Since", map[string]string{"If-Match": `"abc"`, "If-Unmodified-Since": earlier}, 0},
		{"garbage date is ignored", map[string]string{"If-Modified-Since": "yesterday-ish"}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/vb/k", nil)
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}
			if got := checkConditions(req, `"abc"`, mod); got != tt.want {
				t.Fatalf("got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestSingleByteRange(t *testing.T) {
	for in, want := range map[string]string{
		"bytes=0-99":    "bytes=0-99",
		" bytes=-500 ":  "bytes=-500",
		"bytes=0-5,7-9": "", // multiple ranges: serve the whole object
		"items=0-5":     "",
		"":              "",
		"bytes=100-":    "bytes=100-",
	} {
		if got := singleByteRange(in); got != want {
			t.Errorf("singleByteRange(%q) = %q, want %q", in, got, want)
		}
	}
}

// ---- end to end through the handler ----

func getReq(h http.Handler, path string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestGetServesRanges(t *testing.T) {
	h, primary, _ := newTestHandler(t, nil)
	primary.objs["f"] = []byte("0123456789")

	rec := getReq(h, "/vb/f", map[string]string{"Range": "bytes=2-5"})
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	if string(body) != "2345" {
		t.Fatalf("body = %q, want 2345", body)
	}
	if got := rec.Header().Get("Content-Range"); got != "bytes 2-5/10" {
		t.Errorf("Content-Range = %q", got)
	}
	if got := rec.Header().Get("Content-Length"); got != "4" {
		t.Errorf("Content-Length = %q, want the length of the part", got)
	}
	if rec.Header().Get("Accept-Ranges") != "bytes" {
		t.Error("Accept-Ranges not advertised")
	}

	if rec := getReq(h, "/vb/f", map[string]string{"Range": "bytes=50-60"}); rec.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Errorf("out-of-range request: status = %d, want 416", rec.Code)
	}
	if rec := getReq(h, "/vb/f", nil); rec.Code != http.StatusOK || rec.Body.String() != "0123456789" {
		t.Errorf("plain GET: status=%d body=%q", rec.Code, rec.Body)
	}
	// several ranges are not supported: the whole object is returned, which HTTP permits
	if rec := getReq(h, "/vb/f", map[string]string{"Range": "bytes=0-1,4-5"}); rec.Code != http.StatusOK {
		t.Errorf("multi-range: status = %d, want 200 with the full body", rec.Code)
	}
}

func TestGetHonoursConditionalHeaders(t *testing.T) {
	h, primary, _ := newTestHandler(t, nil)
	primary.objs["f"] = []byte("payload")

	rec := getReq(h, "/vb/f", map[string]string{"If-None-Match": `"etag-1"`})
	if rec.Code != http.StatusNotModified {
		t.Fatalf("matching If-None-Match: status = %d, want 304", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Error("a 304 must not carry a body")
	}
	if rec.Header().Get("ETag") != `"etag-1"` {
		t.Error("a 304 must repeat the ETag")
	}
	if rec := getReq(h, "/vb/f", map[string]string{"If-Match": `"other"`}); rec.Code != http.StatusPreconditionFailed {
		t.Errorf("failing If-Match: status = %d, want 412", rec.Code)
	}
	if rec := getReq(h, "/vb/f", map[string]string{"If-None-Match": `"changed"`}); rec.Code != http.StatusOK || rec.Body.String() != "payload" {
		t.Errorf("non-matching If-None-Match must serve the object: status=%d", rec.Code)
	}

	req := httptest.NewRequest(http.MethodHead, "/vb/f", nil)
	req.Header.Set("If-None-Match", `"etag-1"`)
	hrec := httptest.NewRecorder()
	h.ServeHTTP(hrec, req)
	if hrec.Code != http.StatusNotModified {
		t.Errorf("HEAD with matching If-None-Match: status = %d, want 304", hrec.Code)
	}
}

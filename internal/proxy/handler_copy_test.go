package proxy

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseCopySource(t *testing.T) {
	tests := []struct {
		in          string
		bucket      string
		key         string
		versioned   bool
		wantOK      bool
		description string
	}{
		{"/vb/plain.txt", "vb", "plain.txt", false, true, "leading slash"},
		{"vb/plain.txt", "vb", "plain.txt", false, true, "no leading slash"},
		{"vb/my%20file%C3%A9.txt", "vb", "my fileé.txt", false, true, "percent-encoded space and unicode"},
		{"vb/dir/sub/file", "vb", "dir/sub/file", false, true, "nested key"},
		{"vb/a%2Fb", "vb", "a/b", false, true, "encoded slash"},
		{"vb/file?versionId=abc", "vb", "file", true, true, "version id"},
		{"vb", "", "", false, false, "no key"},
		{"/", "", "", false, false, "empty"},
		{"vb/%zz", "", "", false, false, "broken encoding"},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			b, k, v, ok := parseCopySource(tt.in)
			if ok != tt.wantOK || b != tt.bucket || k != tt.key || v != tt.versioned {
				t.Fatalf("parseCopySource(%q) = %q %q %v %v; want %q %q %v %v", tt.in, b, k, v, ok, tt.bucket, tt.key, tt.versioned, tt.wantOK)
			}
		})
	}
}

func copyRequest(h http.Handler, dst, source string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPut, dst, strings.NewReader(""))
	req.Header.Set("x-amz-copy-source", source)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestCopyRejectsOtherBucketAndVersions(t *testing.T) {
	h, primary, _ := newTestHandler(t, nil)
	primary.objs["src"] = []byte("x")

	if rec := copyRequest(h, "/vb/dst", "/other-bucket/src"); rec.Code != http.StatusNotImplemented {
		t.Errorf("cross-bucket copy: status = %d, want 501 (it must not silently copy from this bucket)", rec.Code)
	}
	if _, copied := primary.objs["dst"]; copied {
		t.Error("a cross-bucket copy created an object")
	}
	if rec := copyRequest(h, "/vb/dst", "/vb/src?versionId=1"); rec.Code != http.StatusNotImplemented {
		t.Errorf("versioned copy: status = %d, want 501 (the version must not be ignored)", rec.Code)
	}
	if rec := copyRequest(h, "/vb/dst", "garbage"); rec.Code != http.StatusBadRequest {
		t.Errorf("malformed source: status = %d, want 400", rec.Code)
	}
}

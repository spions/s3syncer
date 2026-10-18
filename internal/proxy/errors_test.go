package proxy

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestS3ErrorCode(t *testing.T) {
	tests := []struct {
		status int
		msg    string
		want   string
	}{
		{http.StatusNotFound, `bucket "x" not found`, "NoSuchBucket"},
		{http.StatusNotFound, "not found", "NoSuchKey"},
		{http.StatusNotFound, "symlink target not found", "NoSuchKey"},
		{http.StatusForbidden, "x", "AccessDenied"},
		{http.StatusRequestEntityTooLarge, "x", "EntityTooLarge"},
		{http.StatusInternalServerError, "boom", "InternalError"},
	}
	for _, tt := range tests {
		if got := s3ErrorCode(tt.status, tt.msg); got != tt.want {
			t.Errorf("s3ErrorCode(%d, %q) = %q, want %q", tt.status, tt.msg, got, tt.want)
		}
	}
}

// Client-supplied names must not be able to inject XML into error responses.
func TestXMLErrorEscapesMessage(t *testing.T) {
	rec := httptest.NewRecorder()
	xmlError(rec, http.StatusNotFound, "NoSuchBucket", `bucket "</Message><Evil/>" not found`)

	var parsed struct {
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	if err := xml.Unmarshal(rec.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("response is not well-formed XML: %v\n%s", err, rec.Body.String())
	}
	if parsed.Code != "NoSuchBucket" || parsed.Message != `bucket "</Message><Evil/>" not found` {
		t.Fatalf("unexpected parse result: %+v", parsed)
	}
}

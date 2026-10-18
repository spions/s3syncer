package proxy

import (
	"net/http"
	"strings"
	"time"
)

// checkConditions evaluates the HTTP preconditions of a GET/HEAD (RFC 7232, section 6)
// against the object's current ETag and modification time. It returns 0 to proceed,
// 304 (Not Modified) or 412 (Precondition Failed).
func checkConditions(r *http.Request, etag string, lastModified time.Time) int {
	lastModified = lastModified.Truncate(time.Second) // HTTP dates have second resolution

	if im := r.Header.Get("If-Match"); im != "" {
		if !etagListMatches(im, etag, true) {
			return http.StatusPreconditionFailed
		}
	} else if ius := parseHTTPDate(r.Header.Get("If-Unmodified-Since")); !ius.IsZero() && !lastModified.IsZero() {
		if lastModified.After(ius) {
			return http.StatusPreconditionFailed
		}
	}

	if inm := r.Header.Get("If-None-Match"); inm != "" {
		if etagListMatches(inm, etag, false) {
			return http.StatusNotModified
		}
	} else if ims := parseHTTPDate(r.Header.Get("If-Modified-Since")); !ims.IsZero() && !lastModified.IsZero() {
		if !lastModified.After(ims) {
			return http.StatusNotModified
		}
	}
	return 0
}

func parseHTTPDate(v string) time.Time {
	if v == "" {
		return time.Time{}
	}
	t, err := http.ParseTime(v)
	if err != nil {
		return time.Time{} // an invalid date is ignored, as RFC 7232 prescribes
	}
	return t
}

// etagListMatches reports whether any entity tag in the header value matches etag. Strong
// comparison (If-Match) rejects weak tags; weak comparison (If-None-Match) ignores W/.
func etagListMatches(header, etag string, strong bool) bool {
	etag = strings.Trim(etag, `"`)
	for _, candidate := range strings.Split(header, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" {
			return true
		}
		if weak := strings.HasPrefix(candidate, "W/"); weak {
			if strong {
				continue
			}
			candidate = strings.TrimPrefix(candidate, "W/")
		}
		if strings.Trim(candidate, `"`) == etag {
			return true
		}
	}
	return false
}

// singleByteRange returns the Range header if it is a single "bytes=" range the backend can
// serve, otherwise "" (the whole object is returned, which HTTP allows).
func singleByteRange(h string) string {
	h = strings.TrimSpace(h)
	if !strings.HasPrefix(h, "bytes=") || strings.Contains(h, ",") {
		return ""
	}
	return h
}

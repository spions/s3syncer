package proxy

import (
	"net/http"
	"strings"
)

// adminPaths are served by the admin mux; everything else is S3 traffic.
func isAdminPath(p string) bool {
	return p == "/metrics" || p == "/healthz" || p == "/readyz" || p == "/api" || strings.HasPrefix(p, "/api/")
}

// Route sends the fixed operational endpoints to admin and every other request to s3.
//
// S3 traffic must not go through http.ServeMux: the mux "cleans" request paths and answers
// 307 for ones containing "//", "/./" or "/../". Those are legal object keys (FUSE trees
// routinely produce "dir//file"), and a redirect makes such objects unreachable or, worse,
// turns them into a different key.
func Route(admin, s3 http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isAdminPath(r.URL.Path) {
			admin.ServeHTTP(w, r)
			return
		}
		s3.ServeHTTP(w, r)
	})
}

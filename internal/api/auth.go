package api

import (
	"crypto/subtle"
	"net"
	"net/http"
	"strings"
)

// Protect guards the management API.
//
// If token is non-empty, every request must carry "Authorization: Bearer <token>".
// If token is empty, only requests coming from a loopback address are allowed,
// so the CLI (which talks to localhost) keeps working without configuration
// while the API is not reachable from the network.
func Protect(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token == "" {
			// "Loopback only" is meant to keep the API off the network. A reverse proxy or
			// sidecar in the same network namespace also connects from loopback, so requests
			// that were forwarded for someone else are refused as well.
			for _, h := range []string{"X-Forwarded-For", "X-Real-Ip", "Forwarded"} {
				if r.Header.Get(h) != "" {
					http.Error(w, "forbidden: the management API without api_token does not accept proxied requests", http.StatusForbidden)
					return
				}
			}
			host, _, err := net.SplitHostPort(r.RemoteAddr)
			if ip := net.ParseIP(host); err != nil || ip == nil || !ip.IsLoopback() {
				http.Error(w, "forbidden: management API is limited to loopback; set api_token to enable remote access", http.StatusForbidden)
				return
			}
			next.ServeHTTP(w, r)
			return
		}

		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="s3syncer"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

package api

import (
	"net/http"
)

// Healthz is the liveness probe: the process is up and serving HTTP. It deliberately
// checks nothing else, so a Redis or backend outage does not get the pod restarted.
func Healthz() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("ok\n"))
	})
}

// Readyz is the readiness probe: it answers 200 only if check succeeds (queue reachable),
// so an instance that cannot record replication tasks stops receiving traffic.
// The failure text stays in the log; clients and probes only see "not ready".
func Readyz(check func() error, onFailure func(error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		if err := check(); err != nil {
			if onFailure != nil {
				onFailure(err)
			}
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready\n"))
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})
}

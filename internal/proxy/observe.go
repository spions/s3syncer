package proxy

import (
	"net/http"
	"strconv"
	"time"

	"github.com/spions/s3syncer/internal/metrics"
)

// statusRecorder remembers the response status. It forwards Unwrap so that
// http.ResponseController (read deadlines, flushing) still reaches the real connection.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// methodLabel keeps the metric label set fixed whatever clients send.
func methodLabel(m string) string {
	switch m {
	case http.MethodGet, http.MethodPut, http.MethodHead, http.MethodDelete, http.MethodPost:
		return m
	}
	return "OTHER"
}

// Observe records request count, latency and concurrency of the client-facing handler.
func Observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		method := methodLabel(r.Method)
		metrics.HTTPInflight.Inc()
		defer metrics.HTTPInflight.Dec()

		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)

		status := rec.status
		if status == 0 {
			status = http.StatusOK
		}
		metrics.HTTPRequests.WithLabelValues(method, strconv.Itoa(status)).Inc()
		metrics.HTTPRequestDuration.WithLabelValues(method).Observe(time.Since(start).Seconds())
	})
}

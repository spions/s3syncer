package proxy

import (
	"net/http/httptest"
	"testing"
)

func TestGeeseFS_UserAgent(t *testing.T) {
	tests := []struct {
		userAgent string
		want      bool
	}{
		{"GeeseFS/0.43.5 (go1.26.1; linux; amd64)", true},
		{"GeeseFS/1.0.0", true},
		{"aws-cli/2.0.0", false},
		{"Mozilla/5.0", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.userAgent, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/", nil)
			req.Header.Set("User-Agent", tt.userAgent)

			got := isGeeseFS(req)
			if got != tt.want {
				t.Errorf("isGeeseFS() = %v, want %v", got, tt.want)
			}
		})
	}
}

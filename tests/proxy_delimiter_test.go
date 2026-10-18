//go:build integration

package tests

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/spions/s3syncer/internal/storage"
)

// TestProxyDelimiterBehavior tests that proxy correctly handles delimiter parameter
// This test requires the proxy to be running at TEST_PROXY_ENDPOINT
func TestProxyDelimiterBehavior(t *testing.T) {
	cfg := GetTestConfig()

	// Skip if proxy is not running
	proxyBackend := storage.NewS3Backend(
		"proxy",
		cfg.S3Region,
		cfg.ProxyBucket,
		cfg.ProxyEndpoint,
		cfg.S3AccessKey,
		cfg.S3SecretKey,
		true,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t.Run("ProxyAutoDelimiter", func(t *testing.T) {
		// Request without delimiter - proxy should auto-enable it for performance
		start := time.Now()
		objects, err := proxyBackend.ListObjects(ctx, "")
		elapsed := time.Since(start)

		if err != nil {
			t.Skipf("Proxy not available: %v", err)
			return
		}

		t.Logf("Proxy listing without explicit delimiter: %d items in %v", len(objects), elapsed)

		// Should be fast (under 1 second) because proxy auto-enables delimiter
		if elapsed > time.Second {
			t.Errorf("Listing too slow: %v (expected < 1s with auto-delimiter)", elapsed)
		}

		// Should return reasonable number of items (not 645K)
		if len(objects) > 1000 {
			t.Errorf("Too many objects returned: %d (expected < 1000 with delimiter)", len(objects))
		}

		for _, obj := range objects {
			t.Logf("  %s (size: %d)", obj.Key, obj.Size)
		}
	})

	t.Run("ProxyExplicitDelimiter", func(t *testing.T) {
		// Request with explicit delimiter
		objects, err := proxyBackend.ListObjectsHierarchical(ctx, "", "/")
		if err != nil {
			t.Skipf("Proxy not available: %v", err)
			return
		}

		t.Logf("Proxy listing with explicit delimiter: %d items", len(objects))

		for _, obj := range objects {
			isDir := obj.Size == 0
			t.Logf("  %s (size: %d, isDir: %v)", obj.Key, obj.Size, isDir)
		}
	})

	t.Run("ProxyWithTestFile", func(t *testing.T) {
		// Create a test file in root via direct S3
		directBackend := storage.NewS3Backend(
			"direct",
			cfg.S3Region,
			cfg.S3Bucket,
			cfg.S3Endpoint,
			cfg.S3AccessKey,
			cfg.S3AccessKey,
			true,
		)

		testContent := []byte("test for proxy")
		_, err := directBackend.Put(ctx, "proxy-test.txt", bytes.NewReader(testContent), int64(len(testContent)), "text/plain", nil)
		if err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
		defer func() { _ = directBackend.Delete(context.Background(), "proxy-test.txt") }()

		// Wait a bit for replication
		time.Sleep(100 * time.Millisecond)

		// List via proxy
		objects, err := proxyBackend.ListObjectsHierarchical(ctx, "", "/")
		if err != nil {
			t.Skipf("Proxy not available: %v", err)
			return
		}

		t.Logf("Proxy listing after creating test file: %d items", len(objects))

		foundFile := false
		for _, obj := range objects {
			t.Logf("  %s (size: %d)", obj.Key, obj.Size)
			if obj.Key == "proxy-test.txt" {
				foundFile = true
			}
		}

		if !foundFile {
			t.Error("Test file not visible via proxy")
		}
	})
}

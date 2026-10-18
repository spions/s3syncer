//go:build integration

package tests

import (
	"context"
	"testing"

	"github.com/spions/s3syncer/internal/storage"
)

// TestCompareProxyVsDirectS3 compares what proxy returns vs direct S3
func TestCompareProxyVsDirectS3(t *testing.T) {
	cfg := GetTestConfig()

	// Direct S3 connection
	directBackend := storage.NewS3Backend(
		"direct-s3",
		cfg.S3Region,
		cfg.S3Bucket,
		cfg.S3Endpoint,
		cfg.S3AccessKey,
		cfg.S3SecretKey,
		true,
	)

	// Proxy connection (testsync bucket)
	proxyBackend := storage.NewS3Backend(
		"proxy",
		cfg.S3Region,
		cfg.ProxyBucket,
		cfg.ProxyEndpoint,
		cfg.ProxyAccessKey,
		cfg.ProxySecretKey,
		true,
	)

	ctx := context.Background()

	t.Run("DirectS3_RootWithDelimiter", func(t *testing.T) {
		objects, err := directBackend.ListObjectsHierarchical(ctx, "", "/")
		if err != nil {
			t.Fatalf("Failed: %v", err)
		}

		t.Logf("Direct S3 root with delimiter: %d items", len(objects))
		for _, obj := range objects {
			t.Logf("  %s (size: %d)", obj.Key, obj.Size)
		}
	})

	t.Run("Proxy_RootWithDelimiter", func(t *testing.T) {
		objects, err := proxyBackend.ListObjectsHierarchical(ctx, "", "/")
		if err != nil {
			t.Fatalf("Failed: %v", err)
		}

		t.Logf("Proxy root with delimiter: %d items", len(objects))
		for _, obj := range objects {
			t.Logf("  %s (size: %d)", obj.Key, obj.Size)
		}
	})

	t.Run("DirectS3_RootWithoutDelimiter_First10", func(t *testing.T) {
		objects, err := directBackend.ListObjects(ctx, "")
		if err != nil {
			t.Fatalf("Failed: %v", err)
		}

		t.Logf("Direct S3 root without delimiter: %d total items", len(objects))
		t.Log("First 10:")
		for i, obj := range objects {
			if i >= 10 {
				break
			}
			t.Logf("  %s (size: %d)", obj.Key, obj.Size)
		}
	})
}

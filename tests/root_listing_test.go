//go:build integration

package tests

import (
	"context"
	"testing"

	"github.com/spions/s3syncer/internal/storage"
)

// TestRootListing tests what's in the root of the bucket
func TestRootListing(t *testing.T) {
	cfg := GetTestConfig()

	backend := storage.NewS3Backend(
		"test-backend",
		cfg.S3Region,
		cfg.S3Bucket,
		cfg.S3Endpoint,
		cfg.S3AccessKey,
		cfg.S3SecretKey,
		true,
	)

	ctx := context.Background()

	t.Run("RootWithDelimiter", func(t *testing.T) {
		objects, err := backend.ListObjectsHierarchical(ctx, "", "/")
		if err != nil {
			t.Fatalf("Failed: %v", err)
		}

		t.Logf("Root with delimiter: %d items", len(objects))
		for _, obj := range objects {
			isDir := obj.Size == 0
			t.Logf("  %s (size: %d, isDir: %v)", obj.Key, obj.Size, isDir)
		}
	})

	t.Run("RootWithoutDelimiter_Limited", func(t *testing.T) {
		// List first 10 objects without delimiter
		objects, err := backend.ListObjects(ctx, "")
		if err != nil {
			t.Fatalf("Failed: %v", err)
		}

		t.Logf("Root without delimiter: %d total items", len(objects))
		t.Log("First 10 items:")
		for i, obj := range objects {
			if i >= 10 {
				break
			}
			t.Logf("  %s (size: %d)", obj.Key, obj.Size)
		}
	})
}

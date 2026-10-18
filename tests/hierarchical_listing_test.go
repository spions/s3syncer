//go:build integration

package tests

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/spions/s3syncer/internal/storage"
)

// TestHierarchicalListingPerformance tests that hierarchical listing is much faster than flat listing
func TestHierarchicalListingPerformance(t *testing.T) {
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

	// Seed own fixtures so the test does not depend on pre-existing bucket contents
	for _, key := range []string{"hier-seed/a/1.txt", "hier-seed/a/2.txt", "hier-seed/b/3.txt"} {
		if _, err := backend.Put(ctx, key, bytes.NewReader([]byte("x")), 1, "text/plain", nil); err != nil {
			t.Fatalf("Failed to seed %s: %v", key, err)
		}
		defer func(k string) { _ = backend.Delete(ctx, k) }(key)
	}

	// Test hierarchical listing (should be fast)
	t.Run("HierarchicalListing", func(t *testing.T) {
		start := time.Now()
		objects, err := backend.ListObjectsHierarchical(ctx, "", "/")
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("Hierarchical listing failed: %v", err)
		}

		t.Logf("Hierarchical listing: found %d items in %v", len(objects), elapsed)

		// Should be very fast (under 1 second for root level)
		if elapsed > time.Second {
			t.Errorf("Hierarchical listing too slow: %v (expected < 1s)", elapsed)
		}

		// Should find at least one directory
		if len(objects) == 0 {
			t.Error("Expected at least one item in root")
		}

		// Check that directories are marked with size 0
		foundDir := false
		for _, obj := range objects {
			if obj.Size == 0 {
				foundDir = true
				t.Logf("Found directory: %s", obj.Key)
			}
		}
		if !foundDir {
			t.Error("Expected to find at least one directory (size=0)")
		}
	})

	// Test that hierarchical listing returns different results than flat
	t.Run("HierarchicalVsFlat", func(t *testing.T) {
		hierarchical, err := backend.ListObjectsHierarchical(ctx, "", "/")
		if err != nil {
			t.Fatalf("Hierarchical listing failed: %v", err)
		}

		// For comparison, list just first page of flat listing
		flat, err := backend.ListObjects(ctx, "")
		if err != nil {
			t.Fatalf("Flat listing failed: %v", err)
		}

		t.Logf("Hierarchical: %d items, Flat: %d items", len(hierarchical), len(flat))

		// Hierarchical should return much fewer items (only top-level)
		if len(hierarchical) >= len(flat) {
			t.Errorf("Hierarchical listing should return fewer items than flat (got %d vs %d)", len(hierarchical), len(flat))
		}
	})

	// Test listing subdirectory
	t.Run("SubdirectoryListing", func(t *testing.T) {
		// Create a nested object, then list its parent "directory"
		const key = "hier-test/sub/file.txt"
		if _, err := backend.Put(ctx, key, bytes.NewReader([]byte("x")), 1, "text/plain", nil); err != nil {
			t.Fatalf("Failed to create test object: %v", err)
		}
		defer func() { _ = backend.Delete(ctx, key) }()

		objects, err := backend.ListObjectsHierarchical(ctx, "hier-test/", "/")
		if err != nil {
			t.Fatalf("Subdirectory listing failed: %v", err)
		}

		t.Logf("Found %d items in hier-test/ directory", len(objects))

		// Should find subdirectories
		if len(objects) == 0 {
			t.Error("Expected items in hier-test/ directory")
		}

		// Log first few items
		for i, obj := range objects {
			if i >= 5 {
				break
			}
			t.Logf("  %s (size: %d)", obj.Key, obj.Size)
		}
	})
}

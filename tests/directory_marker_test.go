//go:build integration

package tests

import (
	"bytes"
	"context"
	"testing"

	"github.com/spions/s3syncer/internal/storage"
)

// TestDirectoryMarkers tests if we need directory marker objects
func TestDirectoryMarkers(t *testing.T) {
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

	t.Run("CheckIfFilmHasMarker", func(t *testing.T) {
		// Check if film/ exists as an actual object (directory marker)
		info, err := backend.Head(ctx, "film/")
		if err != nil {
			t.Logf("film/ does not exist as object: %v", err)
		} else {
			t.Logf("film/ exists as object with size: %d", info.Size)
		}
	})

	t.Run("CreateTestDirWithMarker", func(t *testing.T) {
		// Create a test directory with marker
		dirName := "test-dir-with-marker/"

		// Create directory marker (empty object)
		_, err := backend.Put(ctx, dirName, bytes.NewReader([]byte{}), 0, "application/x-directory", nil)
		if err != nil {
			t.Fatalf("Failed to create directory marker: %v", err)
		}
		t.Logf("Created directory marker: %s", dirName)

		// Create a file inside
		fileName := dirName + "test.txt"
		content := []byte("test content")
		_, err = backend.Put(ctx, fileName, bytes.NewReader(content), int64(len(content)), "text/plain", nil)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		t.Logf("Created file: %s", fileName)

		// List root with delimiter
		objects, err := backend.ListObjectsHierarchical(ctx, "", "/")
		if err != nil {
			t.Fatalf("Failed to list: %v", err)
		}

		t.Logf("Root listing after creating test dir:")
		foundTestDir := false
		for _, obj := range objects {
			t.Logf("  %s (size: %d)", obj.Key, obj.Size)
			if obj.Key == dirName {
				foundTestDir = true
			}
		}

		if !foundTestDir {
			t.Error("Test directory not found in listing")
		}

		// Cleanup
		_ = backend.Delete(context.Background(), fileName)
		_ = backend.Delete(context.Background(), dirName)
	})
}

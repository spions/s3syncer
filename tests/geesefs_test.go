//go:build integration

package tests

import (
	"bytes"
	"context"
	"testing"

	"github.com/spions/s3syncer/internal/storage"
)

// TestCreateRootFile creates a test file in root to verify GeeseFS can see it
func TestCreateRootFile(t *testing.T) {
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

	// Create a test file in root
	testContent := []byte("test file for GeeseFS")
	_, err := backend.Put(ctx, "test-geesefs.txt", bytes.NewReader(testContent), int64(len(testContent)), "text/plain", nil)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	t.Log("Created test-geesefs.txt in root")

	// Create a nested object so that a "directory" shows up in the root listing
	const dirKey = "gf-testdir/nested.txt"
	_, err = backend.Put(ctx, dirKey, bytes.NewReader(testContent), int64(len(testContent)), "text/plain", nil)
	if err != nil {
		t.Fatalf("Failed to create nested test file: %v", err)
	}
	defer func() { _ = backend.Delete(ctx, dirKey) }()

	// List root with delimiter
	objects, err := backend.ListObjectsHierarchical(ctx, "", "/")
	if err != nil {
		t.Fatalf("Failed to list: %v", err)
	}

	t.Logf("Root with delimiter after creating file: %d items", len(objects))
	foundFile := false
	foundDir := false
	for _, obj := range objects {
		isDir := obj.Size == 0
		t.Logf("  %s (size: %d, isDir: %v)", obj.Key, obj.Size, isDir)
		if obj.Key == "test-geesefs.txt" {
			foundFile = true
		}
		if obj.Key == "gf-testdir/" {
			foundDir = true
		}
	}

	if !foundFile {
		t.Error("Test file not found in listing")
	}
	if !foundDir {
		t.Error("gf-testdir/ directory not found in listing")
	}

	// Cleanup
	err = backend.Delete(ctx, "test-geesefs.txt")
	if err != nil {
		t.Logf("Warning: failed to cleanup test file: %v", err)
	}
}

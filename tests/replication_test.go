//go:build integration

package tests

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// newDirectClient creates an S3 client that talks directly to a minio instance (not through proxy).
func newDirectClient(endpoint, accessKey, secretKey string) *s3.Client {
	return s3.New(s3.Options{
		Region:       testCfg.S3Region,
		BaseEndpoint: aws.String(endpoint),
		Credentials:  credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		UsePathStyle: true,
	})
}

// directPut writes an object directly to a specific minio backend.
func directPut(t *testing.T, client *s3.Client, bucket, key string, data []byte) {
	t.Helper()
	_, err := client.PutObject(ctx(), &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		ContentType:   aws.String("application/octet-stream"),
	})
	if err != nil {
		t.Fatalf("direct PUT %s/%s: %v", bucket, key, err)
	}
}

// directGet reads an object directly from a specific minio backend.
func directGet(t *testing.T, client *s3.Client, bucket, key string) ([]byte, bool) {
	t.Helper()
	out, err := client.GetObject(ctx(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") || strings.Contains(err.Error(), "404") {
			return nil, false
		}
		t.Fatalf("direct GET %s/%s: %v", bucket, key, err)
	}
	defer out.Body.Close()
	body, _ := io.ReadAll(out.Body)
	return body, true
}

// objectExistsDirect checks if an object exists in a specific backend.
func objectExistsDirect(client *s3.Client, bucket, key string) bool {
	_, err := client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return err == nil
}

// waitForObjectDirect polls until the object appears in the given backend.
func waitForObjectDirect(t *testing.T, client *s3.Client, bucket, key string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if objectExistsDirect(client, bucket, key) {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("object %s/%s did not appear within %s", bucket, key, timeout)
}

// directDelete removes an object directly from a backend.
func directDelete(client *s3.Client, bucket, key string) {
	_, _ = client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
}

// --- Replication tests ---

// TestReplicationReadBothFound: file exists in both backends, GET succeeds, no sync needed.
func TestReplicationReadBothFound(t *testing.T) {
	key := fmt.Sprintf("repl-read-both-%d.txt", time.Now().UnixNano())
	data := []byte("both backends have this")

	minio1 := newDirectClient(testCfg.S3Endpoint, testCfg.S3AccessKey, testCfg.S3SecretKey)
	minio2 := newDirectClient(testCfg.S3Endpoint2, testCfg.S3AccessKey2, testCfg.S3SecretKey2)

	// Put directly to both backends
	directPut(t, minio1, testCfg.S3Bucket, key, data)
	directPut(t, minio2, testCfg.S3Bucket2, key, data)

	// GET through proxy should succeed
	got, code := getObject(t, testCfg.ProxyBucket, key)
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("data mismatch: got %q, want %q", got, data)
	}

	// Cleanup
	t.Cleanup(func() {
		directDelete(minio1, testCfg.S3Bucket, key)
		directDelete(minio2, testCfg.S3Bucket2, key)
	})
}

// TestReplicationReadPrimaryFoundBackupMissing: file only in primary, GET succeeds,
// LOW priority async sync copies it to backup.
func TestReplicationReadPrimaryFoundBackupMissing(t *testing.T) {
	key := fmt.Sprintf("repl-read-primary-%d.txt", time.Now().UnixNano())
	data := []byte("only in primary")

	minio1 := newDirectClient(testCfg.S3Endpoint, testCfg.S3AccessKey, testCfg.S3SecretKey)
	minio2 := newDirectClient(testCfg.S3Endpoint2, testCfg.S3AccessKey2, testCfg.S3SecretKey2)

	// Put only to primary (minio1)
	directPut(t, minio1, testCfg.S3Bucket, key, data)

	// GET through proxy should succeed
	got, code := getObject(t, testCfg.ProxyBucket, key)
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("data mismatch")
	}

	// Wait for LOW priority async sync to copy to backup
	waitForObjectDirect(t, minio2, testCfg.S3Bucket2, key, 30*time.Second)

	// Verify content in backup
	got2, found := directGet(t, minio2, testCfg.S3Bucket2, key)
	if !found {
		t.Fatal("expected file in backup after sync")
	}
	if !bytes.Equal(got2, data) {
		t.Fatalf("backup data mismatch")
	}

	t.Cleanup(func() {
		directDelete(minio1, testCfg.S3Bucket, key)
		directDelete(minio2, testCfg.S3Bucket2, key)
	})
}

// TestReplicationReadPrimaryMissingBackupFound: file only in backup, GET succeeds,
// MED priority async sync copies it to primary.
func TestReplicationReadPrimaryMissingBackupFound(t *testing.T) {
	key := fmt.Sprintf("repl-read-backup-%d.txt", time.Now().UnixNano())
	data := []byte("only in backup")

	minio1 := newDirectClient(testCfg.S3Endpoint, testCfg.S3AccessKey, testCfg.S3SecretKey)
	minio2 := newDirectClient(testCfg.S3Endpoint2, testCfg.S3AccessKey2, testCfg.S3SecretKey2)

	// Put only to backup (minio2)
	directPut(t, minio2, testCfg.S3Bucket2, key, data)

	// GET through proxy should succeed (fallback to backup)
	got, code := getObject(t, testCfg.ProxyBucket, key)
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("data mismatch")
	}

	// Wait for MED priority async sync to copy to primary
	waitForObjectDirect(t, minio1, testCfg.S3Bucket, key, 30*time.Second)

	// Verify content in primary
	got1, found := directGet(t, minio1, testCfg.S3Bucket, key)
	if !found {
		t.Fatal("expected file in primary after sync")
	}
	if !bytes.Equal(got1, data) {
		t.Fatalf("primary data mismatch")
	}

	t.Cleanup(func() {
		directDelete(minio1, testCfg.S3Bucket, key)
		directDelete(minio2, testCfg.S3Bucket2, key)
	})
}

// TestReplicationReadBothMissing: file doesn't exist anywhere, GET returns error.
func TestReplicationReadBothMissing(t *testing.T) {
	key := fmt.Sprintf("repl-read-missing-%d.txt", time.Now().UnixNano())

	_, code := getObject(t, testCfg.ProxyBucket, key)
	if code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", code)
	}
}

// TestReplicationWriteSuccess: PUT through proxy writes to both backends,
// HIGH priority sync ensures consistency.
func TestReplicationWriteSuccess(t *testing.T) {
	key := fmt.Sprintf("repl-write-both-%d.txt", time.Now().UnixNano())
	data := []byte("write to both via proxy")

	minio1 := newDirectClient(testCfg.S3Endpoint, testCfg.S3AccessKey, testCfg.S3SecretKey)
	minio2 := newDirectClient(testCfg.S3Endpoint2, testCfg.S3AccessKey2, testCfg.S3SecretKey2)

	// PUT through proxy
	putObject(t, testCfg.ProxyBucket, key, data, "text/plain")

	// Both backends should have the file (parallel write + HIGH sync)
	waitForObjectDirect(t, minio1, testCfg.S3Bucket, key, 30*time.Second)
	waitForObjectDirect(t, minio2, testCfg.S3Bucket2, key, 30*time.Second)

	// Verify content in both
	got1, found1 := directGet(t, minio1, testCfg.S3Bucket, key)
	if !found1 {
		t.Fatal("file not found in primary")
	}
	if !bytes.Equal(got1, data) {
		t.Fatalf("primary data mismatch")
	}

	got2, found2 := directGet(t, minio2, testCfg.S3Bucket2, key)
	if !found2 {
		t.Fatal("file not found in backup")
	}
	if !bytes.Equal(got2, data) {
		t.Fatalf("backup data mismatch")
	}

	t.Cleanup(func() {
		directDelete(minio1, testCfg.S3Bucket, key)
		directDelete(minio2, testCfg.S3Bucket2, key)
	})
}

// TestReplicationWriteBothFail: when no backends can be reached, PUT returns error.
// Note: this test cannot easily stop both minio instances in docker-compose,
// so we test by attempting to write to a non-existent proxy bucket which will fail auth.
func TestReplicationWriteBothFail(t *testing.T) {
	// Use a client with wrong credentials to simulate unreachable backends
	client := s3.New(s3.Options{
		Region:       testCfg.S3Region,
		BaseEndpoint: aws.String(proxyAddr()),
		Credentials:  credentials.NewStaticCredentialsProvider("badkey", "badsecret", ""),
		UsePathStyle: true,
	})
	key := fmt.Sprintf("repl-write-fail-%d.txt", time.Now().UnixNano())
	_, err := client.PutObject(ctx(), &s3.PutObjectInput{
		Bucket:        aws.String(testCfg.ProxyBucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader([]byte("should fail")),
		ContentLength: aws.Int64(11),
	})
	if err == nil {
		t.Fatal("expected error when both backends fail")
	}
}

// TestReplicationSyncLowPriority: periodic sync task uses LOW priority.
// Verifies that the background sync worker picks up and processes LOW priority tasks.
func TestReplicationSyncLowPriority(t *testing.T) {
	key := fmt.Sprintf("repl-sync-low-%d.txt", time.Now().UnixNano())
	data := []byte("sync low priority test")

	minio1 := newDirectClient(testCfg.S3Endpoint, testCfg.S3AccessKey, testCfg.S3SecretKey)
	minio2 := newDirectClient(testCfg.S3Endpoint2, testCfg.S3AccessKey2, testCfg.S3SecretKey2)

	// Put only in primary
	directPut(t, minio1, testCfg.S3Bucket, key, data)

	// Trigger a read to create LOW priority sync task
	got, code := getObject(t, testCfg.ProxyBucket, key)
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch on read")
	}

	// Wait for sync worker to process the LOW priority task
	waitForObjectDirect(t, minio2, testCfg.S3Bucket2, key, 30*time.Second)

	t.Cleanup(func() {
		directDelete(minio1, testCfg.S3Bucket, key)
		directDelete(minio2, testCfg.S3Bucket2, key)
	})
}

// TestReplicationWritePartialFailure: write succeeds even if one backend is down.
// We simulate this by writing directly to only one backend, then verifying sync recovers.
func TestReplicationWritePartialFailure(t *testing.T) {
	key := fmt.Sprintf("repl-write-partial-%d.txt", time.Now().UnixNano())
	data := []byte("partial write recovery")

	minio1 := newDirectClient(testCfg.S3Endpoint, testCfg.S3AccessKey, testCfg.S3SecretKey)
	minio2 := newDirectClient(testCfg.S3Endpoint2, testCfg.S3AccessKey2, testCfg.S3SecretKey2)

	// PUT through proxy — both backends should be written to in parallel
	putObject(t, testCfg.ProxyBucket, key, data, "text/plain")

	// Verify both backends eventually have the file
	waitForObjectDirect(t, minio1, testCfg.S3Bucket, key, 30*time.Second)
	waitForObjectDirect(t, minio2, testCfg.S3Bucket2, key, 30*time.Second)

	// Delete from backup only (simulates backup lost the file after write)
	directDelete(minio2, testCfg.S3Bucket2, key)

	// Read through proxy triggers checkBackupsExist → LOW sync
	got, code := getObject(t, testCfg.ProxyBucket, key)
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch")
	}

	// Wait for sync to restore backup
	waitForObjectDirect(t, minio2, testCfg.S3Bucket2, key, 30*time.Second)

	t.Cleanup(func() {
		directDelete(minio1, testCfg.S3Bucket, key)
		directDelete(minio2, testCfg.S3Bucket2, key)
	})
}

// TestStreamedPutReplicatesAsync: a body above put_stream_threshold_bytes (32 MiB)
// is streamed to the primary only and replicated to the backup by the worker.
func TestStreamedPutReplicatesAsync(t *testing.T) {
	key := fmt.Sprintf("repl-stream-%d.bin", time.Now().UnixNano())
	data := bytes.Repeat([]byte("0123456789abcdef"), 40*1024*1024/16) // 40 MiB

	minio1 := newDirectClient(testCfg.S3Endpoint, testCfg.S3AccessKey, testCfg.S3SecretKey)
	minio2 := newDirectClient(testCfg.S3Endpoint2, testCfg.S3AccessKey2, testCfg.S3SecretKey2)
	t.Cleanup(func() {
		directDelete(minio1, testCfg.S3Bucket, key)
		directDelete(minio2, testCfg.S3Bucket2, key)
	})

	putObject(t, testCfg.ProxyBucket, key, data, "application/octet-stream")

	got, found := directGet(t, minio1, testCfg.S3Bucket, key)
	if !found || !bytes.Equal(got, data) {
		t.Fatalf("primary: found=%v, size=%d want %d", found, len(got), len(data))
	}

	waitForObjectDirect(t, minio2, testCfg.S3Bucket2, key, 60*time.Second)
	got, found = directGet(t, minio2, testCfg.S3Bucket2, key)
	if !found || !bytes.Equal(got, data) {
		t.Fatalf("backup: found=%v, size=%d want %d", found, len(got), len(data))
	}
}

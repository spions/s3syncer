//go:build integration && cluster

package tests

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Cluster-mode end-to-end tests: two s3syncer instances (A, B) share one Redis and one MinIO pair.
// Run with `make test-cluster`, which starts deploy/docker/docker-compose.cluster.yml.
//
//	S3SYNCER_ADDR_A / S3SYNCER_ADDR_B   instance URLs (default http://localhost:8081 / :8082)
//	S3SYNCER_API_TOKEN                  management API token (default testtest)
//	S3SYNCER_COMPOSE                    compose command prefix used to stop/start an instance;
//	                                    the failover test is skipped without it

const asyncObjectSize = 1536 << 10 // above put_stream_threshold_bytes (1 MiB): replicated through the queue

func clusterAddr(instance string) string {
	if v := os.Getenv("S3SYNCER_ADDR_" + strings.ToUpper(instance)); v != "" {
		return v
	}
	if instance == "b" {
		return "http://localhost:8082"
	}
	return "http://localhost:8081"
}

func clusterClient(instance string) *s3.Client {
	return s3.New(s3.Options{
		Region:       testCfg.S3Region,
		BaseEndpoint: aws.String(clusterAddr(instance)),
		Credentials:  credentials.NewStaticCredentialsProvider(testCfg.ProxyAccessKey, testCfg.ProxySecretKey, ""),
		UsePathStyle: true,
	})
}

func apiToken() string {
	if v := os.Getenv("S3SYNCER_API_TOKEN"); v != "" {
		return v
	}
	return "testtest"
}

func apiCall(t *testing.T, method, instance, path string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, clusterAddr(instance)+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+apiToken())
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("%s %s on %s: %v", method, path, instance, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, body
}

func clusterPut(t *testing.T, instance, key string, data []byte) {
	t.Helper()
	_, err := clusterClient(instance).PutObject(ctx(), &s3.PutObjectInput{
		Bucket:        aws.String(testCfg.ProxyBucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	})
	if err != nil {
		t.Fatalf("PUT %s via %s: %v", key, instance, err)
	}
}

func clusterGet(t *testing.T, instance, key string) []byte {
	t.Helper()
	out, err := clusterClient(instance).GetObject(ctx(), &s3.GetObjectInput{
		Bucket: aws.String(testCfg.ProxyBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GET %s via %s: %v", key, instance, err)
	}
	defer out.Body.Close()
	b, _ := io.ReadAll(out.Body)
	return b
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// metricValue sums a metric (all series matching the label filter) from an instance's /metrics.
func metricValue(t *testing.T, instance, name, labelFilter string) float64 {
	t.Helper()
	resp, err := http.Get(clusterAddr(instance) + "/metrics")
	if err != nil {
		t.Fatalf("metrics of %s: %v", instance, err)
	}
	defer resp.Body.Close()
	var sum float64
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, name) || !strings.Contains(line, labelFilter) {
			continue
		}
		fields := strings.Fields(line)
		if v, err := strconv.ParseFloat(fields[len(fields)-1], 64); err == nil {
			sum += v
		}
	}
	return sum
}

func syncedTotal(t *testing.T) float64 {
	t.Helper()
	return metricValue(t, "a", "s3syncer_sync_tasks_total", `status="synced"`) +
		metricValue(t, "b", "s3syncer_sync_tasks_total", `status="synced"`)
}

func waitAllOnBackup(t *testing.T, keys []string, timeout time.Duration) {
	t.Helper()
	backup := newDirectClient(testCfg.S3Endpoint2, testCfg.S3AccessKey2, testCfg.S3SecretKey2)
	deadline := time.Now().Add(timeout)
	for {
		var missing []string
		for _, k := range keys {
			if !objectExistsDirect(backup, testCfg.S3Bucket2, k) {
				missing = append(missing, k)
			}
		}
		if len(missing) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d objects never reached the backup in %v, e.g. %s", len(missing), len(keys), timeout, missing[0])
		}
		time.Sleep(time.Second)
	}
}

func uniqueKeys(prefix string, n int) []string {
	base := fmt.Sprintf("cluster/%s-%d", prefix, time.Now().UnixNano())
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("%s/obj-%02d", base, i)
	}
	return keys
}

// An object written through instance A is readable through instance B.
func TestClusterPutOnAReadOnB(t *testing.T) {
	key := uniqueKeys("rw", 1)[0]
	data := randomBytes(100 << 10)
	clusterPut(t, "a", key, data)
	if got := clusterGet(t, "b", key); !bytes.Equal(got, data) {
		t.Fatalf("object read through B differs from what was written through A (%d vs %d bytes)", len(got), len(data))
	}
	clusterPut(t, "b", key+"-from-b", data)
	if got := clusterGet(t, "a", key+"-from-b"); !bytes.Equal(got, data) {
		t.Fatal("object written through B is not readable through A")
	}
}

// Tasks enqueued by both instances are processed, each exactly once, by whichever instance is free.
func TestClusterQueuedReplicationProcessedExactlyOnce(t *testing.T) {
	const n = 16
	keys := uniqueKeys("queue", n)
	before := syncedTotal(t)

	for i, k := range keys {
		inst := "a"
		if i%2 == 1 {
			inst = "b"
		}
		clusterPut(t, inst, k, randomBytes(asyncObjectSize))
	}
	waitAllOnBackup(t, keys, 90*time.Second)

	// Give a racing duplicate copy time to show up in the counters before comparing.
	time.Sleep(3 * time.Second)
	if got := syncedTotal(t) - before; got != n {
		t.Fatalf("s3syncer_sync_tasks_total{status=synced} grew by %v across both instances, want exactly %d: tasks were lost or copied twice", got, n)
	}
}

// The same resync fired at both instances at once starts exactly once; the loser gets
// 409 with the id of the running task, whichever instance it hit.
func TestClusterDuplicateResyncRejected(t *testing.T) {
	// A resync needs at least one object to take a moment; make sure there is one.
	clusterPut(t, "a", uniqueKeys("dup", 1)[0], randomBytes(10<<10))

	type result struct {
		code int
		body []byte
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	for _, inst := range []string{"a", "b"} {
		go func(inst string) {
			<-start
			code, body := apiCall(t, http.MethodPost, inst, "/api/tasks/resync?bucket="+testCfg.ProxyBucket)
			results <- result{code, body}
		}(inst)
	}
	close(start)

	var ok, conflict *result
	for i := 0; i < 2; i++ {
		r := <-results
		switch r.code {
		case http.StatusOK:
			ok = &r
		case http.StatusConflict:
			conflict = &r
		default:
			t.Fatalf("unexpected status %d: %s", r.code, r.body)
		}
	}
	if ok == nil || conflict == nil {
		t.Fatalf("want exactly one 200 and one 409 for two simultaneous starts, got ok=%v conflict=%v", ok != nil, conflict != nil)
	}
	var started, rejected map[string]string
	_ = json.Unmarshal(ok.body, &started)
	_ = json.Unmarshal(conflict.body, &rejected)
	if started["task_id"] == "" || rejected["task_id"] != started["task_id"] {
		t.Fatalf("409 must point at the running task %q, got %v", started["task_id"], rejected)
	}

	// Leave nothing running for the next test.
	deadline := time.Now().Add(90 * time.Second)
	for {
		_, body := apiCall(t, http.MethodGet, "a", "/api/tasks/"+started["task_id"])
		if !bytes.Contains(body, []byte(`"status":"running"`)) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("resync did not finish in 90s")
		}
		time.Sleep(time.Second)
	}
}

// A resync started through A is listed, readable and finishes as seen through B.
func TestClusterTasksVisibleAcrossInstances(t *testing.T) {
	code, body := apiCall(t, http.MethodPost, "a", "/api/tasks/resync?bucket="+testCfg.ProxyBucket)
	if code != http.StatusOK {
		t.Fatalf("start resync via A: %d %s", code, body)
	}
	var started map[string]string
	if err := json.Unmarshal(body, &started); err != nil || started["task_id"] == "" {
		t.Fatalf("bad start response %q: %v", body, err)
	}
	id := started["task_id"]

	type status struct {
		ID, Status, Owner string
	}
	get := func(instance string) status {
		code, body := apiCall(t, http.MethodGet, instance, "/api/tasks/"+id)
		if code != http.StatusOK {
			t.Fatalf("GET task %s via %s: %d %s", id, instance, code, body)
		}
		var raw struct {
			ID     string `json:"id"`
			Status string `json:"status"`
			Owner  string `json:"owner"`
		}
		if err := json.Unmarshal(body, &raw); err != nil {
			t.Fatal(err)
		}
		return status{raw.ID, raw.Status, raw.Owner}
	}

	if s := get("b"); s.ID != id || s.Owner == "" {
		t.Fatalf("instance B does not see the task started on A: %+v", s)
	}

	code, body = apiCall(t, http.MethodGet, "b", "/api/tasks")
	if code != http.StatusOK || !bytes.Contains(body, []byte(id)) {
		t.Fatalf("GET /api/tasks via B (%d) does not list task %s: %s", code, id, body)
	}

	deadline := time.Now().Add(90 * time.Second)
	for get("b").Status == "running" {
		if time.Now().After(deadline) {
			t.Fatal("resync did not finish in 90s")
		}
		time.Sleep(time.Second)
	}
	if s := get("b"); s.Status != "completed" {
		t.Fatalf("resync ended as %q, want completed", s.Status)
	}
}

func compose(t *testing.T, args ...string) {
	t.Helper()
	parts := strings.Fields(os.Getenv("S3SYNCER_COMPOSE"))
	cmd := exec.Command(parts[0], append(parts[1:], args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%s %s: %v\n%s", strings.Join(parts, " "), strings.Join(args, " "), err, out)
	}
}

// Tasks created by an instance that then goes away are finished by the survivor.
// The backup is taken down first so the tasks are guaranteed to still be queued
// (not yet copied) when instance A stops; then B must do all the work.
func TestClusterInstanceStopDoesNotLoseTasks(t *testing.T) {
	if os.Getenv("S3SYNCER_COMPOSE") == "" {
		t.Skip("S3SYNCER_COMPOSE not set: cannot stop containers")
	}
	if testing.Short() {
		t.Skip("takes about two minutes")
	}
	const n = 10
	keys := uniqueKeys("failover", n)
	t.Cleanup(func() {
		compose(t, "start", "minio2")
		compose(t, "start", "s3syncer-a")
	})

	compose(t, "stop", "minio2")
	bBefore := metricValue(t, "b", "s3syncer_sync_tasks_total", `status="synced"`)
	for _, k := range keys {
		clusterPut(t, "a", k, randomBytes(asyncObjectSize)) // primary only; the backup copy is queued
	}
	time.Sleep(5 * time.Second) // let the workers try (and fail) while the backup is down
	compose(t, "stop", "s3syncer-a")
	compose(t, "start", "minio2")

	// Failed copies are retried after 60s, so allow for one retry round on the survivor.
	waitAllOnBackup(t, keys, 150*time.Second)

	time.Sleep(3 * time.Second)
	if got := metricValue(t, "b", "s3syncer_sync_tasks_total", `status="synced"`) - bBefore; got != n {
		t.Fatalf("instance B synced %v tasks, want exactly %d (all work was left to it)", got, n)
	}
	for _, k := range keys {
		if got := clusterGet(t, "b", k); len(got) != asyncObjectSize {
			t.Fatalf("%s through B: %d bytes, want %d", k, len(got), asyncObjectSize)
		}
	}
}

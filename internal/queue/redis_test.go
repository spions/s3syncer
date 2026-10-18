package queue

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

const testLockTTL = 300 * time.Millisecond

// redisEnv is a Redis (miniredis by default, a real server when
// S3SYNCER_TEST_REDIS_ADDR is set) plus a fake clock shared by all stores
// built from it, so "two s3syncer instances" see the same time.
type redisEnv struct {
	t      *testing.T
	client *redis.Client
	mini   *miniredis.Miniredis // nil for a real server
	prefix string
	offset atomic.Int64 // ms added to Redis TIME inside the scripts
}

func newRedisEnv(t *testing.T) *redisEnv {
	t.Helper()
	e := &redisEnv{t: t, prefix: fmt.Sprintf("t%d", time.Now().UnixNano())}
	if addr := os.Getenv("S3SYNCER_TEST_REDIS_ADDR"); addr != "" {
		e.client = redis.NewClient(&redis.Options{Addr: addr})
		t.Cleanup(func() {
			ctx := context.Background()
			keys, _ := e.client.Keys(ctx, "s3s:"+e.prefix+":*").Result()
			if len(keys) > 0 {
				e.client.Del(ctx, keys...)
			}
			e.client.Close()
		})
		return e
	}
	e.mini = miniredis.RunT(t)
	e.client = redis.NewClient(&redis.Options{Addr: e.mini.Addr()})
	t.Cleanup(func() { e.client.Close() })
	return e
}

// store returns a new RedisStore ("instance") on the shared Redis.
func (e *redisEnv) store() *RedisStore {
	e.t.Helper()
	s, err := NewRedisStore(e.client, RedisOptions{
		KeyPrefix:  e.prefix,
		OpTimeout:  time.Second,
		timeOffset: func() time.Duration { return time.Duration(e.offset.Load()) * time.Millisecond },
	})
	if err != nil {
		e.t.Fatalf("NewRedisStore: %v", err)
	}
	return s
}

func (e *redisEnv) locker() *RedisLocker {
	return NewRedisLocker(e.client, RedisLockOptions{KeyPrefix: e.prefix, TTL: testLockTTL})
}

func (e *redisEnv) advance(d time.Duration) {
	e.offset.Add(d.Milliseconds())
}

func TestRedisStoreContract(t *testing.T) {
	runStoreContract(t, func(t *testing.T) harness {
		e := newRedisEnv(t)
		return harness{Store: e.store(), Advance: e.advance}
	})
}

func TestRedisLockerContract(t *testing.T) {
	runLockContract(t, func(t *testing.T) lockHarness {
		e := newRedisEnv(t)
		a, b := e.locker(), e.locker()
		t.Cleanup(func() { a.Close(); b.Close() })
		return lockHarness{
			A: a, B: b, TTL: testLockTTL,
			CrashA: a.Close,
			Advance: func(d time.Duration) {
				if e.mini != nil {
					e.mini.FastForward(d)
					return
				}
				time.Sleep(testLockTTL + 100*time.Millisecond) // real server: let the TTL pass
			},
		}
	})
}

// Two instances on one Redis must behave like one queue.
func TestRedisTwoInstancesShareQueue(t *testing.T) {
	e := newRedisEnv(t)
	a, b := e.store(), e.store()

	mustEnqueue(t, a, "bkt", "obj", "s", "d", PriorityLow)
	mustEnqueue(t, b, "bkt", "obj", "s", "d", PriorityHigh) // dedup across instances
	assertCounts(t, a, 1, 0, 0)
	assertCounts(t, b, 1, 0, 0)

	got := mustDequeue(t, b, 10)
	if len(got) != 1 || got[0].Priority != PriorityHigh {
		t.Fatalf("instance B should see the task with upgraded priority, got %+v", got)
	}
	if again := mustDequeue(t, a, 10); len(again) != 0 {
		t.Fatalf("instance A must not receive a task B already holds, got %+v", again)
	}
	assertCounts(t, a, 0, 1, 0)
}

func TestRedisTwoInstancesNoDuplicateDequeue(t *testing.T) {
	e := newRedisEnv(t)
	stores := []*RedisStore{e.store(), e.store(), e.store()}
	const total = 300
	for i := 0; i < total; i++ {
		mustEnqueue(t, stores[i%3], "b", fmt.Sprintf("k%03d", i), "s", "d", PriorityLow)
	}
	var (
		mu   sync.Mutex
		seen = map[string]int{}
		wg   sync.WaitGroup
	)
	for _, s := range stores {
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func(s *RedisStore) {
				defer wg.Done()
				for {
					tasks, err := s.Dequeue(7)
					if err != nil {
						t.Error(err)
						return
					}
					if len(tasks) == 0 {
						return
					}
					mu.Lock()
					for _, tk := range tasks {
						seen[tk.ID]++
					}
					mu.Unlock()
				}
			}(s)
		}
	}
	wg.Wait()
	if len(seen) != total {
		t.Fatalf("got %d distinct tasks, want %d", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("task %s dequeued %d times", id, n)
		}
	}
}

// Redis loses its script cache on restart/failover; operations must keep working.
func TestRedisScriptsReloadAfterFlush(t *testing.T) {
	e := newRedisEnv(t)
	s := e.store()
	mustEnqueue(t, s, "b", "one", "s", "d", PriorityLow)
	if err := e.client.ScriptFlush(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	mustEnqueue(t, s, "b", "two", "s", "d", PriorityLow) // EVALSHA -> NOSCRIPT -> EVAL
	if got := mustDequeue(t, s, 10); len(got) != 2 {
		t.Fatalf("expected 2 tasks after script flush, got %d", len(got))
	}
}

func TestRedisSchemaVersion(t *testing.T) {
	e := newRedisEnv(t)
	ctx := context.Background()

	s := e.store()
	if v, err := e.client.Get(ctx, s.key("schema_version")).Result(); err != nil || v != redisSchemaVersion {
		t.Fatalf("schema_version = %q, %v; want %q", v, err, redisSchemaVersion)
	}
	e.store() // same version: second instance starts fine

	if err := e.client.Set(ctx, s.key("schema_version"), "999", 0).Err(); err != nil {
		t.Fatal(err)
	}
	_, err := NewRedisStore(e.client, RedisOptions{KeyPrefix: e.prefix})
	if err == nil || !strings.Contains(err.Error(), "schema") {
		t.Fatalf("a newer schema in Redis must refuse to start, got %v", err)
	}
}

func TestRedisKeysShareHashTagAndTaskHasSafetyTTL(t *testing.T) {
	e := newRedisEnv(t)
	if e.mini == nil {
		t.Skip("inspects keys; miniredis only")
	}
	s := e.store()
	mustEnqueue(t, s, "b", "k", "s", "d", PriorityLow)
	mustDequeue(t, s, 1)

	keys := e.mini.Keys()
	if len(keys) == 0 {
		t.Fatal("no keys written")
	}
	for _, k := range keys {
		if !strings.Contains(k, "{q}") {
			t.Errorf("key %q lacks the {q} hash tag (breaks Redis Cluster scripts)", k)
		}
	}
	for _, k := range keys {
		if strings.Contains(k, ":t:") {
			if ttl := e.mini.TTL(k); ttl <= 0 {
				t.Errorf("task key %q has no safety TTL", k)
			}
		}
	}
}

func TestRedisUnavailableReturnsErrorFast(t *testing.T) {
	e := newRedisEnv(t)
	if e.mini == nil {
		t.Skip("needs to stop the server; miniredis only")
	}
	s := e.store()
	e.mini.Close()

	start := time.Now()
	if err := s.Enqueue("b", "k", "s", "d", PriorityLow); err == nil {
		t.Fatal("Enqueue must fail when Redis is down")
	}
	if _, err := s.Dequeue(10); err == nil {
		t.Fatal("Dequeue must fail when Redis is down")
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("operations took %v with Redis down; the op timeout must bound them", d)
	}
}

func TestRedisStoreRejectsEmptyAddr(t *testing.T) {
	if _, err := NewRedisClient(RedisConfig{}); err == nil {
		t.Fatal("empty address must be rejected")
	}
}

// --- compile-time contract checks ---
var (
	_ Store     = (*SQLiteStore)(nil)
	_ Store     = (*RedisStore)(nil)
	_ KeyLocker = (*MemoryLocker)(nil)
	_ KeyLocker = (*RedisLocker)(nil)
)

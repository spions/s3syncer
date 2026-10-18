package taskstate

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type redisEnv struct {
	client *redis.Client
	prefix string
	offset atomic.Int64
}

// newRedisEnv uses miniredis, or a real server when S3SYNCER_TEST_REDIS_ADDR is set.
func newRedisEnv(t *testing.T) *redisEnv {
	t.Helper()
	e := &redisEnv{prefix: fmt.Sprintf("ts%d", time.Now().UnixNano())}
	if addr := os.Getenv("S3SYNCER_TEST_REDIS_ADDR"); addr != "" {
		e.client = redis.NewClient(&redis.Options{Addr: addr})
		t.Cleanup(func() {
			ctx := context.Background()
			if keys, _ := e.client.Keys(ctx, "s3s:"+e.prefix+":*").Result(); len(keys) > 0 {
				e.client.Del(ctx, keys...)
			}
			e.client.Close()
		})
		return e
	}
	mr := miniredis.RunT(t)
	e.client = redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { e.client.Close() })
	return e
}

func (e *redisEnv) store(t *testing.T) *RedisStore {
	t.Helper()
	s, err := NewRedis(e.client, RedisOptions{
		KeyPrefix:  e.prefix,
		timeOffset: func() time.Duration { return time.Duration(e.offset.Load()) * time.Millisecond },
	})
	must(t, err)
	return s
}

func TestRedisContract(t *testing.T) {
	runContract(t, func(t *testing.T) harness {
		e := newRedisEnv(t)
		return harness{
			Store:               e.store(t),
			Advance:             func(d time.Duration) { e.offset.Add(d.Milliseconds()) },
			CrossInstanceCancel: true,
		}
	})
}

// Task started on instance A must be visible, listable and cancellable from instance B.
func TestRedisTwoInstancesShareState(t *testing.T) {
	e := newRedisEnv(t)
	a, b := e.store(t), e.store(t)

	must(t, a.Create("t1", "resync", "bkt", "pod-a"))
	must(t, a.UpdateProgress("t1", 10, 100, 1))

	s, err := b.Get("t1")
	must(t, err)
	if s.Owner != "pod-a" || s.ProcessedObjects != 10 {
		t.Fatalf("B sees %+v", s)
	}
	if list, _ := b.List(); len(list) != 1 {
		t.Fatalf("B lists %d tasks", len(list))
	}

	must(t, b.RequestCancel("t1"))
	if on, _ := a.CancelRequested("t1"); !on {
		t.Fatal("owner A must see the cancel requested through B")
	}
	if changed, _ := a.Finish("t1", StatusCancelled, ""); !changed {
		t.Fatal("owner must be able to finish the task")
	}
}

func TestRedisScriptsReloadAfterFlush(t *testing.T) {
	e := newRedisEnv(t)
	s := e.store(t)
	must(t, s.Create("a", "resync", "b1", "o"))
	must(t, e.client.ScriptFlush(context.Background()).Err())
	must(t, s.Create("b", "resync", "b2", "o"))
	if list, _ := s.List(); len(list) != 2 {
		t.Fatalf("got %d tasks", len(list))
	}
}

// The duplicate check must hold across instances, not just inside one process.
func TestRedisCreateConflictAcrossInstances(t *testing.T) {
	e := newRedisEnv(t)
	a, b := e.store(t), e.store(t)

	must(t, a.Create("from-a", "resync", "bkt", "pod-a"))
	err := b.Create("from-b", "resync", "bkt", "pod-b")
	var c *ConflictError
	if !errors.As(err, &c) || c.ExistingID != "from-a" {
		t.Fatalf("instance B: err = %v, want conflict with from-a", err)
	}
	if _, err := a.Finish("from-a", StatusCompleted, ""); err != nil {
		t.Fatal(err)
	}
	must(t, b.Create("from-b", "resync", "bkt", "pod-b"))
}

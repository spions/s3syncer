package queue

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// lockHarness builds two lockers that share state the way two s3syncer
// instances would (same Redis); for the in-memory locker both are the same object.
type lockHarness struct {
	A, B    KeyLocker
	Advance func(d time.Duration) // move time past lock TTL; nil if the locker has no TTL
	// CrashA simulates instance A dying: its background lease renewals stop,
	// but the keys it holds are NOT released.
	CrashA func()
	TTL    time.Duration
}

// runLockContract is the behavioural contract every KeyLocker must pass.
func runLockContract(t *testing.T, newHarness func(t *testing.T) lockHarness) {
	t.Helper()
	ctx := context.Background()

	t.Run("ExcludesSecondHolder", func(t *testing.T) {
		h := newHarness(t)
		release, ok, err := h.A.TryLock(ctx, "bucket/key")
		if err != nil || !ok {
			t.Fatalf("first TryLock = %v, %v", ok, err)
		}
		defer release()
		if _, ok, err := h.B.TryLock(ctx, "bucket/key"); err != nil || ok {
			t.Fatalf("second TryLock on another instance = %v, %v; want false", ok, err)
		}
	})

	t.Run("DifferentKeysAreIndependent", func(t *testing.T) {
		h := newHarness(t)
		r1, ok1, _ := h.A.TryLock(ctx, "b/one")
		r2, ok2, _ := h.B.TryLock(ctx, "b/two")
		if !ok1 || !ok2 {
			t.Fatalf("independent keys must both lock: %v %v", ok1, ok2)
		}
		r1()
		r2()
	})

	t.Run("ReleaseFreesKey", func(t *testing.T) {
		h := newHarness(t)
		release, ok, _ := h.A.TryLock(ctx, "b/key")
		if !ok {
			t.Fatal("lock failed")
		}
		release()
		release() // idempotent
		r2, ok, err := h.B.TryLock(ctx, "b/key")
		if err != nil || !ok {
			t.Fatalf("lock after release = %v, %v", ok, err)
		}
		r2()
	})

	t.Run("ExpiresWhenHolderDies", func(t *testing.T) {
		h := newHarness(t)
		if h.Advance == nil {
			t.Skip("implementation has no TTL")
		}
		// Crashed holder: lock taken, never released, renewal stopped.
		if _, ok, _ := h.A.TryLock(ctx, "b/key"); !ok {
			t.Fatal("lock failed")
		}
		h.CrashA()
		h.Advance(h.TTL + time.Second)
		r2, ok, err := h.B.TryLock(ctx, "b/key")
		if err != nil || !ok {
			t.Fatalf("lock after holder death = %v, %v; want acquired", ok, err)
		}
		r2()
	})

	t.Run("StaleReleaseDoesNotFreeNewOwner", func(t *testing.T) {
		h := newHarness(t)
		if h.Advance == nil {
			t.Skip("implementation has no TTL")
		}
		relA, ok, _ := h.A.TryLock(ctx, "b/key")
		if !ok {
			t.Fatal("lock failed")
		}
		h.CrashA()
		h.Advance(h.TTL + time.Second)
		relB, ok, _ := h.B.TryLock(ctx, "b/key")
		if !ok {
			t.Fatal("B must acquire after expiry")
		}
		defer relB()
		relA() // late release by the old owner must be a no-op
		if _, ok, _ := h.A.TryLock(ctx, "b/key"); ok {
			t.Fatal("old owner's release freed the new owner's lock")
		}
	})

	t.Run("MutualExclusionUnderContention", func(t *testing.T) {
		h := newHarness(t)
		var inside, violations atomic.Int32
		var wg sync.WaitGroup
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				l := h.A
				if i%2 == 1 {
					l = h.B
				}
				for n := 0; n < 20; n++ {
					release, ok, err := l.TryLock(ctx, "b/hot")
					if err != nil {
						t.Error(err)
						return
					}
					if !ok {
						continue
					}
					if inside.Add(1) != 1 {
						violations.Add(1)
					}
					time.Sleep(time.Millisecond)
					inside.Add(-1)
					release()
				}
			}(i)
		}
		wg.Wait()
		if v := violations.Load(); v != 0 {
			t.Fatalf("%d mutual-exclusion violations", v)
		}
	})
}

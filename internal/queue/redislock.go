package queue

import (
	"context"
	"crypto/rand"
	"crypto/sha1" //nolint:gosec // key hash, not security
	_ "embed"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/spions/s3syncer/internal/metrics"
)

//go:embed lua/lock_renew.lua
var luaLockRenew string

//go:embed lua/lock_release.lua
var luaLockRelease string

var errLockerClosed = errors.New("locker closed")

type RedisLockOptions struct {
	KeyPrefix string        // same meaning as RedisOptions.KeyPrefix
	TTL       time.Duration // lease length, default 60s; renewed every TTL/3 while held
}

// RedisLocker is the cluster-mode KeyLocker. A lock is a key with a random
// owner token and a TTL that a background goroutine keeps extending, so a
// crashed instance frees its locks within TTL instead of never.
type RedisLocker struct {
	rdb     *redis.Client
	prefix  string
	ttl     time.Duration
	renew   *redis.Script
	release *redis.Script

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewRedisLocker(rdb *redis.Client, opts RedisLockOptions) *RedisLocker {
	l := &RedisLocker{
		rdb:     rdb,
		prefix:  opts.KeyPrefix,
		ttl:     opts.TTL,
		renew:   redis.NewScript(luaLockRenew),
		release: redis.NewScript(luaLockRelease),
	}
	if l.prefix == "" {
		l.prefix = defaultKeyPrefix
	}
	if l.ttl == 0 {
		l.ttl = 60 * time.Second
	}
	l.ctx, l.cancel = context.WithCancel(context.Background())
	return l
}

// Close stops all lease renewals WITHOUT releasing the keys: it models a dead
// instance (locks expire by TTL) and is what shutdown does after the workers drained.
func (l *RedisLocker) Close() {
	l.cancel()
	l.wg.Wait()
}

func (l *RedisLocker) TryLock(ctx context.Context, key string) (func(), bool, error) {
	if l.ctx.Err() != nil {
		return nil, false, errLockerClosed
	}
	sum := sha1.Sum([]byte(key)) //nolint:gosec
	lk := redisKey(l.prefix, "lock:"+hex.EncodeToString(sum[:]))
	tok := make([]byte, 16)
	if _, err := rand.Read(tok); err != nil {
		return nil, false, err
	}
	token := hex.EncodeToString(tok)

	opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ok, err := l.rdb.SetNX(opCtx, lk, token, l.ttl).Result()
	if err != nil || !ok {
		return nil, false, err
	}

	hold, stop := context.WithCancel(l.ctx)
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		t := time.NewTicker(l.ttl / 3)
		defer t.Stop()
		for {
			select {
			case <-hold.Done():
				return
			case <-t.C:
				rctx, rcancel := context.WithTimeout(hold, l.ttl/3)
				n, err := l.renew.Run(rctx, l.rdb, []string{lk}, token, l.ttl.Milliseconds()).Int()
				rcancel()
				if err == nil && n == 0 {
					metrics.LockAttempts.WithLabelValues("lost").Inc() // expired under us: someone else may hold it now
					return
				}
				// transient Redis errors: keep trying until the lease runs out
			}
		}
	}()

	var once sync.Once
	return func() {
		once.Do(func() {
			stop()
			rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer rcancel()
			_ = l.release.Run(rctx, l.rdb, []string{lk}, token).Err()
		})
	}, true, nil
}

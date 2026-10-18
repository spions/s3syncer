package queue

import (
	"context"
	"crypto/sha1" //nolint:gosec // identifier hash, not security
	"crypto/tls"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisSchemaVersion is bumped on any change to the key layout or task format
// that an older binary cannot read. It is also part of every key ("v1").
const redisSchemaVersion = "1"

const (
	defaultKeyPrefix = "default"
	defaultOpTimeout = 5 * time.Second
	maintenanceBatch = 500
	maxMaintBatches  = 200 // bounds one maintenance call to 100k tasks
)

//go:embed lua/prelude.lua
var luaPrelude string

//go:embed lua/enqueue.lua
var luaEnqueue string

//go:embed lua/dequeue.lua
var luaDequeue string

//go:embed lua/mark_done.lua
var luaMarkDone string

//go:embed lua/mark_pending.lua
var luaMarkPending string

//go:embed lua/mark_failed.lua
var luaMarkFailed string

//go:embed lua/touch.lua
var luaTouch string

//go:embed lua/retry_failed.lua
var luaRetryFailed string

//go:embed lua/purge_expired.lua
var luaPurgeExpired string

//go:embed lua/reset_stuck.lua
var luaResetStuck string

// RedisConfig describes the connection to the shared Redis.
type RedisConfig struct {
	Addr     string
	Username string
	Password string
	DB       int
	TLS      bool
}

// NewRedisClient builds a client with bounded timeouts: a hung Redis must not hang the S3 proxy.
func NewRedisClient(cfg RedisConfig) (*redis.Client, error) {
	if cfg.Addr == "" {
		return nil, errors.New("redis address is empty")
	}
	opts := &redis.Options{
		Addr:         cfg.Addr,
		Username:     cfg.Username,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     16,
	}
	if cfg.TLS {
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return redis.NewClient(opts), nil
}

type RedisOptions struct {
	// KeyPrefix separates deployments sharing one Redis (default "default").
	KeyPrefix string
	// OpTimeout bounds every Redis round trip (default 5s).
	OpTimeout time.Duration

	// timeOffset shifts the clock seen by the scripts; tests only.
	timeOffset func() time.Duration
}

// RedisStore is the multi-instance Store. See lua/*.lua for the atomic operations.
type RedisStore struct {
	rdb       *redis.Client
	prefix    string
	opTimeout time.Duration
	offset    func() time.Duration

	enqueue, dequeue, markDone, markPending, markFailed *redis.Script
	touch, retryFailed, purgeExpired, resetStuck        *redis.Script
}

// NewRedisStore verifies the schema version, loads the scripts and returns the store.
func NewRedisStore(rdb *redis.Client, opts RedisOptions) (*RedisStore, error) {
	s := &RedisStore{rdb: rdb, prefix: opts.KeyPrefix, opTimeout: opts.OpTimeout, offset: opts.timeOffset}
	if s.prefix == "" {
		s.prefix = defaultKeyPrefix
	}
	if s.opTimeout == 0 {
		s.opTimeout = defaultOpTimeout
	}
	s.enqueue = redis.NewScript(luaPrelude + luaEnqueue)
	s.dequeue = redis.NewScript(luaPrelude + luaDequeue)
	s.markDone = redis.NewScript(luaPrelude + luaMarkDone)
	s.markPending = redis.NewScript(luaPrelude + luaMarkPending)
	s.markFailed = redis.NewScript(luaPrelude + luaMarkFailed)
	s.touch = redis.NewScript(luaPrelude + luaTouch)
	s.retryFailed = redis.NewScript(luaPrelude + luaRetryFailed)
	s.purgeExpired = redis.NewScript(luaPrelude + luaPurgeExpired)
	s.resetStuck = redis.NewScript(luaPrelude + luaResetStuck)

	ctx, cancel := context.WithTimeout(context.Background(), 3*s.opTimeout)
	defer cancel()

	if err := s.checkSchema(ctx); err != nil {
		return nil, err
	}
	// Warm the script cache. This also fails fast on a Redis that forbids scripts (ACL).
	for _, sc := range []*redis.Script{s.enqueue, s.dequeue, s.markDone, s.markPending, s.markFailed,
		s.touch, s.retryFailed, s.purgeExpired, s.resetStuck} {
		if err := sc.Load(ctx, rdb).Err(); err != nil {
			return nil, fmt.Errorf("load redis script (needs EVAL/SCRIPT permission): %w", err)
		}
	}
	return s, nil
}

func (s *RedisStore) checkSchema(ctx context.Context) error {
	k := s.key("schema_version")
	if err := s.rdb.SetNX(ctx, k, redisSchemaVersion, 0).Err(); err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	got, err := s.rdb.Get(ctx, k).Result()
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	if got != redisSchemaVersion {
		return fmt.Errorf("redis queue schema version is %q, this build supports %q: drain the queue or roll back", got, redisSchemaVersion)
	}
	return nil
}

// redisKey: every key carries the {q} hash tag so multi-key scripts stay in one Redis Cluster slot.
func redisKey(prefix, name string) string {
	return "s3s:" + prefix + ":v" + redisSchemaVersion + ":{q}:" + name
}

func (s *RedisStore) key(name string) string { return redisKey(s.prefix, name) }

func (s *RedisStore) keys() []string {
	return []string{s.key("pending"), s.key("pending_age"), s.key("processing"), s.key("failed")}
}

func (s *RedisStore) offsetMs() string {
	if s.offset == nil {
		return "0"
	}
	return strconv.FormatInt(s.offset().Milliseconds(), 10)
}

// run executes a script with the common ARGV prefix (clock offset, task key prefix).
func (s *RedisStore) run(sc *redis.Script, args ...any) (any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.opTimeout)
	defer cancel()
	full := append([]any{s.offsetMs(), s.key("t:")}, args...)
	return sc.Run(ctx, s.rdb, s.keys(), full...).Result()
}

func taskID(vb, key, src, tgt string) string {
	h := sha1.New() //nolint:gosec
	for _, p := range []string{vb, key, src, tgt} {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (s *RedisStore) Enqueue(virtualBucket, objectKey, source, target string, priority int) error {
	_, err := s.run(s.enqueue, taskID(virtualBucket, objectKey, source, target), virtualBucket, objectKey, source, target, priority)
	if err != nil {
		return fmt.Errorf("enqueue: %w", err)
	}
	return nil
}

// HasTask reports whether the task is still in the queue (its hash exists).
func (s *RedisStore) HasTask(virtualBucket, objectKey, source, target string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.opTimeout)
	defer cancel()
	n, err := s.rdb.Exists(ctx, s.key("t:")+taskID(virtualBucket, objectKey, source, target)).Result()
	if err != nil {
		return false, fmt.Errorf("has task: %w", err)
	}
	return n > 0, nil
}

func (s *RedisStore) Dequeue(limit int) ([]Task, error) {
	res, err := s.run(s.dequeue, limit)
	if err != nil {
		return nil, fmt.Errorf("dequeue: %w", err)
	}
	rows, _ := res.([]any)
	tasks := make([]Task, 0, len(rows))
	for _, r := range rows {
		f, _ := r.([]any)
		if len(f) != 9 {
			return nil, fmt.Errorf("dequeue: malformed task row %v", r)
		}
		str := func(i int) string { v, _ := f[i].(string); return v }
		num := func(i int) int64 { v, _ := strconv.ParseInt(str(i), 10, 64); return v }
		tasks = append(tasks, Task{
			ID:                str(0),
			VirtualBucket:     str(1),
			ObjectKey:         str(2),
			SourceBackendName: str(3),
			TargetBackendName: str(4),
			Priority:          int(num(5)),
			Attempts:          int(num(6)),
			LastError:         str(7),
			CreatedAt:         time.UnixMilli(num(8)),
			Status:            StatusProcessing,
		})
	}
	return tasks, nil
}

func (s *RedisStore) MarkDone(id string) error {
	_, err := s.run(s.markDone, id)
	return err
}

func (s *RedisStore) MarkPending(id string) error {
	_, err := s.run(s.markPending, id)
	return err
}

func (s *RedisStore) MarkFailed(id string, lastErr string) error {
	_, err := s.run(s.markFailed, id, lastErr, RetryBackoffBase.Milliseconds(), RetryBackoffCap.Milliseconds())
	return err
}

func (s *RedisStore) Touch(ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	_, err := s.run(s.touch, args...)
	return err
}

// batched repeats a maintenance script while it keeps returning full batches.
func (s *RedisStore) batched(sc *redis.Script, args ...any) (int64, error) {
	var total int64
	for i := 0; i < maxMaintBatches; i++ {
		res, err := s.run(sc, append(args, maintenanceBatch)...)
		if err != nil {
			return total, err
		}
		n, _ := res.(int64)
		total += n
		if n < maintenanceBatch {
			break
		}
	}
	return total, nil
}

func (s *RedisStore) RetryFailed(retryInterval, maxAge time.Duration) (int64, error) {
	return s.batched(s.retryFailed, retryInterval.Milliseconds(), maxAge.Milliseconds())
}

func (s *RedisStore) ResetStuckProcessing(stuckThreshold time.Duration) (int64, error) {
	return s.batched(s.resetStuck, stuckThreshold.Milliseconds())
}

func (s *RedisStore) PurgeExpired(maxAge time.Duration) (int64, error) {
	var deleted, start int64
	for i := 0; i < maxMaintBatches; i++ {
		res, err := s.run(s.purgeExpired, maxAge.Milliseconds(), start, maintenanceBatch)
		if err != nil {
			return deleted, err
		}
		pair, _ := res.([]any)
		if len(pair) != 2 {
			return deleted, fmt.Errorf("purge: malformed result %v", res)
		}
		d, _ := pair[0].(int64)
		scanned, _ := pair[1].(int64)
		deleted += d
		start += scanned - d // deleted entries shift the window
		if scanned < maintenanceBatch {
			break
		}
	}
	return deleted, nil
}

func (s *RedisStore) card(name string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.opTimeout)
	defer cancel()
	return s.rdb.ZCard(ctx, s.key(name)).Result()
}

func (s *RedisStore) PendingCount() (int64, error)    { return s.card("pending") }
func (s *RedisStore) ProcessingCount() (int64, error) { return s.card("processing") }
func (s *RedisStore) FailedCount() (int64, error)     { return s.card("failed") }

func (s *RedisStore) OldestPendingAge() (time.Duration, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.opTimeout)
	defer cancel()
	oldest, err := s.rdb.ZRangeWithScores(ctx, s.key("pending_age"), 0, 0).Result()
	if err != nil || len(oldest) == 0 {
		return 0, err
	}
	now, err := s.rdb.Time(ctx).Result()
	if err != nil {
		return 0, err
	}
	if s.offset != nil {
		now = now.Add(s.offset())
	}
	age := now.Sub(time.UnixMilli(int64(oldest[0].Score)))
	if age < 0 {
		age = 0
	}
	return age, nil
}

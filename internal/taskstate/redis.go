package taskstate

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed lua/prelude.lua
var luaPrelude string

//go:embed lua/create.lua
var luaCreate string

//go:embed lua/progress.lua
var luaProgress string

//go:embed lua/finish.lua
var luaFinish string

//go:embed lua/request_cancel.lua
var luaRequestCancel string

//go:embed lua/fail_stale.lua
var luaFailStale string

const (
	defaultKeyPrefix = "default"
	defaultOpTimeout = 5 * time.Second
	staleBatch       = 200
)

type RedisOptions struct {
	// KeyPrefix must match the queue's (redis.key_prefix) so one deployment shares one namespace.
	KeyPrefix string
	OpTimeout time.Duration

	// timeOffset shifts the clock seen by the scripts; tests only.
	timeOffset func() time.Duration
}

// RedisStore is the multi-instance Store: any instance can read, list and cancel
// tasks started on another one. Hashes `mt:<id>`, index zset `mt_index`, heartbeat zset `mt_running`.
type RedisStore struct {
	rdb       *redis.Client
	prefix    string
	opTimeout time.Duration
	offset    func() time.Duration

	create, progress, finish, requestCancel, failStale *redis.Script
}

func NewRedis(rdb *redis.Client, opts RedisOptions) (*RedisStore, error) {
	s := &RedisStore{rdb: rdb, prefix: opts.KeyPrefix, opTimeout: opts.OpTimeout, offset: opts.timeOffset}
	if s.prefix == "" {
		s.prefix = defaultKeyPrefix
	}
	if s.opTimeout == 0 {
		s.opTimeout = defaultOpTimeout
	}
	s.create = redis.NewScript(luaPrelude + luaCreate)
	s.progress = redis.NewScript(luaPrelude + luaProgress)
	s.finish = redis.NewScript(luaPrelude + luaFinish)
	s.requestCancel = redis.NewScript(luaPrelude + luaRequestCancel)
	s.failStale = redis.NewScript(luaPrelude + luaFailStale)

	ctx, cancel := context.WithTimeout(context.Background(), 3*s.opTimeout)
	defer cancel()
	for _, sc := range []*redis.Script{s.create, s.progress, s.finish, s.requestCancel, s.failStale} {
		if err := sc.Load(ctx, rdb).Err(); err != nil {
			return nil, fmt.Errorf("load redis script (needs EVAL/SCRIPT permission): %w", err)
		}
	}
	return s, nil
}

// Same layout as the queue: s3s:<prefix>:v1:{q}:<name>, so every key shares one cluster slot.
func (s *RedisStore) key(name string) string { return "s3s:" + s.prefix + ":v1:{q}:" + name }

func (s *RedisStore) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.opTimeout)
}

func (s *RedisStore) run(sc *redis.Script, args ...any) (any, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	off := "0"
	if s.offset != nil {
		off = strconv.FormatInt(s.offset().Milliseconds(), 10)
	}
	full := append([]any{off, s.key("mt:")}, args...)
	return sc.Run(ctx, s.rdb, []string{s.key("mt_index"), s.key("mt_running")}, full...).Result()
}

func (s *RedisStore) Create(id, taskType, bucket, owner string) error {
	res, err := s.run(s.create, id, taskType, bucket, owner, s.key("mt_active:"+taskType+":"+bucket))
	if err != nil {
		return fmt.Errorf("create task: %w", err)
	}
	switch v := res.(type) {
	case string:
		return &ConflictError{Type: taskType, Bucket: bucket, ExistingID: v}
	case int64:
		if v == 0 {
			return fmt.Errorf("create task: id %q already exists", id)
		}
	}
	return nil
}

func parseStatus(h map[string]string) *Status {
	num := func(k string) int64 { v, _ := strconv.ParseInt(h[k], 10, 64); return v }
	return &Status{
		ID: h["id"], Type: h["type"], Bucket: h["bucket"], Status: h["status"],
		TotalObjects: num("total"), ProcessedObjects: num("processed"), CurrentChunk: num("chunk"),
		Error: h["error"], Owner: h["owner"],
		CreatedAt: time.UnixMilli(num("created_ms")), UpdatedAt: time.UnixMilli(num("updated_ms")),
	}
}

func (s *RedisStore) Get(id string) (*Status, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	h, err := s.rdb.HGetAll(ctx, s.key("mt:"+id)).Result()
	if err != nil {
		return nil, fmt.Errorf("query task: %w", err)
	}
	if len(h) == 0 {
		return nil, ErrNotFound
	}
	return parseStatus(h), nil
}

func (s *RedisStore) List() ([]*Status, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	ids, err := s.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: s.key("mt_index"), Start: 0, Stop: -1, Rev: true}).Result()
	if err != nil {
		return nil, fmt.Errorf("query tasks: %w", err)
	}
	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HGetAll(ctx, s.key("mt:"+id))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("query tasks: %w", err)
	}
	out := make([]*Status, 0, len(ids))
	for _, c := range cmds {
		if h := c.Val(); len(h) > 0 { // hashes that expired are skipped
			out = append(out, parseStatus(h))
		}
	}
	return out, nil
}

func (s *RedisStore) UpdateProgress(id string, processed, total, chunk int64) error {
	_, err := s.run(s.progress, id, processed, total, chunk)
	return err
}

func (s *RedisStore) Touch(id string) error {
	_, err := s.run(s.progress, id, -1, 0, 0)
	return err
}

func (s *RedisStore) Finish(id, status, errMsg string) (bool, error) {
	res, err := s.run(s.finish, id, status, errMsg)
	if err != nil {
		return false, err
	}
	n, _ := res.(int64)
	return n == 1, nil
}

func (s *RedisStore) RequestCancel(id string) error {
	res, err := s.run(s.requestCancel, id)
	if err != nil {
		return err
	}
	if n, _ := res.(int64); n == 0 {
		return ErrNotRunning
	}
	return nil
}

func (s *RedisStore) CancelRequested(id string) (bool, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	v, err := s.rdb.HGet(ctx, s.key("mt:"+id), "cancel").Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	return v == "1", err
}

func (s *RedisStore) FailStale(olderThan time.Duration, reason string) (int64, error) {
	var total int64
	for i := 0; i < 50; i++ {
		res, err := s.run(s.failStale, olderThan.Milliseconds(), reason, staleBatch)
		if err != nil {
			return total, err
		}
		n, _ := res.(int64)
		total += n
		if n < staleBatch {
			break
		}
	}
	return total, nil
}

func (s *RedisStore) Prune(keep int) error {
	ctx, cancel := s.ctx()
	defer cancel()
	ids, err := s.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: s.key("mt_index"), Start: 0, Stop: -1, Rev: true}).Result()
	if err != nil {
		return err
	}
	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.StringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HGet(ctx, s.key("mt:"+id), "status")
	}
	_, _ = pipe.Exec(ctx) // redis.Nil for expired hashes is handled per command below

	finished := 0
	var drop []string
	for i, id := range ids {
		st, err := cmds[i].Result()
		switch {
		case errors.Is(err, redis.Nil): // hash gone: clean the index
			drop = append(drop, id)
		case err != nil:
			return err
		case st == StatusRunning:
		default:
			finished++
			if finished > keep {
				drop = append(drop, id)
			}
		}
	}
	if len(drop) == 0 {
		return nil
	}
	pipe = s.rdb.Pipeline()
	members := make([]any, len(drop))
	for i, id := range drop {
		pipe.Del(ctx, s.key("mt:"+id))
		members[i] = id
	}
	pipe.ZRem(ctx, s.key("mt_index"), members...)
	_, err = pipe.Exec(ctx)
	return err
}

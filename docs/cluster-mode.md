# Cluster mode (several instances on a shared Redis)

`mode: single` (default) keeps the sync queue in local SQLite and per-object locks in process memory.
That is only correct for **one** instance. To run several instances, set `mode: cluster` and point
all of them at the same Redis.

```yaml
mode: cluster
redis:
  addr: redis:6379
  password: ""        # or S3SYNCER_REDIS_PASSWORD
  tls: false
  key_prefix: default # one prefix per s3syncer deployment sharing a Redis
```

The mode is never inferred. `redis:` without `mode: cluster` and `mode: cluster` without
`redis.addr` are config errors. At startup the process exits if Redis is unreachable.

## What lives in Redis

All keys are `s3s:<key_prefix>:v1:{q}:<name>`; `{q}` is a hash tag, so everything stays in one Redis Cluster slot.

| Key | Type | Content |
|---|---|---|
| `t:<id>` | hash | one task; `<id>` = sha1(bucket, key, source, target), which gives deduplication. 7-day safety TTL |
| `pending` | zset | task ids; score = `(2-priority)*1e13 + created_ms` (priority first, then oldest) |
| `pending_age` | zset | the same ids scored by `created_ms` (for the oldest-pending metric) |
| `processing` | zset | score = last heartbeat, ms |
| `failed` | zset | score = failure time, ms |
| `mt:<task id>` | hash | state of a resync/bisync task (status, progress, owner, cancel flag) |
| `mt_index` | zset | task ids by creation time (listing, pruning to the newest 100 finished) |
| `mt_running` | zset | running task ids by last heartbeat (finds orphans) |
| `mt_active:<type>:<bucket>` | string | id of the running task for that type and bucket (the duplicate-start guard) |
| `lock:<sha1>` | string | per-object lock: owner token with a TTL (60s), renewed every 20s while held |
| `schema_version` | string | a build with a different version refuses to start |

Every queue operation is a Lua script (`internal/queue/lua/`), so it is atomic. Scripts are embedded in the
binary and sent via `EVALSHA` (falling back to `EVAL` after a Redis restart). Time comes from Redis `TIME`,
not from the instances, so clock skew between pods does not matter.

The Redis user needs `EVAL`, `EVALSHA` and `SCRIPT LOAD`.

## Behaviour to know

- **Crashed instance:** its in-flight tasks lose their heartbeat and return to `pending` after 5 minutes
  (`stuckThreshold`); its object locks expire within 60 seconds. Workers send heartbeats every minute.
- **Write during copy:** an object re-enqueued while it is being copied is copied again after the first pass
  (the task is marked dirty), so the newer version is not lost. This applies to single mode as well.
- **Redis is down:** queue operations return errors after the 5s operation timeout; `s3syncer_queue_up`
  drops to 0. Persistence matters: use AOF (`appendfsync everysec`) or accept that up to ~1s of queue state can be lost.
  The queue is derivable: `resync`/`bisync` rebuild it from the backends.
- **resync/bisync tasks are shared.** Their state lives in Redis too, so `task-status`, `task-list` and `task-cancel`
  (or the `/api/tasks` endpoints) work through any instance, e.g. behind a load balancer. The instance that started a task
  (its `owner`) runs it, refreshes a heartbeat every 5 seconds and, when someone requests a cancel through another
  instance, cancels it. A running task whose heartbeat is older than 90 seconds is marked `failed` (`owner lost: no heartbeat`)
  by any live instance. Unlike single mode, startup does **not** fail running tasks: they may belong to other instances.
  If the owner dies mid-task, start the task again; resync and bisync are idempotent.
- **No duplicate resync/bisync.** Only one task of a type can run per bucket, across all instances: starting a second one
  returns `409 Conflict` with the id of the running task (the CLI prints it), and cancelling a task that already ended
  fails. A `resync` and a `bisync` of the same bucket may run together. The check is atomic in Redis (and in one SQL
  statement in single mode). If the owner of a running task dies, a new start is refused until the task is reaped
  (up to ~2 minutes), because its heartbeat only goes stale after 90 seconds.
- **Redis Cluster:** scripts address task hashes by prefix instead of listing them in `KEYS`. This works because all keys
  share the `{q}` slot; it has been tested on a standalone Redis 7 and miniredis only.

## Failed tasks and deletes

- A failed task is retried with exponential backoff: after 1 minute, then 2, 4, ... up to 30 minutes. The failed set is ordered by the time a task becomes retryable, so tasks waiting out a long backoff never hide due ones.
- **Delete tasks (`DELETE:<backend>`) never expire**: they are retried until they succeed, however long a backend is down. Copy tasks that keep failing are purged after 12 hours.
- While a delete task for a backend exists, that backend's copy of the object is treated as deleted: it is neither served nor used as a source for restoring the primary or for bisync.

## Monitoring

| Metric | Meaning |
|---|---|
| `s3syncer_info{version,mode}` | build and mode |
| `s3syncer_queue_up{backend}` | 1 if the queue backend answered the scrape; while 0 the gauges below are absent |
| `s3syncer_queue_tasks{state}` | pending / processing / failed. **Shared queue: every instance reports the same value, aggregate with `max`, not `sum`** |
| `s3syncer_queue_oldest_pending_age_seconds` | age of the oldest pending task; grows when workers cannot keep up (same aggregation rule) |
| `s3syncer_queue_op_duration_seconds{backend,op}` | latency of queue operations |
| `s3syncer_queue_op_errors_total{backend,op}` | failed queue operations |
| `s3syncer_queue_transitions_total{kind}` | `retried`, `stuck_reset`, `purged`; a rising `stuck_reset` means workers die or copies outlive the lease |
| `s3syncer_key_lock_attempts_total{result}` | `acquired`, `contended`, `error`, `lost` (a lease expired while held) |

Example alert rules:

```yaml
groups:
  - name: s3syncer-queue
    rules:
      - alert: S3SyncerQueueBackendDown
        expr: min by (job) (s3syncer_queue_up) == 0
        for: 2m
        labels: {severity: critical}
        annotations: {summary: "s3syncer cannot reach its queue backend (Redis)"}
      - alert: S3SyncerQueueBacklogAging
        expr: max(s3syncer_queue_oldest_pending_age_seconds) > 900
        for: 10m
        labels: {severity: warning}
        annotations: {summary: "oldest pending sync task is older than 15 minutes"}
      - alert: S3SyncerTasksStuck
        expr: increase(s3syncer_queue_transitions_total{kind="stuck_reset"}[15m]) > 0
        labels: {severity: warning}
        annotations: {summary: "tasks were reset as stuck: a worker died or a copy outlived its lease"}
      - alert: S3SyncerLockLeaseLost
        expr: increase(s3syncer_key_lock_attempts_total{result="lost"}[15m]) > 0
        labels: {severity: warning}
        annotations: {summary: "a per-object lock expired while held; possible concurrent copy"}
      - alert: S3SyncerQueueErrors
        expr: sum(rate(s3syncer_queue_op_errors_total[5m])) > 0
        for: 5m
        labels: {severity: warning}
        annotations: {summary: "queue operations are failing"}
```

## Testing

```bash
go test -race ./internal/...   # contracts on SQLite and miniredis, multi-instance worker and task manager tests
make test-redis                # the queue and task-state contracts against a real Redis in Docker
make test-cluster              # end to end: Redis + two instances + two MinIO (deploy/docker/docker-compose.cluster.yml)
```

`make test-cluster` checks that an object written through one instance is readable through the other, that queued
replication tasks are processed exactly once across both instances, that a resync started on one instance is visible
and finishes as seen from the other, and that stopping an instance does not lose queued tasks. The environment
(`deploy/docker/docker-compose.cluster.yml`) is self-contained: its own compose project, MinIO pair and Redis, with the instances
published on ports 8081 and 8082 and MinIO on 19000 and 19002, so it can run next to `make up`.

`internal/queue/store_contract_test.go`, `lock_contract_test.go` and `internal/taskstate/contract_test.go` define the
behaviour every backend must satisfy. A new queue operation is added to the interface, to the contract, and then to each implementation.

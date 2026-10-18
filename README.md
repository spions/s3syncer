# s3syncer

[Русская версия](docs/README.RU.md)

`s3syncer` is an HTTP proxy that sits in front of several S3-compatible storages and keeps them in sync. Clients talk to it as to a regular S3 endpoint. Writes go to every backend, reads fall back to a backup when the primary is unavailable or missing the object, and everything that could not be replicated immediately is queued and retried in the background.

## Features

- **Fault tolerance** — automatic fallback to a backup backend when the primary is down or missing an object.
- **Replicated writes** — small PUTs are written to all backends in parallel; large ones are streamed to the primary and copied to the backups by the background worker. Failed copies are queued for retry.
- **Priority sync queue** — HIGH/MED/LOW priorities, deduplication, retries, per-object locks.
- **Two run modes** — `single` (one instance, local SQLite) and `cluster` (several instances on a shared Redis).
- **Resync / bisync** — one-way and two-way full synchronisation as background tasks, managed with the CLI or the REST API.
- **S3 compatibility** — AWS Signature V2/V4 (including presigned URLs and `aws-chunked` streaming uploads with trailing checksums), `Range` and conditional (`If-Match`, `If-None-Match`, `If-(Un)Modified-Since`) requests, listing with delimiters, CopyObject, symlinks for GeeseFS (`--symlink-target`) and s3fs (`mode=41471`).
- **Access control** — one access key per virtual bucket, valid for that bucket only; optional anonymous read per bucket.
- **Operations** — Prometheus metrics, JSON logs, memory budget for in-flight uploads, Helm chart, multi-arch container image.
- A single static binary (`CGO_ENABLED=0`, pure-Go SQLite).

## How it works

```
 client (aws cli / mc / s3fs / GeeseFS)
        │
        ▼
 ┌────────────────────────────────────────────┐
 │ s3syncer                                   │
 │  auth (SigV2/V4) ──► replicator            │
 │                         │        │         │
 │                  sync queue      │         │
 │          SQLite (single) / Redis (cluster) │
 │                         │        │         │
 │                      worker  task manager  │
 └─────────────────────────┼────────┼─────────┘
                           ▼        ▼
                   primary S3     backup S3 ...
```

- **PUT** below `put_stream_threshold_bytes` (32 MiB by default): the body is buffered in memory and written to all backends in parallel; success if at least one backend accepted it, the others are queued with HIGH priority. Larger (or unknown-size) bodies are streamed to the primary through a multipart upload with bounded memory and replicated to the backups by the worker.
- **GET**: HEAD on the primary (bounded by `operation_timeout_seconds`), then the body is streamed without a timeout. If the primary is missing the object, the backups are tried and a restore is queued. If a backend answers HEAD but fails to serve the body, another copy is used.
- **DELETE**: sent to all backends; failed ones are queued as delete tasks. A delete task is never dropped, and a copy that still waits for its delete is neither served nor replicated, so a deleted object does not come back.
- **Writes to one key are serialised** (PUT, DELETE, COPY), across instances in `cluster` mode, so overlapping writes cannot leave the backends with different versions.
- **Worker**: a pool works the queue with per-object locks; a slow copy never holds up the others. Failed tasks are retried with exponential backoff (1 min doubling to 30 min); copy tasks older than 12 h are purged, tasks stuck in `processing` for more than 5 minutes return to `pending`. On shutdown running tasks go back to `pending`.
- If a replication task cannot be recorded (queue backend down), the write fails with `503` and `Retry-After`, so the client retries instead of getting an acknowledgement for a write that exists on one backend only.

## Quick start

### Docker

```bash
cp deploy/config.example.yaml config.yaml   # edit endpoints and credentials
docker run -d --name s3syncer -p 8080:8080 \
  -v $PWD/config.yaml:/app/config.yaml:ro \
  -v s3syncer-data:/app/data \
  ghcr.io/spions/s3syncer:latest -config /app/config.yaml
```

or with Compose: [`deploy/docker/docker-compose.example.yml`](deploy/docker/docker-compose.example.yml).

### Kubernetes (Helm)

```bash
helm install s3syncer oci://ghcr.io/spions/charts/s3syncer --version 0.1.0 -f values.yaml
```

See [`deploy/charts/s3syncer/values.yaml`](deploy/charts/s3syncer/values.yaml). By default (`mode: single`) the chart runs one replica with a PVC for the SQLite queue; with `mode: cluster` it runs several replicas on a shared Redis. The config (with credentials) is kept in a Secret.

### From source

```bash
make build                       # → bin/s3syncer
./bin/s3syncer -config config.yaml
```

### Try it locally (two MinIO-compatible servers + s3syncer)

```bash
make up      # starts minio1, minio2 and s3syncer on localhost:8080
make down
```

Credentials in the dev environment are `testtest` / `testtest`.

### Use it

```bash
# AWS CLI (see "Client compatibility" for large files)
aws s3 ls s3://mybucket/ --endpoint-url http://localhost:8080

# MinIO client
mc alias set myproxy http://localhost:8080 ACCESS_KEY SECRET_KEY
mc ls myproxy/mybucket/

# s3fs
s3fs mybucket /mnt/mybucket -o url=http://localhost:8080 -o use_path_request_style -o passwd_file=~/.passwd-s3fs
```

## Client compatibility

Tested against the AWS CLI v2, `mc` and GeeseFS, including `aws-chunked` uploads over plain HTTP.

**Multipart upload is not supported.** Clients switch to it for big files: the AWS CLI above 8 MiB, `mc` above 64 MiB. Make them use a single PUT instead; s3syncer streams large single PUTs itself (bounded memory, no 5 GiB limit):

```bash
aws configure set default.s3.multipart_threshold 5GB        # AWS CLI
mc cp --disable-multipart big.bin myproxy/mybucket/         # mc
```

Other unsupported S3 features (ACL, tagging, versioning, copying between buckets, copying a specific version, multi-range requests) are answered with stubs, an error or ignored.

## Configuration

A YAML file passed with `-config <path>` (see [`deploy/config.example.yaml`](deploy/config.example.yaml)).

```yaml
mode: single
listen_addr: ":8080"
db_path: "/app/data/s3syncer.db"
sync_interval_seconds: 10
operation_timeout_seconds: 5

virtual_buckets:
  - virtualBucket: mybucket
    primaryBackendName: primary
    access_key_id: CHANGE_ME          # credentials clients use against s3syncer
    secret_access_key: CHANGE_ME
    public_read: false
    backends:
      - name: primary
        type: s3
        region: us-east-1
        bucket: my-primary-bucket
        endpoint: "https://s3.example.com"
        force_path_style: true
        access_key_id: CHANGE_ME
        secret_access_key: CHANGE_ME
      - name: backup
        # ...
```

| Parameter | Default | Description |
|---|---|---|
| `mode` | `single` | `single`: one instance, SQLite queue. `cluster`: several instances on a shared Redis ([details](#running-several-instances)) |
| `redis.addr` / `username` / `password` / `db` / `tls` / `key_prefix` | — | Redis connection, `cluster` mode only. The password can also come from `S3SYNCER_REDIS_PASSWORD` |
| `listen_addr` | `:8080` | HTTP listen address |
| `db_path` | `s3syncer.db` | SQLite file: the sync queue in `single` mode, local scratch state in `cluster` mode |
| `sync_interval_seconds` | `60` | worker cycle interval |
| `operation_timeout_seconds` | `5` | timeout for HEAD/PUT/DELETE/LIST towards backends (not applied to streamed bodies) |
| `log_level` | `info` | `debug`, `info`, `warn` or `error`. Per-request logging is at `debug` |
| `api_token` | empty | Bearer token for `/api/*` (also `S3SYNCER_API_TOKEN`). If empty, the API accepts loopback connections only |
| `put_memory_budget_bytes` | `268435456` | cap on memory held by in-flight PUT bodies (minimum 48 MiB); extra uploads wait instead of exhausting RAM. Keep it well below the container memory limit |
| `put_memory_wait_seconds` | `30` | how long an upload waits for memory before it is rejected with `503` + `Retry-After` |
| `request_body_idle_timeout_seconds` | `60` | an upload whose client sends nothing for this long is aborted (idle timeout: long uploads that keep sending are fine) |
| `shutdown_grace_seconds` | `30` | on SIGTERM, how long running requests and tasks get to finish; keep the pod's `terminationGracePeriodSeconds` above it |
| `put_stream_threshold_bytes` | `33554432` | PUT bodies of at least this size (or of unknown size) are streamed to the primary only; the worker copies them to the backups |
| `max_put_size_bytes` | `0` | maximum size of a single PUT body, `0` = unlimited (413 above the limit). Set it in production |
| `virtual_buckets[].virtualBucket` | — | bucket name exposed to clients (unique) |
| `virtual_buckets[].primaryBackendName` | — | must match one of `backends[].name` |
| `virtual_buckets[].access_key_id` / `secret_access_key` | — | client credentials; required, and each `access_key_id` must be unique. A key is valid for its own bucket only |
| `virtual_buckets[].public_read` | `false` | allow anonymous GET/HEAD of objects |
| `backends[].region` | — | only `[a-z0-9-]+` |
| `backends[].endpoint` | — | ASCII only |
| `backends[].force_path_style` | `false` | path-style addressing (needed for MinIO) |

The configuration is validated at startup; the process exits with a descriptive error if it is invalid.

### Running several instances

By default (`mode: single`) the sync queue lives in a local SQLite file and per-object locks in process memory, so exactly one instance may run. To scale out or run highly available, set `mode: cluster` and point every instance at one Redis:

```yaml
mode: cluster
redis:
  addr: "redis:6379"
```

The queue, the per-object locks and the state of `resync`/`bisync` tasks then live in Redis; any instance can serve requests, process tasks and answer `/api/tasks`, and a second `resync` of the same bucket is rejected with `409` while one is running. The mode is always explicit and never inferred. Redis layout, durability advice, metrics and alert examples: [`docs/cluster-mode.md`](docs/cluster-mode.md). The Helm chart supports it with `mode: cluster` and `replicaCount`.

## CLI

CLI commands call the REST API of a running server.

```bash
s3syncer -config config.yaml                    # run the server
s3syncer -config config.yaml resync [bucket]    # one-way sync: primary → backups
s3syncer -config config.yaml bisync [bucket]    # two-way sync
s3syncer -config config.yaml task-list          # list tasks
s3syncer -config config.yaml task-status <id>
s3syncer -config config.yaml task-cancel <id>
s3syncer -version
```

## REST API

| Method | Path | Description |
|---|---|---|
| POST | `/api/tasks/resync?bucket=X` | start resync (`409` if one is already running for the bucket) |
| POST | `/api/tasks/bisync?bucket=X` | start bisync (same rule) |
| GET | `/api/tasks/` | list tasks |
| GET | `/api/tasks/:id` | task status |
| DELETE | `/api/tasks/:id` | cancel task |
| GET | `/metrics` | Prometheus metrics (no auth) |
| GET | `/healthz` | liveness probe: the process serves HTTP (no auth) |
| GET | `/readyz` | readiness probe: the sync queue is reachable (no auth) |

```bash
curl -X POST -H "Authorization: Bearer $S3SYNCER_API_TOKEN" \
  "http://localhost:8080/api/tasks/resync?bucket=mybucket"
# {"bucket": "mybucket", "task_id": "…", "type": "resync"}
```

## Security notes

- Clients authenticate with AWS Signature V2/V4 or presigned URLs (15 minute clock-skew window; a signed `x-amz-content-sha256` is checked against the body, so a captured request cannot be replayed with other content). Rejected requests are logged at a limited rate. An access key is accepted for its own virtual bucket only; `ListBuckets` shows just that bucket. Terminate TLS in front of s3syncer when it is reachable over untrusted networks.
- `/api/*` requires `Authorization: Bearer <api_token>`. Without `api_token` it is reachable from loopback only (and not through a reverse proxy: requests carrying `X-Forwarded-For` are refused), so the CLI works inside the container or on the host but not from the network. The Helm chart refuses to enable the ingress without a token.
- Error responses for server-side failures say only "internal error"; details go to the log.
- `/metrics` is served without authentication, as is usual for Prometheus exporters. Restrict it at the network or reverse-proxy level.
- Backend credentials live in the config file; in Kubernetes the chart keeps it in a Secret.
- Report vulnerabilities as described in [SECURITY.md](.github/SECURITY.md).

## Monitoring

Logs are JSON on stdout (`log_level`). Main metrics:

| Metric | Type | Description |
|---|---|---|
| `s3syncer_sync_tasks_total{status}` | counter | synced / skipped / failed |
| `s3syncer_request_duration_seconds{backend,operation}` | histogram | backend operation latency |
| `s3syncer_http_requests_total{method,code}`, `s3syncer_http_request_duration_seconds{method}`, `s3syncer_http_inflight_requests` | counter, histogram, gauge | client traffic |
| `s3syncer_http_errors_total{code}` | counter | error responses |
| `s3syncer_backend_errors_total{backend,operation}` | counter | failed backend requests (not counting "not found") |
| `s3syncer_upload_memory_bytes`, `s3syncer_overload_rejections_total{reason}` | gauge, counter | upload memory in use; requests shed with `503` |
| `s3syncer_managed_tasks_active{type,bucket}` | gauge | running resync/bisync tasks |
| `s3syncer_managed_task_progress{task_id,type,bucket}` | gauge | task progress |
| `s3syncer_managed_tasks_total{type,status}` | counter | finished tasks |
| `s3syncer_queue_tasks{state}`, `s3syncer_queue_oldest_pending_age_seconds`, `s3syncer_queue_up`, … | gauge | sync queue state and health; the full list and alert examples are in [`docs/cluster-mode.md`](docs/cluster-mode.md#monitoring) |

## Limitations and notes

- **Multipart upload is not supported** (see [Client compatibility](#client-compatibility)).
- **Large objects have a window without redundancy.** Bodies at or above `put_stream_threshold_bytes` exist only on the primary until the worker has copied them. Set an `AbortIncompleteMultipartUpload` lifecycle rule on the backend buckets to clean up parts left by a crashed upload.
- **Bodies below the threshold are buffered in memory**; the total is capped by `put_memory_budget_bytes`. Use `max_put_size_bytes` to cap object size.
- **GET has no total timeout**: the body is streamed to the client; a backend has `operation_timeout_seconds` to start answering, then the next copy is tried.
- **One instance in `single` mode.** The queue lives in a local SQLite database and the locks are in-process; use `mode: cluster` for several replicas.
- Symlinks are at most 10 levels deep, cycles are detected.
- For clients other than GeeseFS, `delimiter=/` is added automatically to listings to avoid scanning huge buckets.

## Roadmap

- Multipart upload.
- Tiered storage (hot/cold) with lifecycle rules.

## License

Apache License 2.0 — see [LICENSE](LICENSE).

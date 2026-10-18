# Contributing to s3syncer

Thanks for your interest! This document describes how to build, test and navigate the code.

## Requirements

- Go (version from `go.mod`)
- Docker with Compose (integration tests)
- `pre-commit` and `golangci-lint` (optional but recommended)

## Build and run

```bash
make build                 # → bin/s3syncer (CGO_ENABLED=0)
./bin/s3syncer -config config.yaml
```

## Tests

| Command | What it does |
|---|---|
| `make test-unit` | unit tests of `internal/…` with `-race`, no Docker |
| `make test-integration` | starts two MinIO-compatible servers + s3syncer (`make up`) and runs `tests/` against `localhost` (build tag `integration`) |
| `make test-e2e` | runs the replication tests inside the Compose network |
| `make test-geesefs`, `test-symlinks`, `test-dirpersist` | FUSE-based GeeseFS scenarios (privileged container, `/dev/fuse`) |
| `make test` | unit + integration |
| `make down` | stops the environment and removes volumes |

Integration tests read their endpoints from `TEST_*` environment variables (see `tests/config.go`); the Makefile sets them for the local environment. The object storage image is configurable via `MINIO_IMAGE` / `MINIO_MC_IMAGE` (default: the `pgsty/silo` and `pgsty/mc` community images, because upstream MinIO images are no longer published on Docker Hub).

The dev environment (`deploy/docker/docker-compose.dev.yml`): `minio1` (9000/9001), `minio2` (9002/9003), `minio-init` (creates `testsync1`/`testsync2`), `s3syncer` (8080) and the test containers. Credentials are `testtest` / `testtest`.

Shell scenarios in `tests/` (`test-symlinks.sh`, `test-paginated-symlinks.sh`, `test-directory-persistence*.sh`, `entrypoint.sh`) run inside the GeeseFS test image (`tests/Dockerfile.geesefs`).

## Code layout

| Package | Responsibility |
|---|---|
| `cmd/s3syncer` | entry point: flags, CLI commands, HTTP mux, graceful shutdown |
| `internal/config` | YAML loading, defaults, validation |
| `internal/storage` | S3 backend on `aws-sdk-go-v2` (`Put/Get/Head/Delete/CopyObject/List*`) |
| `internal/proxy` | S3 HTTP handler, SigV2/V4 auth, GeeseFS/s3fs client detection and symlink handling |
| `internal/replicator` | read/write logic with parallel writes and fallback |
| `internal/queue` | SQLite sync queue (priorities, dedup, retry, purge) |
| `internal/worker` | background sync worker (per-key locks, direction-based skip, ETag/size skip) |
| `internal/taskmanager` | long-running resync/bisync tasks (chunks of 500, progress, cleanup on start) |
| `internal/api` | REST API for tasks |
| `internal/metrics` | Prometheus metrics |

Key invariants worth knowing before changing things:

- Queue deduplication is a `UNIQUE(virtual_bucket, object_key, source_backend_name, target_backend_name)` upsert: a `completed` task is reset to `pending`, an existing `pending/processing/failed` one gets its priority raised to the max.
- Priorities: HIGH=2 (failed write/delete), MED=1 (restore to primary), LOW=0 (resync, missing on backup).
- Delete tasks are encoded with a `DELETE:` prefix in `source_backend_name`.
- Symlink metadata is filtered out when replicating regular files.

## Debugging

Logs are JSON on stdout. The SQLite database can be inspected directly:

```bash
sqlite3 s3syncer.db "SELECT status, COUNT(*) FROM sync_tasks GROUP BY status;"
sqlite3 s3syncer.db "SELECT object_key, last_error FROM sync_tasks WHERE status='failed' LIMIT 10;"
sqlite3 s3syncer.db "UPDATE sync_tasks SET status='pending' WHERE status='processing';"   # reset stuck tasks
sqlite3 s3syncer.db "SELECT id, type, bucket, status, processed_objects, total_objects FROM managed_tasks;"
```

## Style and checks

```bash
pre-commit install                 # once
pre-commit run --all-files
make lint                          # golangci-lint
```

yamllint runs in strict mode with a 350 character line limit (warning).

## Pull requests

- Keep changes focused and add or update tests.
- Make sure `make test-unit` passes and, for changes in proxy/replication logic, `make test-integration`.
- Describe the motivation in the PR description.

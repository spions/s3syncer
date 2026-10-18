# AGENTS.md — s3syncer

Project description: `README.md` (Russian: `docs/README.RU.md`); development guide: `.github/CONTRIBUTING.md`; cluster mode: `docs/cluster-mode.md`.

## Commands

```bash
make build                 # build → bin/s3syncer
make test-unit             # unit tests (go test -race ./internal/...)
make test                  # unit + integration (Docker, 2x MinIO)
make up / make down        # dev environment (2x MinIO + s3syncer); make logs follows s3syncer
make test-integration      # integration tests against the dev environment
make test-e2e              # replication tests inside the compose network
make test-awscli           # real AWS CLI over plain HTTP (aws-chunked uploads)
make test-redis            # queue/task-state contract on a real Redis
make test-cluster          # cluster mode: Redis + two instances
make test-geesefs          # GeeseFS scenarios (FUSE, privileged container)
make lint                  # golangci-lint
```

Compose files live in `deploy/docker/`; the Helm chart in `deploy/charts/s3syncer`.

## Pre-commit

```bash
pre-commit install                      # once
pre-commit run --all-files
```

yamllint: line length 350 (warning), strict mode. check-yaml: `--allow-multiple-documents`.

## Key facts

- Build with **CGO_ENABLED=0** (pure-Go SQLite via modernc.org); **Go 1.26** (go.mod).
- Two run modes, always explicit: `single` (SQLite queue, one instance) and `cluster` (queue, locks and resync/bisync state in Redis, several instances).
- PUT bodies below `put_stream_threshold_bytes` are buffered in memory and written to all backends in parallel; larger ones are streamed to the primary and replicated asynchronously.
- GET has no timeout (the body is streamed); fallback is decided by HEAD (5 s).
- Worker: up to 500 tasks per cycle, per-key locks, direction-based skip; tasks older than 12 h are purged, tasks stuck in `processing` for more than 5 min return to `pending`.
- Queue deduplication: re-enqueueing a task raises its priority to the maximum.
- Symlinks: `--symlink-target` (GeeseFS) or `mode=41471` (s3fs), at most 10 levels.
- An access key is valid only for its own virtual bucket; `/api/*` needs `api_token` (loopback only without it).
- CLI commands (resync/bisync/task-*) talk to the running server over HTTP.
- Dev credentials: `testtest/testtest`; MinIO ports 9000/9002.

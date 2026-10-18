.PHONY: release build run stop logs up down test test-unit test-integration test-redis test-cluster test-e2e test-awscli test-geesefs test-symlinks test-dirpersist lint clean resync bisync

COMPOSE ?= docker compose -p s3syncer -f deploy/docker/docker-compose.dev.yml

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

# Test endpoints when the compose environment is published on localhost
TEST_ENV = TEST_S3_ENDPOINT=http://localhost:9000 \
	TEST_S3_ENDPOINT2=http://localhost:9002 \
	TEST_PROXY_ENDPOINT=http://localhost:8080 \
	S3SYNCER_ADDR=http://localhost:8080

# Build the binary
build:
	CGO_ENABLED=0 go build -ldflags "-s -w -X main.version=$(VERSION)" -o bin/s3syncer ./cmd/s3syncer

# Start the dev environment (2x MinIO + s3syncer) and wait until healthy
up:
	$(COMPOSE) up -d --build --wait minio1 minio2 s3syncer
	@echo "s3syncer:  http://localhost:8080"
	@echo "minio1:    http://localhost:9001 (console)"
	@echo "minio2:    http://localhost:9003 (console)"
	@echo "metrics:   http://localhost:8080/metrics"

# Stop the dev environment and remove its volumes
down:
	$(COMPOSE) down -v

# Aliases
run: up
stop: down

# Follow s3syncer logs
logs:
	$(COMPOSE) logs -f s3syncer

# All tests except the FUSE-based GeeseFS scenarios
test: test-unit test-integration

# Unit tests (no Docker)
test-unit:
	go test -race ./internal/...

# Integration tests: the environment comes from `make up`, tests run on the host.
# This is exactly what CI runs.
test-integration: up
	$(TEST_ENV) go test -tags integration -count=1 -timeout 300s ./tests/

# Queue/task-state contract on a real Redis (cluster mode); needs Docker
REDIS_TEST_PORT ?= 16379
test-redis:
	-@docker rm -f s3syncer-test-redis >/dev/null 2>&1
	docker run -d --rm --name s3syncer-test-redis -p $(REDIS_TEST_PORT):6379 redis:7-alpine >/dev/null
	@sleep 1
	S3SYNCER_TEST_REDIS_ADDR=localhost:$(REDIS_TEST_PORT) go test -race -count=1 ./internal/queue ./internal/taskstate ; rc=$$? ; docker stop s3syncer-test-redis >/dev/null ; exit $$rc

# End-to-end test of cluster mode: Redis + two s3syncer instances + their own MinIO pair (deploy/docker/docker-compose.cluster.yml).
# Separate compose project and ports (s3syncer 8081/8082, MinIO 19000/19002), does not interfere with `make up`.
CLUSTER_COMPOSE = docker compose -p s3syncer-cluster -f $(CURDIR)/deploy/docker/docker-compose.cluster.yml
test-cluster:
	$(CLUSTER_COMPOSE) up -d --build --wait s3syncer-a s3syncer-b
	@rc=0; \
	TEST_S3_ENDPOINT=http://localhost:19000 TEST_S3_ENDPOINT2=http://localhost:19002 \
		S3SYNCER_ADDR_A=http://localhost:8081 S3SYNCER_ADDR_B=http://localhost:8082 \
		S3SYNCER_COMPOSE="$(CLUSTER_COMPOSE)" \
		go test -tags "integration cluster" -count=1 -timeout 600s -run 'TestCluster' ./tests/ || rc=$$?; \
	$(CLUSTER_COMPOSE) down -v; exit $$rc

# The same tests inside the compose network (no ports published to the host)
test-e2e:
	$(COMPOSE) up --build --abort-on-container-exit --exit-code-from test-replication test-replication

# Real AWS CLI over plain HTTP (aws-chunked uploads, big files sent as one chunk)
test-awscli:
	$(COMPOSE) up --build --abort-on-container-exit --exit-code-from aws-cli-test aws-cli-test

# GeeseFS functional tests (FUSE, needs a privileged container)
test-geesefs:
	$(COMPOSE) up --build --abort-on-container-exit --exit-code-from geesefs-test geesefs-test

# Symlink tests
test-symlinks:
	$(COMPOSE) up --build --abort-on-container-exit --exit-code-from geesefs-paginated-symlink-test geesefs-paginated-symlink-test

# Directory persistence tests
test-dirpersist:
	$(COMPOSE) up --build --abort-on-container-exit --exit-code-from geesefs-dirtest geesefs-dirtest

lint:
	golangci-lint run ./...

# Resync: one-way sync primary -> backups
resync: build
	bin/s3syncer -config config.yaml resync $(BUCKET)

# Bisync: two-way sync (progress bar, resumable)
bisync: build
	bin/s3syncer -config config.yaml bisync $(BUCKET)

# Create a release tag: make release TAG=v0.1.0
release:
	@test -n "$(TAG)" || { echo "usage: make release TAG=vX.Y.Z" >&2; exit 1; }
	@echo "$(TAG)" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$$' || { echo "TAG must look like vMAJOR.MINOR.PATCH" >&2; exit 1; }
	@test -z "$$(git status --porcelain)" || { echo "working tree is not clean" >&2; exit 1; }
	git tag -a "$(TAG)" -m "Release $(TAG)"
	@echo "Created tag $(TAG). Publish it (this triggers the release workflow) with: git push origin $(TAG)"

# Clean up
clean:
	$(COMPOSE) down -v
	rm -rf bin/s3syncer s3syncer.db

# The compiler always runs natively on the build host ($BUILDPLATFORM) and cross-compiles for
# $TARGETARCH, so multi-arch images do not pay for emulating a Go build under QEMU.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags "-s -w -X main.version=${VERSION}" -o /s3syncer ./cmd/s3syncer

FROM alpine:3.24
RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 10001 s3syncer \
    && adduser -S -u 10001 -G s3syncer s3syncer \
    && mkdir -p /app/data \
    && chown -R s3syncer:s3syncer /app
WORKDIR /app
COPY --from=builder /s3syncer /app/s3syncer
USER s3syncer
EXPOSE 8080
ENTRYPOINT ["/app/s3syncer"]

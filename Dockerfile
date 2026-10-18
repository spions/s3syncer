FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -ldflags "-s -w -X main.version=${VERSION}" -o /s3syncer ./cmd/s3syncer

FROM alpine:3.22
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

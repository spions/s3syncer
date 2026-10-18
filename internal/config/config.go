package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

type Backend struct {
	Name            string `yaml:"name"`
	Type            string `yaml:"type"`
	Region          string `yaml:"region"`
	Bucket          string `yaml:"bucket"`
	Endpoint        string `yaml:"endpoint"`
	ForcePathStyle  bool   `yaml:"force_path_style"`
	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
}

type VirtualBucket struct {
	Name               string `yaml:"virtualBucket"`
	PrimaryBackendName string `yaml:"primaryBackendName"`
	AccessKeyID        string `yaml:"access_key_id"`
	SecretAccessKey    string `yaml:"secret_access_key"`
	// PublicRead allows unauthenticated GET/HEAD access to objects in this
	// bucket, analogous to an S3 bucket policy granting s3:GetObject to *.
	PublicRead bool      `yaml:"public_read"`
	Backends   []Backend `yaml:"backends"`
}

func (vb VirtualBucket) PrimaryBackend() (Backend, error) {
	for _, b := range vb.Backends {
		if b.Name == vb.PrimaryBackendName {
			return b, nil
		}
	}
	return Backend{}, fmt.Errorf("primary backend %q not found in virtual bucket %q", vb.PrimaryBackendName, vb.Name)
}

func (vb VirtualBucket) BackupBackends() []Backend {
	var backups []Backend
	for _, b := range vb.Backends {
		if b.Name != vb.PrimaryBackendName {
			backups = append(backups, b)
		}
	}
	return backups
}

const (
	// ModeSingle: one instance; task queue in local SQLite, per-object locks in memory.
	ModeSingle = "single"
	// ModeCluster: several instances; task queue and per-object locks in a shared Redis.
	ModeCluster = "cluster"
)

// RedisConfig is used in cluster mode only.
type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Username string `yaml:"username"`
	// Password can also be set via S3SYNCER_REDIS_PASSWORD.
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
	TLS      bool   `yaml:"tls"`
	// KeyPrefix separates several s3syncer deployments on one Redis (default "default").
	KeyPrefix string `yaml:"key_prefix"`
}

type Config struct {
	// Mode is "single" (default) or "cluster". It selects the queue backend and
	// is never inferred: a second instance on a local SQLite would silently diverge.
	Mode       string       `yaml:"mode"`
	Redis      *RedisConfig `yaml:"redis"`
	ListenAddr string       `yaml:"listen_addr"`
	// DBPath is the SQLite file: the task queue in single mode; in cluster mode
	// only the local state of resync/bisync tasks.
	DBPath                  string `yaml:"db_path"`
	SyncInterval            int    `yaml:"sync_interval_seconds"`
	OperationTimeoutSeconds int    `yaml:"operation_timeout_seconds"`
	// LogLevel is debug, info (default), warn or error. Per-request logging is at debug level.
	LogLevel string `yaml:"log_level"`
	// RequestBodyIdleTimeoutSeconds aborts an upload whose client sends nothing for this
	// long (default 60). It is an idle timeout, so long uploads that keep sending are fine.
	RequestBodyIdleTimeoutSeconds int `yaml:"request_body_idle_timeout_seconds"`
	// PutMemoryWaitSeconds: how long an upload waits for memory from the budget before
	// being rejected with 503 (default 30).
	PutMemoryWaitSeconds int `yaml:"put_memory_wait_seconds"`
	// ShutdownGraceSeconds is how long running requests and tasks get to finish on
	// SIGTERM (default 30). Set the pod's terminationGracePeriodSeconds above it.
	ShutdownGraceSeconds int `yaml:"shutdown_grace_seconds"`
	// APIToken protects /api/*. Can also be set via S3SYNCER_API_TOKEN.
	// When empty the API is reachable from loopback only.
	APIToken string `yaml:"api_token"`
	// MaxPutSizeBytes limits the size of a single PUT body (0 = unlimited).
	MaxPutSizeBytes int64 `yaml:"max_put_size_bytes"`
	// PutStreamThresholdBytes: PUT bodies of at least this size (or of unknown
	// size) are streamed to the primary only and replicated to backups
	// asynchronously, without buffering the whole body (default 32 MiB).
	PutStreamThresholdBytes int64 `yaml:"put_stream_threshold_bytes"`
	// PutMemoryBudgetBytes caps memory held by in-flight PUT bodies; further
	// uploads wait for memory (default 256 MiB, minimum 48 MiB).
	PutMemoryBudgetBytes int64           `yaml:"put_memory_budget_bytes"`
	VirtualBuckets       []VirtualBucket `yaml:"virtual_buckets"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	// Try parsing as array first (spec format)
	var buckets []VirtualBucket
	if err := yaml.Unmarshal(data, &buckets); err == nil && len(buckets) > 0 && buckets[0].Name != "" {
		cfg := &Config{
			Mode:                    ModeSingle,
			ListenAddr:              ":8080",
			DBPath:                  "s3syncer.db",
			SyncInterval:            60,
			OperationTimeoutSeconds: 5,
			APIToken:                os.Getenv("S3SYNCER_API_TOKEN"),
			PutMemoryBudgetBytes:    256 << 20,
			VirtualBuckets:          buckets,
		}
		if err := cfg.Validate(); err != nil {
			return nil, fmt.Errorf("invalid config: %w", err)
		}
		return cfg, nil
	}

	// Try full config format
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if cfg.Mode == "" {
		cfg.Mode = ModeSingle
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":8080"
	}
	if cfg.DBPath == "" {
		cfg.DBPath = "s3syncer.db"
	}
	if cfg.SyncInterval == 0 {
		cfg.SyncInterval = 60
	}
	if cfg.OperationTimeoutSeconds == 0 {
		cfg.OperationTimeoutSeconds = 5
	}

	if v := os.Getenv("S3SYNCER_API_TOKEN"); v != "" {
		cfg.APIToken = v
	}
	if cfg.Redis != nil {
		if v := os.Getenv("S3SYNCER_REDIS_PASSWORD"); v != "" {
			cfg.Redis.Password = v
		}
	}
	if cfg.MaxPutSizeBytes < 0 {
		return nil, fmt.Errorf("invalid config: max_put_size_bytes must not be negative")
	}
	if cfg.PutMemoryBudgetBytes == 0 {
		cfg.PutMemoryBudgetBytes = 256 << 20
	}
	if cfg.PutMemoryBudgetBytes < 48<<20 {
		return nil, fmt.Errorf("invalid config: put_memory_budget_bytes must be at least 50331648 (48 MiB)")
	}
	if cfg.PutStreamThresholdBytes < 0 {
		return nil, fmt.Errorf("invalid config: put_stream_threshold_bytes must not be negative")
	}
	if cfg.RequestBodyIdleTimeoutSeconds < 0 || cfg.PutMemoryWaitSeconds < 0 || cfg.ShutdownGraceSeconds < 0 {
		return nil, fmt.Errorf("invalid config: timeouts must not be negative")
	}
	if cfg.RequestBodyIdleTimeoutSeconds == 0 {
		cfg.RequestBodyIdleTimeoutSeconds = 60
	}
	if cfg.PutMemoryWaitSeconds == 0 {
		cfg.PutMemoryWaitSeconds = 30
	}
	if cfg.ShutdownGraceSeconds == 0 {
		cfg.ShutdownGraceSeconds = 30
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &cfg, nil
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	switch c.Mode {
	case "", ModeSingle:
		if c.Redis != nil {
			return fmt.Errorf("redis section is only valid with mode: cluster (current mode: single)")
		}
	case ModeCluster:
		if c.Redis == nil || c.Redis.Addr == "" {
			return fmt.Errorf("mode: cluster requires redis.addr")
		}
	default:
		return fmt.Errorf("unknown mode %q (want %q or %q)", c.Mode, ModeSingle, ModeCluster)
	}

	// AWS region format: lowercase letters, numbers, and hyphens only
	regionRegex := regexp.MustCompile(`^[a-z0-9-]+$`)

	switch strings.ToLower(c.LogLevel) {
	case "", "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("unknown log_level %q (want debug, info, warn or error)", c.LogLevel)
	}

	seenBuckets := map[string]bool{}
	seenKeys := map[string]string{}
	for _, vb := range c.VirtualBuckets {
		if vb.Name == "" {
			return fmt.Errorf("virtual bucket name cannot be empty")
		}
		if seenBuckets[vb.Name] {
			return fmt.Errorf("virtual bucket %q is defined twice", vb.Name)
		}
		seenBuckets[vb.Name] = true
		if vb.PrimaryBackendName == "" {
			return fmt.Errorf("virtual bucket %q: primary backend name cannot be empty", vb.Name)
		}

		primaryFound := false
		for _, backend := range vb.Backends {
			if backend.Name == "" {
				return fmt.Errorf("virtual bucket %q: backend name cannot be empty", vb.Name)
			}
			if backend.Region == "" {
				return fmt.Errorf("virtual bucket %q, backend %q: region cannot be empty", vb.Name, backend.Name)
			}
			// Check for non-ASCII characters in region
			if !regionRegex.MatchString(backend.Region) {
				return fmt.Errorf("virtual bucket %q, backend %q: invalid region %q (must contain only lowercase letters, numbers, and hyphens)", vb.Name, backend.Name, backend.Region)
			}
			if backend.Bucket == "" {
				return fmt.Errorf("virtual bucket %q, backend %q: bucket cannot be empty", vb.Name, backend.Name)
			}
			if backend.Endpoint == "" {
				return fmt.Errorf("virtual bucket %q, backend %q: endpoint cannot be empty", vb.Name, backend.Name)
			}
			// Check for non-ASCII characters in endpoint
			for _, r := range backend.Endpoint {
				if r > 127 {
					return fmt.Errorf("virtual bucket %q, backend %q: endpoint contains non-ASCII characters", vb.Name, backend.Name)
				}
			}

			if backend.Name == vb.PrimaryBackendName {
				primaryFound = true
			}
		}

		if !primaryFound {
			return fmt.Errorf("virtual bucket %q: primary backend %q not found in backends list", vb.Name, vb.PrimaryBackendName)
		}

		// Client credentials: an empty or shared key would let requests authenticate
		// as the wrong bucket (or with an empty secret).
		if vb.AccessKeyID == "" || vb.SecretAccessKey == "" {
			return fmt.Errorf("virtual bucket %q: access_key_id and secret_access_key must be set", vb.Name)
		}
		if other, dup := seenKeys[vb.AccessKeyID]; dup {
			return fmt.Errorf("virtual buckets %q and %q use the same access_key_id", other, vb.Name)
		}
		seenKeys[vb.AccessKeyID] = vb.Name
	}

	return nil
}

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			config: Config{
				VirtualBuckets: []VirtualBucket{
					{
						Name:               "test",
						PrimaryBackendName: "backend1",
						AccessKeyID:        "AK",
						SecretAccessKey:    "SK",
						Backends: []Backend{
							{
								Name:     "backend1",
								Region:   "us-east-1",
								Bucket:   "bucket1",
								Endpoint: "http://localhost:9000",
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid region with cyrillic",
			config: Config{
				VirtualBuckets: []VirtualBucket{
					{
						Name:               "test",
						PrimaryBackendName: "backend1",
						Backends: []Backend{
							{
								Name:     "backend1",
								Region:   "us-east-1/сщт",
								Bucket:   "bucket1",
								Endpoint: "http://localhost:9000",
							},
						},
					},
				},
			},
			wantErr: true,
			errMsg:  "invalid region",
		},
		{
			name: "invalid region with slash",
			config: Config{
				VirtualBuckets: []VirtualBucket{
					{
						Name:               "test",
						PrimaryBackendName: "backend1",
						Backends: []Backend{
							{
								Name:     "backend1",
								Region:   "us-east-1/com",
								Bucket:   "bucket1",
								Endpoint: "http://localhost:9000",
							},
						},
					},
				},
			},
			wantErr: true,
			errMsg:  "invalid region",
		},
		{
			name: "empty region",
			config: Config{
				VirtualBuckets: []VirtualBucket{
					{
						Name:               "test",
						PrimaryBackendName: "backend1",
						Backends: []Backend{
							{
								Name:     "backend1",
								Region:   "",
								Bucket:   "bucket1",
								Endpoint: "http://localhost:9000",
							},
						},
					},
				},
			},
			wantErr: true,
			errMsg:  "region cannot be empty",
		},
		{
			name: "primary backend not found",
			config: Config{
				VirtualBuckets: []VirtualBucket{
					{
						Name:               "test",
						PrimaryBackendName: "backend2",
						Backends: []Backend{
							{
								Name:     "backend1",
								Region:   "us-east-1",
								Bucket:   "bucket1",
								Endpoint: "http://localhost:9000",
							},
						},
					},
				},
			},
			wantErr: true,
			errMsg:  "primary backend \"backend2\" not found",
		},
		{
			name: "endpoint with cyrillic",
			config: Config{
				VirtualBuckets: []VirtualBucket{
					{
						Name:               "test",
						PrimaryBackendName: "backend1",
						Backends: []Backend{
							{
								Name:     "backend1",
								Region:   "us-east-1",
								Bucket:   "bucket1",
								Endpoint: "http://локалхост:9000",
							},
						},
					},
				},
			},
			wantErr: true,
			errMsg:  "endpoint contains non-ASCII characters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && err != nil {
				if tt.errMsg != "" && !contains(err.Error(), tt.errMsg) {
					t.Errorf("Validate() error = %v, want error containing %q", err, tt.errMsg)
				}
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || containsMiddle(s, substr)))
}

func containsMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

const modeTestBuckets = `
virtual_buckets:
  - virtualBucket: vb
    primaryBackendName: a
    access_key_id: AK
    secret_access_key: SK
    backends:
      - {name: a, region: us-east-1, bucket: b, endpoint: "http://x"}
`

func loadYAML(t *testing.T, body string) (*Config, error) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return Load(p)
}

func TestModeDefaultsToSingle(t *testing.T) {
	cfg, err := loadYAML(t, modeTestBuckets)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Mode != ModeSingle {
		t.Fatalf("mode = %q, want single", cfg.Mode)
	}
}

func TestModeClusterRequiresRedisAddr(t *testing.T) {
	if _, err := loadYAML(t, "mode: cluster\n"+modeTestBuckets); err == nil {
		t.Fatal("cluster without redis must be rejected")
	}
	if _, err := loadYAML(t, "mode: cluster\nredis: {db: 1}\n"+modeTestBuckets); err == nil {
		t.Fatal("cluster with empty redis.addr must be rejected")
	}
	cfg, err := loadYAML(t, "mode: cluster\nredis: {addr: 'redis:6379'}\n"+modeTestBuckets)
	if err != nil || cfg.Redis.Addr != "redis:6379" {
		t.Fatalf("valid cluster config rejected: %v", err)
	}
}

func TestModeSingleRejectsRedisSection(t *testing.T) {
	if _, err := loadYAML(t, "redis: {addr: 'redis:6379'}\n"+modeTestBuckets); err == nil {
		t.Fatal("redis section in single mode must be an error, not silently ignored")
	}
}

func TestModeUnknownRejected(t *testing.T) {
	if _, err := loadYAML(t, "mode: ha\n"+modeTestBuckets); err == nil {
		t.Fatal("unknown mode must be rejected")
	}
}

func TestRedisPasswordFromEnv(t *testing.T) {
	t.Setenv("S3SYNCER_REDIS_PASSWORD", "s3cret")
	cfg, err := loadYAML(t, "mode: cluster\nredis: {addr: 'redis:6379'}\n"+modeTestBuckets)
	if err != nil || cfg.Redis.Password != "s3cret" {
		t.Fatalf("password from env not applied: %v %+v", err, cfg.Redis)
	}
}

func validBucket(name, key string) VirtualBucket {
	return VirtualBucket{
		Name: name, PrimaryBackendName: "b", AccessKeyID: key, SecretAccessKey: "secret",
		Backends: []Backend{{Name: "b", Region: "us-east-1", Bucket: "x", Endpoint: "http://localhost:9000"}},
	}
}

func TestValidateClientCredentials(t *testing.T) {
	tests := []struct {
		name    string
		buckets []VirtualBucket
		wantErr string
	}{
		{"two buckets, distinct keys", []VirtualBucket{validBucket("a", "KA"), validBucket("b", "KB")}, ""},
		{"empty access key", []VirtualBucket{validBucket("a", "")}, "must be set"},
		{"shared access key", []VirtualBucket{validBucket("a", "SAME"), validBucket("b", "SAME")}, "same access_key_id"},
		{"duplicate bucket name", []VirtualBucket{validBucket("a", "KA"), validBucket("a", "KB")}, "defined twice"},
		{"empty secret", []VirtualBucket{func() VirtualBucket { v := validBucket("a", "KA"); v.SecretAccessKey = ""; return v }()}, "must be set"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := (&Config{VirtualBuckets: tt.buckets}).Validate()
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Fatalf("error = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestValidateLogLevel(t *testing.T) {
	for _, ok := range []string{"", "debug", "INFO", "warn", "error"} {
		if err := (&Config{LogLevel: ok}).Validate(); err != nil {
			t.Errorf("log_level %q rejected: %v", ok, err)
		}
	}
	if err := (&Config{LogLevel: "verbose"}).Validate(); err == nil {
		t.Error("unknown log_level must be rejected")
	}
}

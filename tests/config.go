package tests

import (
	"os"
)

// TestConfig holds test configuration that can be overridden via environment variables
type TestConfig struct {
	// S3 Backend 1 (primary, for direct S3 tests)
	S3Endpoint  string
	S3Bucket    string
	S3Region    string
	S3AccessKey string
	S3SecretKey string

	// S3 Backend 2 (backup, for direct replication verification)
	S3Endpoint2  string
	S3Bucket2    string
	S3AccessKey2 string
	S3SecretKey2 string

	// Proxy (for proxy tests)
	ProxyEndpoint  string
	ProxyBucket    string
	ProxyAccessKey string
	ProxySecretKey string
}

// GetTestConfig returns test configuration from environment or defaults
func GetTestConfig() TestConfig {
	return TestConfig{
		// Direct S3 backend 1 - defaults to docker-compose minio1
		S3Endpoint:  getEnv("TEST_S3_ENDPOINT", "http://minio1:9000"),
		S3Bucket:    getEnv("TEST_S3_BUCKET", "testsync1"),
		S3Region:    getEnv("TEST_S3_REGION", "us-east-1"),
		S3AccessKey: getEnv("TEST_S3_ACCESS_KEY", "testtest"),
		S3SecretKey: getEnv("TEST_S3_SECRET_KEY", "testtest"),

		// Direct S3 backend 2 - defaults to docker-compose minio2
		S3Endpoint2:  getEnv("TEST_S3_ENDPOINT2", "http://minio2:9000"),
		S3Bucket2:    getEnv("TEST_S3_BUCKET2", "testsync2"),
		S3AccessKey2: getEnv("TEST_S3_ACCESS_KEY2", "testtest"),
		S3SecretKey2: getEnv("TEST_S3_SECRET_KEY2", "testtest"),

		// Proxy - defaults to docker-compose s3syncer
		ProxyEndpoint:  getEnv("TEST_PROXY_ENDPOINT", "http://s3syncer:8080"),
		ProxyBucket:    getEnv("TEST_PROXY_BUCKET", "testsync"),
		ProxyAccessKey: getEnv("TEST_PROXY_ACCESS_KEY", "testtest"),
		ProxySecretKey: getEnv("TEST_PROXY_SECRET_KEY", "testtest"),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

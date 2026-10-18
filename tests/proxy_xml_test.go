//go:build integration

package tests

import (
	"context"
	"encoding/xml"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"io"
	"net/http"
	"testing"
	"time"
)

// TestProxyXMLResponse tests the actual XML response from proxy
func TestProxyXMLResponse(t *testing.T) {
	proxyURL := GetTestConfig().ProxyEndpoint + "/" + GetTestConfig().ProxyBucket + "?delimiter=%2F&prefix="

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", proxyURL, nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	req.Header.Set("User-Agent", "GeeseFS/0.43.5 (go1.26.1; linux; amd64)")

	cfg := GetTestConfig()
	const emptyPayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	req.Header.Set("X-Amz-Content-Sha256", emptyPayloadHash)
	err = v4.NewSigner().SignHTTP(ctx,
		aws.Credentials{AccessKeyID: cfg.ProxyAccessKey, SecretAccessKey: cfg.ProxySecretKey},
		req, emptyPayloadHash, "s3", cfg.S3Region, time.Now())
	if err != nil {
		t.Fatalf("Failed to sign request: %v", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Skipf("Proxy not available: %v", err)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	t.Logf("Response status: %d", resp.StatusCode)
	t.Logf("Response body:\n%s", string(body))

	// Parse XML
	type Content struct {
		Key          string `xml:"Key"`
		Size         int64  `xml:"Size"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		StorageClass string `xml:"StorageClass"`
	}
	type CommonPrefix struct {
		Prefix string `xml:"Prefix"`
	}
	type ListBucketResult struct {
		XMLName        xml.Name       `xml:"ListBucketResult"`
		Name           string         `xml:"Name"`
		Prefix         string         `xml:"Prefix"`
		Delimiter      string         `xml:"Delimiter"`
		MaxKeys        int            `xml:"MaxKeys"`
		IsTruncated    bool           `xml:"IsTruncated"`
		Contents       []Content      `xml:"Contents"`
		CommonPrefixes []CommonPrefix `xml:"CommonPrefixes"`
	}

	var result ListBucketResult
	if err := xml.Unmarshal(body, &result); err != nil {
		t.Fatalf("Failed to parse XML: %v", err)
	}

	t.Logf("Parsed result:")
	t.Logf("  Bucket: %s", result.Name)
	t.Logf("  Prefix: %q", result.Prefix)
	t.Logf("  Delimiter: %q", result.Delimiter)
	t.Logf("  MaxKeys: %d", result.MaxKeys)
	t.Logf("  IsTruncated: %v", result.IsTruncated)
	t.Logf("  Contents count: %d", len(result.Contents))
	t.Logf("  CommonPrefixes count: %d", len(result.CommonPrefixes))

	t.Log("Contents:")
	for _, c := range result.Contents {
		t.Logf("    %s (size: %d)", c.Key, c.Size)
	}

	t.Log("CommonPrefixes:")
	for _, cp := range result.CommonPrefixes {
		t.Logf("    %s", cp.Prefix)
	}

	if len(result.Contents) == 0 && len(result.CommonPrefixes) == 0 {
		t.Error("No contents and no common prefixes - bucket appears empty!")
	}
}

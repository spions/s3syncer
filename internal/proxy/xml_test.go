package proxy

import (
	"encoding/xml"
	"testing"
)

func TestXMLStructure(t *testing.T) {
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
		Xmlns          string         `xml:"xmlns,attr"`
		Name           string         `xml:"Name"`
		Prefix         string         `xml:"Prefix"`
		Delimiter      string         `xml:"Delimiter,omitempty"`
		MaxKeys        int            `xml:"MaxKeys"`
		IsTruncated    bool           `xml:"IsTruncated"`
		Contents       []Content      `xml:"Contents"`
		CommonPrefixes []CommonPrefix `xml:"CommonPrefixes"`
	}

	result := ListBucketResult{
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:        "testsync",
		Prefix:      "",
		Delimiter:   "/",
		MaxKeys:     1000,
		IsTruncated: false,
		Contents: []Content{
			{Key: "file1.txt", Size: 100, LastModified: "2026-01-01T00:00:00Z", ETag: "abc123", StorageClass: "STANDARD"},
		},
		CommonPrefixes: []CommonPrefix{
			{Prefix: "film/"},
			{Prefix: "dir2/"},
		},
	}

	xmlData, err := xml.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	xmlStr := xml.Header + string(xmlData)
	t.Logf("Generated XML:\n%s", xmlStr)

	// Verify it contains CommonPrefixes
	if !contains(xmlStr, "<CommonPrefixes>") {
		t.Error("XML does not contain <CommonPrefixes>")
	}
	if !contains(xmlStr, "<Prefix>film/</Prefix>") {
		t.Error("XML does not contain film/ prefix")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || contains(s[1:], substr)))
}

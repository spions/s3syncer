package storage

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// fakeS3 answers ListObjectsV2 and HEAD for a bucket holding three objects and counts HEADs.
func fakeS3(t *testing.T) (*S3Backend, *atomic.Int64) {
	t.Helper()
	var heads atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>b</Name><IsTruncated>false</IsTruncated>
<Contents><Key>a</Key><Size>1</Size><ETag>"1"</ETag></Contents>
<Contents><Key>b</Key><Size>2</Size><ETag>"2"</ETag></Contents>
<Contents><Key>c</Key><Size>3</Size><ETag>"3"</ETag></Contents>
</ListBucketResult>`)
		case r.Method == http.MethodHead:
			heads.Add(1)
			w.Header().Set("Content-Type", "text/plain")
			w.Header().Set("X-Amz-Meta-Origin", "test")
			w.Header().Set("Content-Length", "1")
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return NewS3Backend("fake", "us-east-1", "b", srv.URL, "ak", "sk", true), &heads
}

// Listing is the most frequent heavy call: it must not silently fan out into one HEAD per
// object for clients that never look at the metadata.
func TestListObjectsV2DoesNotHeadEveryObjectByDefault(t *testing.T) {
	b, heads := fakeS3(t)

	res, err := b.ListObjectsV2(context.Background(), "", "", "", "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Objects) != 3 {
		t.Fatalf("got %d objects, want 3", len(res.Objects))
	}
	if n := heads.Load(); n != 0 {
		t.Fatalf("plain listing issued %d HEAD requests", n)
	}
	if res.Objects[0].Metadata != nil {
		t.Fatal("metadata present without being requested")
	}
}

func TestListObjectsV2EnrichesMetadataWhenAskedFor(t *testing.T) {
	b, heads := fakeS3(t)

	res, err := b.ListObjectsV2(WithMetadata(context.Background()), "", "", "", "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if n := heads.Load(); n != 3 {
		t.Fatalf("enriched listing issued %d HEADs, want 3", n)
	}
	for _, o := range res.Objects {
		if o.Metadata["origin"] != "test" {
			t.Fatalf("%s: metadata not filled in: %v", o.Key, o.Metadata)
		}
	}
}

// Resync only needs keys; paging through a big bucket must not HEAD every object.
func TestPaginatedListingDoesNotHeadEveryObject(t *testing.T) {
	b, heads := fakeS3(t)

	var seen int
	total, err := b.ListObjectsPaginatedFull(context.Background(), "", 1000, func(objs []ObjectInfo) error {
		seen += len(objs)
		return nil
	})
	if err != nil || total != 3 || seen != 3 {
		t.Fatalf("total=%d seen=%d err=%v", total, seen, err)
	}
	if n := heads.Load(); n != 0 {
		t.Fatalf("paginated listing issued %d HEAD requests", n)
	}
}

// The SDK default of 10 idle connections per host makes a busy proxy churn connections;
// backends get a sized pool and a header timeout instead.
func TestBackendHTTPClientIsTunedForLoad(t *testing.T) {
	tr := backendHTTPClient().GetTransport()
	if tr.MaxIdleConnsPerHost < 64 {
		t.Errorf("MaxIdleConnsPerHost = %d, want at least 64", tr.MaxIdleConnsPerHost)
	}
	if tr.MaxConnsPerHost <= 0 {
		t.Error("MaxConnsPerHost is unlimited: a burst can overwhelm a backend")
	}
	if tr.ResponseHeaderTimeout <= 0 {
		t.Error("no ResponseHeaderTimeout: a backend that never answers hangs the request forever")
	}
}

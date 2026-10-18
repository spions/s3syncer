package storage

import (
	"context"
	"io"
	"time"
)

// SymlinkMetaKey is the S3 user-metadata key that marks an object as a symlink.
// The value is the target object key (path within the same bucket).
// Note: AWS SDK v2 normalizes metadata keys to lowercase and adds "--" prefix for keys with dashes.
const SymlinkMetaKey = "--symlink-target"

// S3fsSymlinkMode is the value of X-Amz-Meta-Mode that marks an object as a symlink in s3fs.
const S3fsSymlinkMode = "41471"
const S3fsModeKey = "mode"

type metadataKey struct{}

// WithMetadata asks list operations to fill in each object's user metadata and content type.
// Listing responses do not carry them, so this costs one HEAD per listed object: only the
// clients that need it (GeeseFS recognises symlinks from it) should pay for it.
func WithMetadata(ctx context.Context) context.Context {
	return context.WithValue(ctx, metadataKey{}, true)
}

func wantsMetadata(ctx context.Context) bool {
	v, _ := ctx.Value(metadataKey{}).(bool)
	return v
}

type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
	ContentType  string
	ETag         string
	ContentRange string            // set when only part of the object was returned (HTTP 206)
	Metadata     map[string]string // S3 user-metadata (without x-amz-meta- prefix)
}

// IsSymlink returns true if the object has a symlink target in metadata.
func (o *ObjectInfo) IsSymlink() bool {
	if o == nil || o.Metadata == nil {
		return false
	}
	// Check GeeseFS style symlink
	if _, ok := o.Metadata[SymlinkMetaKey]; ok {
		return true
	}
	// Check s3fs style symlink
	if mode, ok := o.Metadata[S3fsModeKey]; ok && mode == S3fsSymlinkMode {
		return true
	}
	return false
}

// SymlinkTarget returns the target key if this is a symlink, empty string otherwise.
func (o *ObjectInfo) SymlinkTarget() string {
	if o == nil || o.Metadata == nil {
		return ""
	}
	return o.Metadata[SymlinkMetaKey]
}

type StorageBackend interface {
	Name() string
	Put(ctx context.Context, key string, body io.Reader, size int64, contentType string, metadata map[string]string) (etag string, err error)
	// PutStream uploads a body of unknown or large size without needing it to be
	// seekable or held in memory (bounded buffering, multipart under the hood).
	PutStream(ctx context.Context, key string, body io.Reader, contentType string, metadata map[string]string) (etag string, err error)
	Get(ctx context.Context, key string) (io.ReadCloser, *ObjectInfo, error)
	// GetRange is Get for a byte range ("bytes=0-99", "bytes=-500"); an empty range means the whole
	// object. When only part is returned ObjectInfo.ContentRange is set and Size is the part's length.
	// An unsatisfiable range yields ErrInvalidRange.
	GetRange(ctx context.Context, key, rng string) (io.ReadCloser, *ObjectInfo, error)
	Head(ctx context.Context, key string) (*ObjectInfo, error)
	Delete(ctx context.Context, key string) error
	CopyObject(ctx context.Context, srcKey, dstKey string, metadata map[string]string) (etag string, err error)
	ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error)
	// ListObjectsPaginatedFull lists all objects in pages, calling fn for each page
	// Returns total count and error
	ListObjectsPaginatedFull(ctx context.Context, prefix string, pageSize int, fn func([]ObjectInfo) error) (int64, error)
	// ListObjectsV2 provides raw-ish access to S3 ListObjectsV2
	ListObjectsV2(ctx context.Context, prefix, delimiter, continuationToken, startAfter string, maxKeys int32) (*ListObjectsV2Result, error)
	// ListObjectsHierarchical lists objects with delimiter for directory-like browsing
	// Returns objects and common prefixes (directories) at the current level only
	ListObjectsHierarchical(ctx context.Context, prefix, delimiter string) ([]ObjectInfo, error)
}

type ListObjectsV2Result struct {
	Objects        []ObjectInfo
	CommonPrefixes []string
	IsTruncated    bool
	NextToken      string
}

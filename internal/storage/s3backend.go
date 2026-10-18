package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// Streaming uploads buffer up to streamPartSize*streamConcurrency bytes in
// memory per upload, regardless of object size.
const (
	streamPartSize    = 16 << 20
	streamConcurrency = 2
)

type S3Backend struct {
	name     string
	bucket   string
	client   *s3.Client
	uploader *manager.Uploader
}

// Connection pool per backend. The SDK default keeps only 10 idle connections per host, so a
// proxy under load keeps opening and closing connections (a TLS handshake each); the cap on
// total connections stops a traffic burst from overwhelming the backend, extra requests wait.
const (
	maxIdleConnsPerHost   = 128
	maxConnsPerHost       = 256
	responseHeaderTimeout = 60 * time.Second // a backend that accepted the request but never answers
)

func backendHTTPClient() *awshttp.BuildableClient {
	return awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
		tr.MaxIdleConns = 2 * maxIdleConnsPerHost
		tr.MaxIdleConnsPerHost = maxIdleConnsPerHost
		tr.MaxConnsPerHost = maxConnsPerHost
		tr.ResponseHeaderTimeout = responseHeaderTimeout
	})
}

func NewS3Backend(name, region, bucket, endpoint, accessKey, secretKey string, forcePathStyle bool) *S3Backend {
	client := s3.New(s3.Options{
		Region:       region,
		BaseEndpoint: aws.String(endpoint),
		Credentials:  credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		UsePathStyle: forcePathStyle,
		HTTPClient:   backendHTTPClient(),
	})
	return &S3Backend{
		name:   name,
		bucket: bucket,
		client: client,
		uploader: manager.NewUploader(client, func(u *manager.Uploader) {
			u.PartSize = streamPartSize
			u.Concurrency = streamConcurrency
		}),
	}
}

func (b *S3Backend) Name() string { return b.name }

func (b *S3Backend) Put(ctx context.Context, key string, body io.Reader, size int64, contentType string, metadata map[string]string) (string, error) {
	input := &s3.PutObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
		Body:   body,
	}
	if contentType != "" {
		input.ContentType = aws.String(contentType)
	}
	if size >= 0 {
		input.ContentLength = aws.Int64(size)
	}
	if len(metadata) > 0 {
		input.Metadata = metadata
	}
	out, err := b.client.PutObject(ctx, input)
	if err != nil {
		return "", fmt.Errorf("s3 put %s/%s: %w", b.bucket, key, err)
	}
	etag := ""
	if out.ETag != nil {
		etag = *out.ETag
	}
	return etag, nil
}

func (b *S3Backend) PutStream(ctx context.Context, key string, body io.Reader, contentType string, metadata map[string]string) (string, error) {
	input := &s3.PutObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
		Body:   body,
	}
	if contentType != "" {
		input.ContentType = aws.String(contentType)
	}
	if len(metadata) > 0 {
		input.Metadata = metadata
	}
	out, err := b.uploader.Upload(ctx, input)
	if err != nil {
		return "", fmt.Errorf("s3 stream put %s/%s: %w", b.bucket, key, err)
	}
	return aws.ToString(out.ETag), nil
}

func (b *S3Backend) Get(ctx context.Context, key string) (io.ReadCloser, *ObjectInfo, error) {
	return b.GetRange(ctx, key, "")
}

// ErrInvalidRange means the requested byte range cannot be satisfied (HTTP 416).
var ErrInvalidRange = errors.New("invalid range")

func (b *S3Backend) GetRange(ctx context.Context, key, rng string) (io.ReadCloser, *ObjectInfo, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	}
	if rng != "" {
		input.Range = aws.String(rng)
	}
	out, err := b.client.GetObject(ctx, input)
	if err != nil {
		if isNotFound(err) {
			return nil, nil, ErrNotFound
		}
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "InvalidRange" {
			return nil, nil, ErrInvalidRange
		}
		return nil, nil, fmt.Errorf("s3 get %s/%s: %w", b.bucket, key, err)
	}
	info := &ObjectInfo{
		Key:      key,
		Size:     aws.ToInt64(out.ContentLength),
		Metadata: out.Metadata,
	}
	if out.LastModified != nil {
		info.LastModified = *out.LastModified
	}
	if out.ContentType != nil {
		info.ContentType = *out.ContentType
	}
	if out.ETag != nil {
		info.ETag = *out.ETag
	}
	if out.ContentRange != nil {
		info.ContentRange = *out.ContentRange
	}
	return out.Body, info, nil
}

func (b *S3Backend) Head(ctx context.Context, key string) (*ObjectInfo, error) {
	out, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("s3 head %s/%s: %w", b.bucket, key, err)
	}
	info := &ObjectInfo{
		Key:      key,
		Size:     aws.ToInt64(out.ContentLength),
		Metadata: out.Metadata,
	}
	if out.LastModified != nil {
		info.LastModified = *out.LastModified
	}
	if out.ContentType != nil {
		info.ContentType = *out.ContentType
	}
	if out.ETag != nil {
		info.ETag = *out.ETag
	}
	return info, nil
}

func (b *S3Backend) Delete(ctx context.Context, key string) error {
	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("s3 delete %s/%s: %w", b.bucket, key, err)
	}
	return nil
}

func (b *S3Backend) CopyObject(ctx context.Context, srcKey, dstKey string, metadata map[string]string) (string, error) {
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(b.bucket),
		CopySource: aws.String(fmt.Sprintf("%s/%s", b.bucket, srcKey)),
		Key:        aws.String(dstKey),
	}
	if len(metadata) > 0 {
		input.Metadata = metadata
		input.MetadataDirective = types.MetadataDirectiveReplace
	}
	out, err := b.client.CopyObject(ctx, input)
	if err != nil {
		return "", fmt.Errorf("s3 copy %s/%s -> %s: %w", b.bucket, srcKey, dstKey, err)
	}
	etag := ""
	if out.CopyObjectResult != nil && out.CopyObjectResult.ETag != nil {
		etag = *out.CopyObjectResult.ETag
	}
	return etag, nil
}

func (b *S3Backend) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	var objects []ObjectInfo
	paginator := s3.NewListObjectsV2Paginator(b.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(b.bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3 list %s: %w", b.bucket, err)
		}
		for _, obj := range page.Contents {
			info := ObjectInfo{
				Key:  aws.ToString(obj.Key),
				Size: aws.ToInt64(obj.Size),
			}
			if obj.LastModified != nil {
				info.LastModified = *obj.LastModified
			}
			if obj.ETag != nil {
				info.ETag = *obj.ETag
			}
			objects = append(objects, info)
		}
	}
	return objects, nil
}

// ListObjectsHierarchical lists objects with delimiter for directory-like browsing
func (b *S3Backend) ListObjectsHierarchical(ctx context.Context, prefix, delimiter string) ([]ObjectInfo, error) {
	var objects []ObjectInfo

	paginator := s3.NewListObjectsV2Paginator(b.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(b.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String(delimiter),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3 list %s: %w", b.bucket, err)
		}

		// Add regular objects
		for _, obj := range page.Contents {
			info := ObjectInfo{
				Key:  aws.ToString(obj.Key),
				Size: aws.ToInt64(obj.Size),
			}
			if obj.LastModified != nil {
				info.LastModified = *obj.LastModified
			}
			if obj.ETag != nil {
				info.ETag = *obj.ETag
			}
			objects = append(objects, info)
		}

		// Add common prefixes (directories) as zero-size objects
		for _, commonPrefix := range page.CommonPrefixes {
			objects = append(objects, ObjectInfo{
				Key:  aws.ToString(commonPrefix.Prefix),
				Size: 0, // Directories have zero size
			})
		}
	}

	return objects, nil
}

// ListObjectsPaginatedFull lists all objects in pages, calling fn for each page.
// It enriches each object with metadata to ensure symlinks are correctly identified.
func (b *S3Backend) ListObjectsPaginatedFull(ctx context.Context, prefix string, pageSize int, fn func([]ObjectInfo) error) (int64, error) {
	var total int64

	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(b.bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(int32(pageSize)),
	}

	paginator := s3.NewListObjectsV2Paginator(b.client, input)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return total, fmt.Errorf("s3 list %s: %w", b.bucket, err)
		}

		var objects []ObjectInfo
		for _, obj := range page.Contents {
			info := ObjectInfo{
				Key:  aws.ToString(obj.Key),
				Size: aws.ToInt64(obj.Size),
			}
			if obj.LastModified != nil {
				info.LastModified = *obj.LastModified
			}
			if obj.ETag != nil {
				info.ETag = *obj.ETag
			}
			objects = append(objects, info)
		}

		// Enrich metadata in parallel (opt-in: one HEAD per object, see WithMetadata)
		if wantsMetadata(ctx) && len(objects) > 0 {
			const concurrency = 20
			semaphore := make(chan struct{}, concurrency)
			var wg sync.WaitGroup

			for i := range objects {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					semaphore <- struct{}{}
					defer func() { <-semaphore }()

					// Ensure we get metadata for all objects
					headInfo, headErr := b.Head(ctx, objects[idx].Key)
					if headErr == nil {
						objects[idx].Metadata = headInfo.Metadata
						objects[idx].ContentType = headInfo.ContentType
					}
				}(i)
			}
			wg.Wait()
		}

		total += int64(len(objects))

		if err := fn(objects); err != nil {
			return total, err
		}
	}

	return total, nil
}

func (b *S3Backend) ListObjectsV2(ctx context.Context, prefix, delimiter, continuationToken, startAfter string, maxKeys int32) (*ListObjectsV2Result, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(b.bucket),
		Prefix: aws.String(prefix),
	}
	if delimiter != "" {
		input.Delimiter = aws.String(delimiter)
	}
	if continuationToken != "" {
		input.ContinuationToken = aws.String(continuationToken)
	}
	if startAfter != "" {
		input.StartAfter = aws.String(startAfter)
	}
	if maxKeys > 0 {
		input.MaxKeys = aws.Int32(maxKeys)
	}

	output, err := b.client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("s3 list v2 %s: %w", b.bucket, err)
	}

	result := &ListObjectsV2Result{
		IsTruncated: aws.ToBool(output.IsTruncated),
		NextToken:   aws.ToString(output.NextContinuationToken),
	}

	for _, obj := range output.Contents {
		info := ObjectInfo{
			Key:  aws.ToString(obj.Key),
			Size: aws.ToInt64(obj.Size),
		}
		if obj.LastModified != nil {
			info.LastModified = *obj.LastModified
		}
		if obj.ETag != nil {
			info.ETag = *obj.ETag
		}
		result.Objects = append(result.Objects, info)
	}

	// Enrich metadata in parallel (opt-in: one HEAD per object, see WithMetadata)
	if wantsMetadata(ctx) && len(result.Objects) > 0 {
		const concurrency = 20
		semaphore := make(chan struct{}, concurrency)
		var wg sync.WaitGroup

		for i := range result.Objects {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				semaphore <- struct{}{}
				defer func() { <-semaphore }()

				// Ensure we get metadata for all objects, including directory markers
				headInfo, headErr := b.Head(ctx, result.Objects[idx].Key)
				if headErr == nil {
					result.Objects[idx].Metadata = headInfo.Metadata
					result.Objects[idx].ContentType = headInfo.ContentType
				}
			}(i)
		}
		wg.Wait()
	}

	for _, cp := range output.CommonPrefixes {
		result.CommonPrefixes = append(result.CommonPrefixes, aws.ToString(cp.Prefix))
	}

	return result, nil
}

var ErrNotFound = errors.New("object not found")

// IsNotFound reports whether err means "the object does not exist", whichever form the
// backend reports it in.
func IsNotFound(err error) bool { return errors.Is(err, ErrNotFound) || isNotFound(err) }

func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nsb *types.NotFound
	return errors.As(err, &nsb)
}

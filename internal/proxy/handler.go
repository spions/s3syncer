package proxy

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/spions/s3syncer/internal/metrics"
	"github.com/spions/s3syncer/internal/replicator"
	"github.com/spions/s3syncer/internal/storage"
)

// objectSubresources are query parameters that select an S3 object sub-resource
// (ACL, tagging, multipart upload, etc) instead of the object body. We don't
// support these yet, but a PUT with ?acl must not be silently written as the
// object body.
var objectSubresources = []string{
	"acl", "tagging", "torrent", "uploads", "uploadId", "partNumber",
	"versioning", "versionId", "restore", "select", "lifecycle", "policy",
	"website", "cors", "legal-hold", "retention", "object-lock",
}

func unsupportedObjectSubresource(q map[string][]string) string {
	for _, name := range objectSubresources {
		if _, ok := q[name]; ok {
			return name
		}
	}
	return ""
}

// handleObjectSubresourceGet returns minimal stub responses for object
// sub-resource reads so that clients which probe for ACL/tagging/etc don't see
// the actual object body.
func (h *Handler) handleObjectSubresourceGet(w http.ResponseWriter, r *http.Request, sub string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	switch sub {
	case "acl":
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID></ID></Owner><AccessControlList></AccessControlList></AccessControlPolicy>`)
	case "tagging":
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet></TagSet></Tagging>`)
	default:
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Response sub-resource=%q/>`, sub)
	}
}

// resolveContentType returns a meaningful Content-Type for the object, falling
// back to mime.TypeByExtension(ext) when the upstream value is missing or the
// generic application/octet-stream (which FUSE-based clients like GeeseFS/s3fs
// set for every file on upload).
func resolveContentType(stored, key string) string {
	if stored != "" && stored != "application/octet-stream" && stored != "binary/octet-stream" {
		return stored
	}
	if ext := path.Ext(key); ext != "" {
		if ct := mime.TypeByExtension(ext); ct != "" {
			return ct
		}
	}
	return stored
}

type Handler struct {
	replicators map[string]*replicator.Replicator
	log         *slog.Logger
	maxPutSize  int64
	bodyIdle    time.Duration
}

// SetBodyIdleTimeout aborts a request body that makes no progress for d (0 = no limit).
// It is an idle timeout, not a total one, so legitimately long uploads are unaffected.
func (h *Handler) SetBodyIdleTimeout(d time.Duration) { h.bodyIdle = d }

// idleDeadlineReader arms a read deadline before every Read, so a client that stops
// sending (slowloris) cannot hold a connection and the upload memory reserved for it.
type idleDeadlineReader struct {
	io.ReadCloser
	rc   *http.ResponseController
	idle time.Duration
}

func (r *idleDeadlineReader) Read(p []byte) (int, error) {
	_ = r.rc.SetReadDeadline(time.Now().Add(r.idle)) // not supported by every ResponseWriter; best effort
	return r.ReadCloser.Read(p)
}

// SetMaxPutSize limits the size of a single PUT body (0 = unlimited).
func (h *Handler) SetMaxPutSize(n int64) { h.maxPutSize = n }

func NewHandler(replicators map[string]*replicator.Replicator, log *slog.Logger) *Handler {
	return &Handler{replicators: replicators, log: log}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	path := strings.TrimPrefix(r.URL.Path, "/")

	h.log.Debug("request",
		"method", r.Method,
		"path", r.URL.Path,
		"query", r.URL.RawQuery,
		"remote", r.RemoteAddr,
		"user_agent", r.UserAgent(),
		"content_length", r.ContentLength,
		"host", r.Host,
	)

	// Parse path: could be "", "bucket", or "bucket/key..."
	parts := strings.SplitN(path, "/", 2)
	bucket := parts[0]

	// Root request: GET / — list buckets (S3 ListBuckets)
	if bucket == "" {
		h.handleListBuckets(w, r)
		h.logDone(r, start)
		return
	}

	rep, ok := h.replicators[bucket]

	// Bucket-level request: GET /bucket (no key) — list objects or bucket queries
	if len(parts) < 2 || parts[1] == "" {
		if !ok {
			h.errorResponse(w, r, http.StatusNotFound, fmt.Sprintf("bucket %q not found", bucket))
			return
		}
		h.handleBucketRequest(w, r, rep, bucket)
		h.logDone(r, start)
		return
	}

	key := parts[1]

	if !ok {
		h.errorResponse(w, r, http.StatusNotFound, fmt.Sprintf("bucket %q not found", bucket))
		return
	}

	// Object sub-resources (?acl, ?tagging, ?uploads, ?uploadId, ?versioning...) are
	// not supported yet. Without this guard a PUT /bucket/key?acl would write the
	// ACL XML payload as the object body.
	if sub := unsupportedObjectSubresource(r.URL.Query()); sub != "" {
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			h.handleObjectSubresourceGet(w, r, sub)
		} else {
			h.errorResponse(w, r, http.StatusNotImplemented, fmt.Sprintf("sub-resource %q not supported", sub))
		}
		h.logDone(r, start)
		return
	}

	switch r.Method {
	case http.MethodPut:
		h.handlePut(w, r, rep, bucket, key)
	case http.MethodGet:
		h.handleGet(w, r, rep, key)
	case http.MethodHead:
		h.handleHead(w, r, rep, key)
	case http.MethodDelete:
		h.handleDelete(w, r, rep, key)
	default:
		h.errorResponse(w, r, http.StatusMethodNotAllowed, fmt.Sprintf("method %s not allowed", r.Method))
	}

	h.logDone(r, start)
}

func (h *Handler) logDone(r *http.Request, start time.Time) {
	h.log.Debug("request_done",
		"method", r.Method,
		"path", r.URL.Path,
		"duration_ms", time.Since(start).Milliseconds(),
	)
}

// handleListBuckets responds to GET / with an S3-compatible ListBuckets XML.
func (h *Handler) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.errorResponse(w, r, http.StatusMethodNotAllowed, "only GET allowed on /")
		return
	}

	type Bucket struct {
		Name         string `xml:"Name"`
		CreationDate string `xml:"CreationDate"`
	}
	type ListAllMyBucketsResult struct {
		XMLName xml.Name `xml:"ListAllMyBucketsResult"`
		Xmlns   string   `xml:"xmlns,attr"`
		Buckets struct {
			Bucket []Bucket `xml:"Bucket"`
		} `xml:"Buckets"`
	}

	result := ListAllMyBucketsResult{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/"}
	owner, _ := r.Context().Value(ownerKey{}).(string)
	for name := range h.replicators {
		if owner != "" && name != owner {
			continue // a key only sees its own bucket
		}
		result.Buckets.Bucket = append(result.Buckets.Bucket, Bucket{
			Name:         name,
			CreationDate: "2025-01-01T00:00:00.000Z",
		})
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(result)
}

// handleBucketRequest handles bucket-level S3 requests (list objects, location, etc).
func (h *Handler) handleBucketRequest(w http.ResponseWriter, r *http.Request, rep *replicator.Replicator, bucket string) {
	// GET /bucket?uploads — ListMultipartUploads (stub)
	if r.URL.Query().Has("uploads") {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>%s</Bucket><KeyMarker></KeyMarker><UploadIdMarker></UploadIdMarker><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>`, bucket)
		return
	}

	// GET /bucket?location — GetBucketLocation
	if r.URL.Query().Has("location") {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`)
		return
	}

	switch r.Method {
	case http.MethodGet:
		// ListObjects
		h.handleListObjects(w, r, rep, bucket)
	case http.MethodHead:
		// HeadBucket — just return 200 if bucket exists
		w.WriteHeader(http.StatusOK)
	default:
		h.errorResponse(w, r, http.StatusMethodNotAllowed, fmt.Sprintf("method %s not supported on bucket", r.Method))
	}
}

func (h *Handler) handleListObjects(w http.ResponseWriter, r *http.Request, rep *replicator.Replicator, bucket string) {
	prefix := r.URL.Query().Get("prefix")
	delimiter := r.URL.Query().Get("delimiter")
	maxKeysStr := r.URL.Query().Get("max-keys")
	continuationToken := r.URL.Query().Get("continuation-token")

	// Support pagination: start-after (v2/ext-v1) or marker (v1)
	startAfter := r.URL.Query().Get("start-after")
	if startAfter == "" {
		startAfter = r.URL.Query().Get("marker")
	}

	// Parse max-keys parameter (default 1000 per S3 spec)
	maxKeys := 1000
	if maxKeysStr != "" {
		if parsed, err := strconv.Atoi(maxKeysStr); err == nil && parsed > 0 {
			maxKeys = parsed
		}
	}

	// Optimization: For requests without delimiter, automatically use hierarchical listing
	// to avoid full bucket scans on large buckets (645K+ objects).
	// This matches standard S3 behavior and prevents timeouts.
	// Only use flat listing if delimiter is explicitly set to empty with "delimiter=" query param.
	autoDelimiter := false
	if delimiter == "" && !r.URL.Query().Has("delimiter") {
		// GeeseFS often expects flat listing for better directory traversal or has issues with auto-delimiter
		if !isGeeseFS(r) {
			delimiter = "/"
			autoDelimiter = true
			h.log.Debug("auto-enabling delimiter for performance", "user_agent", r.UserAgent())
		}
	}

	// For GeeseFS clients, enrich listing with object metadata so symlinks
	// are recognised without a separate HEAD request after remount. Everyone else gets a
	// plain listing: enrichment costs one backend HEAD per listed object.
	enrichMeta := isGeeseFS(r) && (delimiter == "" || autoDelimiter || delimiter == "/")
	listCtx := r.Context()
	if enrichMeta {
		listCtx = storage.WithMetadata(listCtx)
	}

	// Fetch only what we need for the current page
	result, err := rep.ListObjectsV2(listCtx, prefix, delimiter, continuationToken, startAfter, int32(maxKeys))
	if err != nil {
		h.log.Error("list objects failed", "bucket", bucket, "prefix", prefix, "delimiter", delimiter, "error", err)
		h.errorResponse(w, r, http.StatusInternalServerError, err.Error())
		return
	}

	// MetadataEntry matches the Yandex Cloud S3 ext-v1 ListObjects format
	// that GeeseFS uses to get user metadata inline with listing results.
	type MetadataEntry struct {
		XMLName xml.Name `xml:"Metadata"`
		Name    string   `xml:"Name"`
		Value   string   `xml:"Value"`
	}
	type Content struct {
		Key          string          `xml:"Key"`
		Size         int64           `xml:"Size"`
		LastModified string          `xml:"LastModified"`
		ETag         string          `xml:"ETag"`
		StorageClass string          `xml:"StorageClass"`
		Metadata     []MetadataEntry `xml:",omitempty"`
	}
	type CommonPrefix struct {
		Prefix   string          `xml:"Prefix"`
		Metadata []MetadataEntry `xml:",omitempty"`
	}

	var contents []Content
	var commonPrefixes []CommonPrefix

	// Process CommonPrefixes
	for _, p := range result.CommonPrefixes {
		cp := CommonPrefix{Prefix: p}
		// If we are GeeseFS and this is a directory, we might want to check its metadata
		// for directory-specific attributes if GeeseFS uses them in listing.
		// However, S3 CommonPrefixes are not objects and don't have metadata themselves.
		// If there's an object with the same name as the prefix (e.g. "dir/"),
		// it will be in the Objects list if it exists.
		commonPrefixes = append(commonPrefixes, cp)
	}

	// Process Objects and enrich metadata if needed
	if enrichMeta && len(result.Objects) > 0 {
		for _, obj := range result.Objects {
			lastModified := obj.LastModified.UTC().Format(time.RFC3339)
			if obj.LastModified.IsZero() {
				lastModified = time.Now().UTC().Format(time.RFC3339)
			}

			// Format metadata for XML
			var meta []MetadataEntry
			if len(obj.Metadata) > 0 {
				for k, v := range obj.Metadata {
					meta = append(meta, MetadataEntry{Name: k, Value: v})
				}
			}

			contents = append(contents, Content{
				Key:          obj.Key,
				Size:         obj.Size,
				LastModified: lastModified,
				ETag:         obj.ETag,
				StorageClass: "STANDARD",
				Metadata:     meta,
			})
		}
	} else {
		for _, obj := range result.Objects {
			lastModified := obj.LastModified.UTC().Format(time.RFC3339)
			if obj.LastModified.IsZero() {
				lastModified = time.Now().UTC().Format(time.RFC3339)
			}
			contents = append(contents, Content{
				Key:          obj.Key,
				Size:         obj.Size,
				LastModified: lastModified,
				ETag:         obj.ETag,
				StorageClass: "STANDARD",
			})
		}
	}

	// Build XML result
	type ListBucketResult struct {
		XMLName               xml.Name       `xml:"ListBucketResult"`
		Xmlns                 string         `xml:"xmlns,attr"`
		Name                  string         `xml:"Name"`
		Prefix                string         `xml:"Prefix"`
		Delimiter             string         `xml:"Delimiter,omitempty"`
		MaxKeys               int            `xml:"MaxKeys"`
		KeyCount              int            `xml:"KeyCount,omitempty"`
		IsTruncated           bool           `xml:"IsTruncated"`
		NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
		Contents              []Content      `xml:"Contents"`
		CommonPrefixes        []CommonPrefix `xml:"CommonPrefixes,omitempty"`
	}
	resBody := ListBucketResult{
		Xmlns:                 "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:                  bucket,
		Prefix:                prefix,
		Delimiter:             delimiter,
		MaxKeys:               maxKeys,
		IsTruncated:           result.IsTruncated,
		NextContinuationToken: result.NextToken,
		Contents:              contents,
		CommonPrefixes:        commonPrefixes,
	}
	if r.URL.Query().Get("list-type") == "2" {
		resBody.KeyCount = len(contents) + len(commonPrefixes)
	}

	xmlData, _ := xml.Marshal(resBody)
	body := xml.Header + string(xmlData)

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, body)
}

func (h *Handler) handlePut(w http.ResponseWriter, r *http.Request, rep *replicator.Replicator, bucket, key string) {
	defer r.Body.Close()

	if copySource := r.Header.Get("x-amz-copy-source"); copySource != "" {
		h.handleCopy(w, r, rep, bucket, key, copySource)
		return
	}

	contentType := r.Header.Get("Content-Type")
	size := r.ContentLength
	metadata := parseAmzMetadata(r)

	if h.bodyIdle > 0 {
		r.Body = &idleDeadlineReader{ReadCloser: r.Body, rc: http.NewResponseController(w), idle: h.bodyIdle}
	}

	if h.maxPutSize > 0 {
		if size > h.maxPutSize {
			h.errorResponse(w, r, http.StatusRequestEntityTooLarge, "EntityTooLarge: request body exceeds max_put_size_bytes")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, h.maxPutSize)
	}

	etag, err := rep.Put(r.Context(), key, r.Body, size, contentType, metadata)
	if err != nil {
		h.writeFailure(w, r, "put", key, err)
		return
	}
	if etag != "" {
		w.Header().Set("ETag", etag)
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleCopy(w http.ResponseWriter, r *http.Request, rep *replicator.Replicator, bucket, key string, copySource string) {
	// x-amz-copy-source is URL-encoded: "/bucket/my%20key" or "bucket/my%20key[?versionId=..]"
	srcBucket, srcKey, versioned, ok := parseCopySource(copySource)
	if !ok {
		xmlError(w, http.StatusBadRequest, "InvalidArgument", "invalid x-amz-copy-source")
		return
	}
	if versioned {
		xmlError(w, http.StatusNotImplemented, "NotImplemented", "copying a specific object version is not supported")
		return
	}
	if srcBucket != bucket {
		xmlError(w, http.StatusNotImplemented, "NotImplemented", "copying between buckets is not supported")
		return
	}

	h.log.Debug("copy request", "srcBucket", srcBucket, "srcKey", srcKey, "dstKey", key)

	// x-amz-metadata-directive: COPY keeps the source metadata, REPLACE takes the request's.
	// Without the header, metadata sent with the request is applied (what existing clients rely on).
	var metadata map[string]string
	if !strings.EqualFold(r.Header.Get("x-amz-metadata-directive"), "COPY") {
		metadata = parseAmzMetadata(r)
	}
	etag, err := rep.CopyObject(r.Context(), srcKey, key, metadata)
	if err != nil {
		h.writeFailure(w, r, "copy", key, err)
		return
	}

	// S3 CopyObject returns an XML body with ETag and LastModified
	type CopyObjectResult struct {
		XMLName      xml.Name `xml:"CopyObjectResult"`
		LastModified string   `xml:"LastModified"`
		ETag         string   `xml:"ETag"`
	}

	res := CopyObjectResult{
		LastModified: time.Now().UTC().Format(time.RFC3339Nano),
		ETag:         etag,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(res)
}

// parseAmzMetadata extracts X-Amz-Meta-* headers into a map (without prefix).
func parseAmzMetadata(r *http.Request) map[string]string {
	meta := make(map[string]string)
	for k, v := range r.Header {
		lower := strings.ToLower(k)
		if strings.HasPrefix(lower, "x-amz-meta-") && len(v) > 0 {
			// Extract the key after the prefix
			// AWS SDK v2 lowercases metadata keys and adds "--" prefix for keys containing dashes
			metaKey := lower[len("x-amz-meta-"):]
			// GeeseFS already sends keys with "--" prefix, so don't add it again
			// Only add "--" prefix if the key contains dashes and doesn't already have the prefix
			if strings.Contains(metaKey, "-") && !strings.HasPrefix(metaKey, "--") {
				metaKey = "--" + metaKey
			}
			meta[metaKey] = v[0]
		}
	}
	if len(meta) == 0 {
		return nil
	}
	return meta
}

func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request, rep *replicator.Replicator, key string) {
	body, info, err := rep.GetRange(r.Context(), key, singleByteRange(r.Header.Get("Range")))
	if err != nil {
		switch {
		case errors.Is(err, storage.ErrNotFound):
			h.errorResponse(w, r, http.StatusNotFound, "not found")
		case errors.Is(err, storage.ErrInvalidRange):
			xmlError(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "The requested range is not satisfiable")
		default:
			h.log.Error("get failed", "key", key, "error", err)
			h.errorResponse(w, r, http.StatusInternalServerError, "internal error")
		}
		return
	}

	// Resolve symlink for non-GeeseFS clients (may replace body)
	resolvedBody, resolvedInfo, err := resolveSymlink(r.Context(), r, rep, body, info, h.log)
	if err != nil {
		if err == storage.ErrNotFound {
			h.errorResponse(w, r, http.StatusNotFound, "symlink target not found")
			return
		}
		h.log.Error("symlink resolution failed", "key", key, "error", err)
		h.errorResponse(w, r, http.StatusInternalServerError, err.Error())
		return
	}
	defer resolvedBody.Close()

	if status := checkConditions(r, resolvedInfo.ETag, effectiveLastModified(resolvedInfo)); status != 0 {
		h.writeNotSatisfied(w, status, resolvedInfo)
		return
	}

	w.Header().Set("Accept-Ranges", "bytes")
	if ct := resolveContentType(resolvedInfo.ContentType, resolvedInfo.Key); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	if resolvedInfo.Size > 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(resolvedInfo.Size, 10))
	}
	if resolvedInfo.ETag != "" {
		w.Header().Set("ETag", resolvedInfo.ETag)
	}
	if lm := effectiveLastModified(resolvedInfo); !lm.IsZero() {
		w.Header().Set("Last-Modified", lm.UTC().Format(http.TimeFormat))
	}
	// Return user metadata as X-Amz-Meta-* headers
	filteredMeta := filterMetadataForClient(r, resolvedInfo.Metadata)
	for k, v := range filteredMeta {
		// Remove "--" prefix that AWS SDK v2 adds for keys with dashes
		metaKey := k
		if strings.HasPrefix(k, "--") {
			metaKey = k[2:]
		}
		w.Header().Set("X-Amz-Meta-"+metaKey, v)
	}
	if resolvedInfo.ContentRange != "" {
		w.Header().Set("Content-Range", resolvedInfo.ContentRange)
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	io.Copy(w, resolvedBody)
}

// writeNotSatisfied answers a failed precondition: 304 carries the validators, 412 an S3 error.
func (h *Handler) writeNotSatisfied(w http.ResponseWriter, status int, info *storage.ObjectInfo) {
	if status == http.StatusNotModified {
		if info.ETag != "" {
			w.Header().Set("ETag", info.ETag)
		}
		if lm := effectiveLastModified(info); !lm.IsZero() {
			w.Header().Set("Last-Modified", lm.UTC().Format(http.TimeFormat))
		}
		w.WriteHeader(http.StatusNotModified)
		return
	}
	xmlError(w, http.StatusPreconditionFailed, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold")
}

func (h *Handler) handleHead(w http.ResponseWriter, r *http.Request, rep *replicator.Replicator, key string) {
	info, err := rep.Head(r.Context(), key)
	if err != nil {
		if err == storage.ErrNotFound {
			h.errorResponse(w, r, http.StatusNotFound, "not found")
			return
		}
		h.log.Error("head failed", "key", key, "error", err)
		h.errorResponse(w, r, http.StatusInternalServerError, err.Error())
		return
	}

	// Resolve symlink for non-GeeseFS clients
	resolvedInfo, err := resolveSymlinkHead(r.Context(), r, rep, info, h.log)
	if err != nil {
		if err == storage.ErrNotFound {
			h.errorResponse(w, r, http.StatusNotFound, "symlink target not found")
			return
		}
		h.log.Error("symlink resolution failed", "key", key, "error", err)
		h.errorResponse(w, r, http.StatusInternalServerError, err.Error())
		return
	}

	if ct := resolveContentType(resolvedInfo.ContentType, resolvedInfo.Key); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	if status := checkConditions(r, resolvedInfo.ETag, effectiveLastModified(resolvedInfo)); status != 0 {
		h.writeNotSatisfied(w, status, resolvedInfo)
		return
	}

	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.FormatInt(resolvedInfo.Size, 10))
	if resolvedInfo.ETag != "" {
		w.Header().Set("ETag", resolvedInfo.ETag)
	}
	if lm := effectiveLastModified(resolvedInfo); !lm.IsZero() {
		w.Header().Set("Last-Modified", lm.UTC().Format(http.TimeFormat))
	}
	// Return user metadata as X-Amz-Meta-* headers
	filteredMeta := filterMetadataForClient(r, resolvedInfo.Metadata)
	for k, v := range filteredMeta {
		// For GeeseFS clients, keep the metadata key as-is (including -- prefix for symlinks)
		// For non-GeeseFS clients, remove the -- prefix that AWS SDK v2 adds
		metaKey := k
		if !isGeeseFS(r) && strings.HasPrefix(k, "--") {
			metaKey = k[2:]
		}
		w.Header().Set("X-Amz-Meta-"+metaKey, v)
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request, rep *replicator.Replicator, key string) {
	if err := rep.Delete(r.Context(), key); err != nil {
		h.writeFailure(w, r, "delete", key, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// parseCopySource splits an x-amz-copy-source header into bucket and key (both decoded).
func parseCopySource(v string) (bucket, key string, versioned, ok bool) {
	raw, query, _ := strings.Cut(strings.TrimPrefix(v, "/"), "?")
	versioned = strings.Contains(query, "versionId=")
	decoded, err := url.PathUnescape(raw)
	if err != nil {
		return "", "", false, false
	}
	bucket, key, found := strings.Cut(decoded, "/")
	if !found || bucket == "" || key == "" {
		return "", "", false, false
	}
	return bucket, key, versioned, true
}

// writeFailure answers a failed write. Retryable conditions get 503 + Retry-After, a damaged
// upload body gets 4xx; everything else is a generic 500: the details (backend names,
// endpoints) go to the log, not to the client.
func (h *Handler) writeFailure(w http.ResponseWriter, r *http.Request, op, key string, err error) {
	switch {
	case errors.Is(err, errChunkSignature):
		xmlError(w, http.StatusForbidden, "SignatureDoesNotMatch", "The chunk signature does not match")
	case errors.Is(err, errBadChunk):
		xmlError(w, http.StatusBadRequest, "IncompleteBody", "The request body is incomplete or malformed")
	case errors.Is(err, errPayloadHash):
		xmlError(w, http.StatusBadRequest, "XAmzContentSHA256Mismatch", "The provided x-amz-content-sha256 does not match what was computed")
	case errors.Is(err, replicator.ErrQueueUnavailable), errors.Is(err, replicator.ErrObjectBusy), errors.Is(err, replicator.ErrOverloaded):
		reason := "queue_unavailable"
		switch {
		case errors.Is(err, replicator.ErrObjectBusy):
			reason = "object_busy"
		case errors.Is(err, replicator.ErrOverloaded):
			reason = "upload_memory" // counted where the wait expired
		}
		if reason != "upload_memory" {
			metrics.OverloadRejections.WithLabelValues(reason).Inc()
		}
		h.log.Warn(op+" rejected, client should retry", "key", key, "error", err)
		w.Header().Set("Retry-After", "5")
		xmlError(w, http.StatusServiceUnavailable, "ServiceUnavailable", "The service is temporarily unable to handle the request, retry")
	default:
		h.log.Error(op+" failed", "key", key, "error", err)
		h.errorResponse(w, r, http.StatusInternalServerError, "internal error")
	}
}

func (h *Handler) errorResponse(w http.ResponseWriter, r *http.Request, code int, msg string) {
	h.log.Warn("error_response",
		"status", code,
		"error", msg,
		"method", r.Method,
		"path", r.URL.Path,
		"query", r.URL.RawQuery,
		"remote", r.RemoteAddr,
		"user_agent", r.UserAgent(),
	)
	metrics.HTTPErrors.WithLabelValues(strconv.Itoa(code)).Inc()

	// Return S3-compatible XML error (clients such as mc and the AWS SDKs
	// match on the textual <Code>, e.g. NoSuchBucket)
	if code >= 500 {
		msg = "internal error" // backend names, endpoints and driver messages stay in the log
	}
	xmlError(w, code, s3ErrorCode(code, msg), msg)
}

// s3ErrorCode maps an HTTP status (and the message, to tell a missing bucket
// from a missing key) to the matching S3 error code.
func s3ErrorCode(status int, msg string) string {
	switch status {
	case http.StatusNotFound:
		if strings.HasPrefix(msg, "bucket ") {
			return "NoSuchBucket"
		}
		return "NoSuchKey"
	case http.StatusForbidden:
		return "AccessDenied"
	case http.StatusBadRequest:
		return "InvalidRequest"
	case http.StatusMethodNotAllowed:
		return "MethodNotAllowed"
	case http.StatusRequestEntityTooLarge:
		return "EntityTooLarge"
	case http.StatusNotImplemented:
		return "NotImplemented"
	default:
		return "InternalError"
	}
}

// effectiveLastModified returns the object's true modification time.
// If the object carries a "mtime" user-metadata key (Unix timestamp in seconds),
// that value takes precedence over S3's own LastModified (which reflects upload time).
func effectiveLastModified(info *storage.ObjectInfo) time.Time {
	if info == nil {
		return time.Time{}
	}
	if raw, ok := info.Metadata["mtime"]; ok {
		if sec, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return time.Unix(sec, 0)
		}
	}
	return info.LastModified
}

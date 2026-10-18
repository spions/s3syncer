package proxy

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spions/s3syncer/internal/config"
)

// ownerKey is the context key under which AuthMiddleware stores the virtual
// bucket an authenticated request belongs to.
type ownerKey struct{}

// Credentials maps access_key_id -> VirtualBucket config for auth lookup.
type Credentials map[string]config.VirtualBucket

func BuildCredentials(buckets []config.VirtualBucket) Credentials {
	creds := make(Credentials)
	for _, vb := range buckets {
		creds[vb.AccessKeyID] = vb
	}
	return creds
}

// PublicReadBuckets returns the set of virtual bucket names that allow
// unauthenticated GET/HEAD of objects (analog of s3:GetObject Allow for *).
func PublicReadBuckets(buckets []config.VirtualBucket) map[string]struct{} {
	public := make(map[string]struct{})
	for _, vb := range buckets {
		if vb.PublicRead {
			public[vb.Name] = struct{}{}
		}
	}
	return public
}

// isPublicObjectRead reports whether the request is an anonymous-allowed
// object read (GET/HEAD of /{bucket}/{key}) for a bucket marked public_read.
// Bucket-level operations (ListObjects, ListBuckets, bucket queries) still
// require authentication.
func isPublicObjectRead(r *http.Request, public map[string]struct{}) bool {
	if len(public) == 0 {
		return false
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}
	path := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, ok := strings.Cut(path, "/")
	if !ok || bucket == "" || key == "" {
		return false
	}
	_, isPublic := public[bucket]
	return isPublic
}

// AuthMiddleware verifies AWS Signature V2 or V4 on every request, except
// for anonymous object reads on virtual buckets configured with public_read.
func AuthMiddleware(creds Credentials, public map[string]struct{}, slogger *slog.Logger, next http.Handler) http.Handler {
	log := &authLogger{Logger: slogger}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isPublicObjectRead(r, public) {
			log.Debug("auth_bypass_public_read",
				"method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr)
			next.ServeHTTP(w, r)
			return
		}

		authHeader := r.Header.Get("Authorization")
		presigned := authHeader == "" && r.URL.Query().Get("X-Amz-Algorithm") != ""
		if authHeader == "" && !presigned {
			log.warn("reason", "missing Authorization header",
				"method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr)
			xmlError(w, http.StatusForbidden, "AccessDenied", "Missing Authorization header")
			return
		}

		var ok bool
		var owner string // the virtual bucket this access key belongs to
		var sess *v4Session
		switch {
		case presigned || strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256 "):
			if sess = verifyV4(r, creds, log); sess != nil {
				ok = true
				owner = sess.bucket
			}
		case strings.HasPrefix(authHeader, "AWS "):
			owner = verifyV2(r, authHeader, creds, log)
			ok = owner != ""
		default:
			log.warn("reason", "unsupported auth scheme", "remote", r.RemoteAddr)
			xmlError(w, http.StatusForbidden, "AccessDenied", "Unsupported authorization scheme")
			return
		}

		if !ok {
			xmlError(w, http.StatusForbidden, "SignatureDoesNotMatch", "The request signature does not match")
			return
		}

		// A key is valid for its own virtual bucket only. The first path segment is
		// the bucket (empty for ListBuckets, which is filtered to the owner's bucket).
		if bucket, _, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/"); bucket != "" && bucket != owner {
			log.warn("reason", "access key not allowed for this bucket",
				"owner", owner, "bucket", bucket, "method", r.Method, "remote", r.RemoteAddr)
			xmlError(w, http.StatusForbidden, "AccessDenied", "Access Denied")
			return
		}

		if sess != nil {
			if !verifyPayloadHash(r, sess) {
				xmlError(w, http.StatusBadRequest, "XAmzContentSHA256Mismatch", "The provided x-amz-content-sha256 does not match what was computed")
				return
			}
			decodeStreamingBody(r, sess)
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ownerKey{}, owner)))
	})
}

// maxClockSkew is the allowed difference between the request timestamp and
// the server time (same window as AWS S3). It limits replay of captured requests.
const maxClockSkew = 15 * time.Minute

// withinClockSkew reports whether the given request header holds a timestamp
// in the given layout that is within maxClockSkew of the current time.
func withinClockSkew(r *http.Request, header string, layouts ...string) bool {
	v := r.Header.Get(header)
	if v == "" {
		return false
	}
	var t time.Time
	var err error
	for _, layout := range layouts {
		if t, err = time.Parse(layout, v); err == nil {
			break
		}
	}
	if err != nil {
		return false
	}
	d := time.Since(t)
	if d < 0 {
		d = -d
	}
	return d <= maxClockSkew
}

// ===================== AWS Signature V2 =====================

// verifyV2 returns the virtual bucket the access key belongs to, or "" if the request is not authentic.
func verifyV2(r *http.Request, authHeader string, creds Credentials, log *authLogger) string {
	// Format: "AWS AccessKeyId:Signature"
	body := strings.TrimPrefix(authHeader, "AWS ")
	parts := strings.SplitN(body, ":", 2)
	if len(parts) != 2 {
		log.warn("reason", "malformed V2 auth header", "remote", r.RemoteAddr)
		return ""
	}

	accessKey := parts[0]
	clientSig := parts[1]

	vb, ok := creds[accessKey]
	if !ok {
		log.warn("reason", "unknown access key",
			"access_key", accessKey, "remote", r.RemoteAddr)
		return ""
	}

	if !withinClockSkew(r, "Date", http.TimeFormat, time.RFC1123Z) && !withinClockSkew(r, "X-Amz-Date", http.TimeFormat, time.RFC1123Z) {
		log.warn("reason", "request time outside allowed skew or missing",
			"access_key", accessKey, "remote", r.RemoteAddr)
		return ""
	}

	expectedSig := computeSignatureV2(r, vb.SecretAccessKey)

	if !hmac.Equal([]byte(clientSig), []byte(expectedSig)) {
		log.warn(
			"reason", "V2 signature mismatch",
			"access_key", accessKey,
			"method", r.Method,
			"path", r.URL.Path,
			"query", r.URL.RawQuery,
			"remote", r.RemoteAddr,
			"user_agent", r.UserAgent(),
		)
		return ""
	}

	log.Debug("auth_ok", "version", "v2", "access_key", accessKey, "path", r.URL.Path)
	return vb.Name
}

func computeSignatureV2(r *http.Request, secretKey string) string {
	// StringToSign = HTTP-Verb + "\n" +
	//                Content-MD5 + "\n" +
	//                Content-Type + "\n" +
	//                Date + "\n" +
	//                CanonicalizedAmzHeaders +
	//                CanonicalizedResource

	date := r.Header.Get("Date")
	// If x-amz-date is present, use empty string for Date in signature
	if r.Header.Get("X-Amz-Date") != "" {
		date = ""
	}

	stringToSign := strings.Join([]string{
		r.Method,
		r.Header.Get("Content-Md5"),
		r.Header.Get("Content-Type"),
		date,
	}, "\n") + "\n"

	// Canonicalized AMZ headers
	stringToSign += canonicalizedAmzHeaders(r)

	// Canonicalized resource
	stringToSign += canonicalizedResource(r)

	mac := hmac.New(sha1.New, []byte(secretKey))
	mac.Write([]byte(stringToSign))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func canonicalizedAmzHeaders(r *http.Request) string {
	headers := make(map[string]string)
	for k, v := range r.Header {
		lower := strings.ToLower(k)
		if strings.HasPrefix(lower, "x-amz-") {
			headers[lower] = strings.Join(v, ",")
		}
	}
	if len(headers) == 0 {
		return ""
	}

	keys := make([]string, 0, len(headers))
	for k := range headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buf strings.Builder
	for _, k := range keys {
		buf.WriteString(k)
		buf.WriteString(":")
		buf.WriteString(strings.TrimSpace(headers[k]))
		buf.WriteString("\n")
	}
	return buf.String()
}

func canonicalizedResource(r *http.Request) string {
	path := r.URL.Path
	if path == "" {
		path = "/"
	}

	// S3 sub-resources that must be included in the signature
	subResources := []string{
		"acl", "cors", "delete", "lifecycle", "location", "logging",
		"notification", "partNumber", "policy", "requestPayment",
		"response-cache-control", "response-content-disposition",
		"response-content-encoding", "response-content-language",
		"response-content-type", "response-expires",
		"torrent", "uploadId", "uploads", "versionId", "versioning",
		"versions", "website",
	}

	query := r.URL.Query()
	var params []string
	for _, sub := range subResources {
		if val, ok := query[sub]; ok {
			if val[0] != "" {
				params = append(params, sub+"="+val[0])
			} else {
				params = append(params, sub)
			}
		}
	}

	if len(params) > 0 {
		return path + "?" + strings.Join(params, "&")
	}
	return path
}

// ===================== AWS Signature V4 =====================

// v4Request is everything the SigV4 check needs, whether the signature came in the
// Authorization header or in the query string (presigned URL).
type v4Request struct {
	accessKey, dateStamp, region, service string
	signedHeaders                         string
	clientSig                             string
	amzDate                               string
	payloadHash                           string     // as it appears in the canonical request
	query                                 url.Values // parameters that are part of the canonical request
}

// maxPresignExpiry is the longest validity AWS allows for a presigned URL.
const maxPresignExpiry = 7 * 24 * time.Hour

// parseV4Header reads the Authorization header form.
func parseV4Header(r *http.Request, authHeader string) (v4Request, bool) {
	params := parseAuthHeader(authHeader)
	credParts := strings.Split(params["Credential"], "/")
	if len(credParts) < 5 {
		return v4Request{}, false
	}
	payload := r.Header.Get("X-Amz-Content-Sha256")
	if payload == "" {
		payload = "UNSIGNED-PAYLOAD"
	}
	return v4Request{
		accessKey: credParts[0], dateStamp: credParts[1], region: credParts[2], service: credParts[3],
		signedHeaders: params["SignedHeaders"], clientSig: params["Signature"],
		amzDate: r.Header.Get("X-Amz-Date"), payloadHash: payload, query: r.URL.Query(),
	}, true
}

// parseV4Query reads the presigned-URL form (X-Amz-* parameters in the query string).
func parseV4Query(r *http.Request) (v4Request, bool) {
	q := r.URL.Query()
	if q.Get("X-Amz-Algorithm") != "AWS4-HMAC-SHA256" {
		return v4Request{}, false
	}
	credParts := strings.Split(q.Get("X-Amz-Credential"), "/")
	if len(credParts) < 5 || q.Get("X-Amz-Signature") == "" {
		return v4Request{}, false
	}
	signed := url.Values{}
	for k, v := range q {
		if k != "X-Amz-Signature" {
			signed[k] = v
		}
	}
	payload := q.Get("X-Amz-Content-Sha256")
	if payload == "" {
		payload = "UNSIGNED-PAYLOAD"
	}
	return v4Request{
		accessKey: credParts[0], dateStamp: credParts[1], region: credParts[2], service: credParts[3],
		signedHeaders: q.Get("X-Amz-SignedHeaders"), clientSig: q.Get("X-Amz-Signature"),
		amzDate: q.Get("X-Amz-Date"), payloadHash: payload, query: signed,
	}, true
}

// presignedValid checks the validity window of a presigned URL.
func presignedValid(r *http.Request, in v4Request) bool {
	signedAt, err := time.Parse("20060102T150405Z", in.amzDate)
	if err != nil {
		return false
	}
	secs, err := strconv.Atoi(r.URL.Query().Get("X-Amz-Expires"))
	if err != nil || secs < 1 || time.Duration(secs)*time.Second > maxPresignExpiry {
		return false
	}
	now := time.Now()
	return !now.Before(signedAt.Add(-maxClockSkew)) && now.Before(signedAt.Add(time.Duration(secs)*time.Second))
}

// verifyV4 checks a SigV4 request (Authorization header or presigned URL). On success it
// returns the signing session, needed to verify the chunk signatures of aws-chunked
// bodies; on failure nil.
func verifyV4(r *http.Request, creds Credentials, log *authLogger) *v4Session {
	var in v4Request
	var ok, presigned bool
	if authHeader := r.Header.Get("Authorization"); authHeader != "" {
		in, ok = parseV4Header(r, authHeader)
	} else {
		in, ok = parseV4Query(r)
		presigned = true
	}
	if !ok {
		log.warn("reason", "malformed V4 credentials", "remote", r.RemoteAddr)
		return nil
	}

	vb, found := creds[in.accessKey]
	if !found {
		log.warn("reason", "unknown access key", "access_key", in.accessKey, "remote", r.RemoteAddr)
		return nil
	}

	if presigned {
		if !presignedValid(r, in) {
			log.warn("reason", "presigned URL expired or not yet valid", "access_key", in.accessKey, "remote", r.RemoteAddr)
			return nil
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodPut {
			log.warn("reason", "presigned URL used with an unsupported method", "method", r.Method, "remote", r.RemoteAddr)
			return nil
		}
	} else if !withinClockSkew(r, "X-Amz-Date", "20060102T150405Z") {
		log.warn("reason", "request time outside allowed skew or missing", "access_key", in.accessKey, "remote", r.RemoteAddr)
		return nil
	}

	expectedSig := computeSignatureV4(r, vb.SecretAccessKey, in)
	if !hmac.Equal([]byte(in.clientSig), []byte(expectedSig)) {
		log.warn(
			"reason", "V4 signature mismatch",
			"access_key", in.accessKey,
			"method", r.Method,
			"path", r.URL.Path,
			"remote", r.RemoteAddr,
			"user_agent", r.UserAgent(),
			"signed_headers", in.signedHeaders,
		)
		return nil
	}

	log.Debug("auth_ok", "version", "v4", "access_key", in.accessKey, "path", r.URL.Path, "presigned", presigned)
	return &v4Session{
		signingKey: deriveSigningKey(vb.SecretAccessKey, in.dateStamp, in.region, in.service),
		amzDate:    in.amzDate,
		scope:      in.dateStamp + "/" + in.region + "/" + in.service + "/aws4_request",
		seedSig:    in.clientSig,
		bucket:     vb.Name,
		payload:    in.payloadHash,
	}
}

func parseAuthHeader(header string) map[string]string {
	result := make(map[string]string)
	body := strings.TrimPrefix(header, "AWS4-HMAC-SHA256 ")
	// The spec allows a comma with or without whitespace (minio-go omits it for streaming uploads)
	for _, part := range strings.Split(body, ",") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) == 2 {
			result[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}
	return result
}

// canonicalHeaderValue applies the SigV4 rules to a header value: surrounding whitespace is
// dropped and runs of whitespace collapse to one space; repeated headers are joined with ",".
func canonicalHeaderValue(values []string) string {
	trimmed := make([]string, len(values))
	for i, v := range values {
		trimmed[i] = strings.Join(strings.Fields(v), " ")
	}
	return strings.Join(trimmed, ",")
}

func computeSignatureV4(r *http.Request, secretKey string, in v4Request) string {
	// AWS Signature V4 requires URI-encoding the path.
	// Go's r.URL.Path is decoded. r.URL.RawPath may contain the original encoded path.
	// However, to be safe and consistent with S3 specs, we should take the decoded path
	// and encode it, ensuring that '/' is NOT encoded.
	canonicalURI := r.URL.Path
	if canonicalURI == "" {
		canonicalURI = "/"
	}

	// Split by '/' and encode each part, then join back with '/'
	parts := strings.Split(canonicalURI, "/")
	for i, part := range parts {
		parts[i] = uriEncode(part)
	}
	canonicalURI = strings.Join(parts, "/")

	canonicalQueryString := buildCanonicalQuery(in.query)
	signedHeaders := strings.Split(in.signedHeaders, ";")
	sort.Strings(signedHeaders)

	var canonicalHeaders strings.Builder
	for _, h := range signedHeaders {
		val := ""
		if h == "host" {
			val = canonicalHeaderValue([]string{r.Host})
		} else {
			// Header names are lowercase in the canonical request; Go stores them Title-Case.
			if values := r.Header.Values(h); len(values) > 0 {
				val = canonicalHeaderValue(values)
			}
		}
		canonicalHeaders.WriteString(h)
		canonicalHeaders.WriteString(":")
		canonicalHeaders.WriteString(val)
		canonicalHeaders.WriteString("\n")
	}

	canonicalRequest := strings.Join([]string{
		r.Method,
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders.String(),
		in.signedHeaders,
		in.payloadHash,
	}, "\n")

	amzDate := in.amzDate
	if amzDate == "" {
		amzDate = time.Now().UTC().Format("20060102T150405Z")
	}
	scope := in.dateStamp + "/" + in.region + "/" + in.service + "/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hashSHA256(canonicalRequest)

	signingKey := deriveSigningKey(secretKey, in.dateStamp, in.region, in.service)

	return hex.EncodeToString(hmacSHA256(signingKey, stringToSign))
}

func buildCanonicalQuery(query url.Values) string {
	if len(query) == 0 {
		return ""
	}
	keys := make([]string, 0, len(query))
	for k := range query {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var parts []string
	for _, k := range keys {
		vals := query[k]
		sort.Strings(vals)
		for _, v := range vals {
			parts = append(parts, uriEncode(k)+"="+uriEncode(v))
		}
	}
	return strings.Join(parts, "&")
}

func uriEncode(s string) string {
	var buf strings.Builder
	for _, b := range []byte(s) {
		if isUnreserved(b) {
			buf.WriteByte(b)
		} else {
			fmt.Fprintf(&buf, "%%%02X", b)
		}
	}
	return buf.String()
}

func isUnreserved(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
		(c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~'
}

func deriveSigningKey(secret, dateStamp, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	return hmacSHA256(kService, "aws4_request")
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func hashSHA256(data string) string {
	h := sha256.New()
	io.WriteString(h, data)
	return hex.EncodeToString(h.Sum(nil))
}

func xmlError(w http.ResponseWriter, code int, s3Code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(code)
	var esc strings.Builder
	_ = xml.EscapeText(&esc, []byte(message)) // messages may contain client-supplied names
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, s3Code, esc.String())
}

// authLogger logs rejected requests. Rejections are attacker-controlled, so a flood must not
// become a log flood: at most authLogBurst entries per second are written, with a summary of
// how many were dropped. Each AuthMiddleware owns its limiter.
const authLogBurst = 20

type authLogger struct {
	*slog.Logger
	mu         sync.Mutex
	second     int64
	written    int
	suppressed int
}

func (a *authLogger) warn(args ...any) {
	now := time.Now().Unix()
	a.mu.Lock()
	if a.second != now {
		if a.suppressed > 0 {
			a.Warn("auth_rejected", "reason", "log rate limit", "suppressed", a.suppressed)
		}
		a.second, a.written, a.suppressed = now, 0, 0
	}
	if a.written >= authLogBurst {
		a.suppressed++
		a.mu.Unlock()
		return
	}
	a.written++
	a.mu.Unlock()
	a.Warn("auth_rejected", args...)
}

// errPayloadHash is returned by the body reader when the received body does not match the
// x-amz-content-sha256 the request was signed with.
var errPayloadHash = errors.New("x-amz-content-sha256 mismatch")

// verifyPayloadHash makes a signed payload hash binding. SigV4 signs the body only through
// the hash in x-amz-content-sha256; if the server never compared it with the body, a captured
// signed request could be replayed with different content within the validity window.
// Requests with UNSIGNED-PAYLOAD or aws-chunked bodies (verified chunk by chunk) are exempt.
func verifyPayloadHash(r *http.Request, sess *v4Session) bool {
	want := strings.ToLower(sess.payload)
	if len(want) != 64 {
		return true // UNSIGNED-PAYLOAD, STREAMING-*, ...
	}
	if _, err := hex.DecodeString(want); err != nil {
		return false
	}
	if r.ContentLength == 0 || r.Body == nil || r.Body == http.NoBody {
		return want == emptyPayloadHash
	}
	r.Body = &hashingBody{ReadCloser: r.Body, h: sha256.New(), want: want}
	return true
}

const emptyPayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// hashingBody hashes what passes through and fails the final Read if the digest is not the
// announced one, so the consumer sees an error before it can treat the body as complete.
type hashingBody struct {
	io.ReadCloser
	h    hash.Hash
	want string
	done bool
}

func (b *hashingBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.h.Write(p[:n])
	if errors.Is(err, io.EOF) && !b.done {
		b.done = true
		if hex.EncodeToString(b.h.Sum(nil)) != b.want {
			return n, errPayloadHash
		}
	}
	return n, err
}

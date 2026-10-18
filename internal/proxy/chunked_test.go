package proxy

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

func TestParseAuthHeaderCommaVariants(t *testing.T) {
	const (
		withSpace = "AWS4-HMAC-SHA256 Credential=AK/20261017/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123"
		noSpace   = "AWS4-HMAC-SHA256 Credential=AK/20261017/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-date,Signature=abc123"
	)
	for name, h := range map[string]string{"with space": withSpace, "no space": noSpace} {
		got := parseAuthHeader(h)
		if got["Credential"] != "AK/20261017/us-east-1/s3/aws4_request" ||
			got["SignedHeaders"] != "host;x-amz-date" || got["Signature"] != "abc123" {
			t.Errorf("%s: unexpected parse result %v", name, got)
		}
	}
}

// ---- test-side encoder (follows the AWS aws-chunked specification) ----

type testSession struct{ *v4Session }

func newTestSession(t *testing.T) (testSession, time.Time) {
	t.Helper()
	now := time.Now().UTC()
	date := now.Format("20060102")
	return testSession{&v4Session{
		signingKey: deriveSigningKey(testSK, date, "us-east-1", "s3"),
		amzDate:    now.Format("20060102T150405Z"),
		scope:      date + "/us-east-1/s3/aws4_request",
		seedSig:    strings.Repeat("a", 64),
	}}, now
}

func (s testSession) chunkSig(prev string, data []byte) string {
	sts := strings.Join([]string{"AWS4-HMAC-SHA256-PAYLOAD", s.amzDate, s.scope, prev, hashSHA256(""), hashSHA256(string(data))}, "\n")
	return hex.EncodeToString(hmacSHA256(s.signingKey, sts))
}

// encode builds an aws-chunked body from data split into pieces of chunk bytes.
func (s testSession) encode(data []byte, chunk int, signed bool, trailerName string) []byte {
	var out bytes.Buffer
	prev := s.seedSig
	for off := 0; off < len(data); off += chunk {
		end := min(off+chunk, len(data))
		part := data[off:end]
		if signed {
			sig := s.chunkSig(prev, part)
			fmt.Fprintf(&out, "%x;chunk-signature=%s\r\n", len(part), sig)
			prev = sig
		} else {
			fmt.Fprintf(&out, "%x\r\n", len(part))
		}
		out.Write(part)
		out.WriteString("\r\n")
	}
	if signed {
		sig := s.chunkSig(prev, nil)
		fmt.Fprintf(&out, "0;chunk-signature=%s\r\n", sig)
		prev = sig
	} else {
		out.WriteString("0\r\n")
	}
	if trailerName != "" {
		var sum [4]byte
		c := crc32.ChecksumIEEE(data)
		sum[0], sum[1], sum[2], sum[3] = byte(c>>24), byte(c>>16), byte(c>>8), byte(c)
		val := base64.StdEncoding.EncodeToString(sum[:])
		fmt.Fprintf(&out, "%s:%s\r\n", trailerName, val)
		if signed {
			canonical := trailerName + ":" + val + "\n"
			sts := strings.Join([]string{"AWS4-HMAC-SHA256-TRAILER", s.amzDate, s.scope, prev, hashSHA256(canonical)}, "\n")
			fmt.Fprintf(&out, "x-amz-trailer-signature:%s\r\n", hex.EncodeToString(hmacSHA256(s.signingKey, sts)))
		}
	}
	out.WriteString("\r\n")
	return out.Bytes()
}

func decode(t *testing.T, s testSession, payload string, body []byte, decodedLen int, trailerHeader string) ([]byte, error) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPut, "/b/k", bytes.NewReader(body))
	r.Header.Set("X-Amz-Content-Sha256", payload)
	r.Header.Set("X-Amz-Decoded-Content-Length", strconv.Itoa(decodedLen))
	r.Header.Set("Content-Encoding", "aws-chunked")
	if trailerHeader != "" {
		r.Header.Set("X-Amz-Trailer", trailerHeader)
	}
	decodeStreamingBody(r, s.v4Session)
	if r.ContentLength != int64(decodedLen) {
		t.Fatalf("ContentLength = %d, want %d", r.ContentLength, decodedLen)
	}
	if r.Header.Get("Content-Encoding") != "" {
		t.Fatal("aws-chunked Content-Encoding must be stripped")
	}
	return io.ReadAll(r.Body)
}

func payloadOf(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + 3)
	}
	return b
}

func TestChunkedDecodeVariants(t *testing.T) {
	s, _ := newTestSession(t)
	data := payloadOf(200_000)
	const crc = "x-amz-checksum-crc32"

	tests := []struct {
		name, payload, trailer string
		signed                 bool
	}{
		{"signed", payloadStreamingSigned, "", true},
		{"signed + trailer checksum", payloadStreamingSignedTrailer, crc, true},
		{"unsigned + trailer checksum", payloadStreamingUnsignedTrails, crc, false},
	}
	for _, tt := range tests {
		for _, chunk := range []int{1, 777, 65536, 1 << 20, 200_000, 1 << 30} { // 1<<30: the whole body as ONE chunk
			if chunk == 1 && len(data) > 1000 {
				continue // 200k single-byte chunks: covered with a smaller payload below
			}
			t.Run(fmt.Sprintf("%s/chunk=%d", tt.name, chunk), func(t *testing.T) {
				body := s.encode(data, chunk, tt.signed, tt.trailer)
				got, err := decode(t, s, tt.payload, body, len(data), tt.trailer)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, data) {
					t.Fatalf("decoded data differs (got %d bytes)", len(got))
				}
			})
		}
	}

	t.Run("tiny chunks", func(t *testing.T) {
		small := payloadOf(300)
		got, err := decode(t, s, payloadStreamingSigned, s.encode(small, 1, true, ""), len(small), "")
		if err != nil || !bytes.Equal(got, small) {
			t.Fatalf("err=%v equal=%v", err, bytes.Equal(got, small))
		}
	})
	t.Run("empty object", func(t *testing.T) {
		got, err := decode(t, s, payloadStreamingSigned, s.encode(nil, 64, true, ""), 0, "")
		if err != nil || len(got) != 0 {
			t.Fatalf("err=%v len=%d", err, len(got))
		}
	})
}

func TestChunkedRejectsTampering(t *testing.T) {
	s, _ := newTestSession(t)
	data := payloadOf(5000)

	t.Run("modified data", func(t *testing.T) {
		body := s.encode(data, 1000, true, "")
		i := bytes.Index(body, []byte("\r\n")) + 2 + 10 // inside the first chunk's data
		body[i] ^= 0xff
		_, err := decode(t, s, payloadStreamingSigned, body, len(data), "")
		if !errors.Is(err, errChunkSignature) {
			t.Fatalf("err = %v, want chunk signature error", err)
		}
	})
	t.Run("wrong signing key", func(t *testing.T) {
		body := s.encode(data, 1000, true, "")
		other := s
		other.v4Session = &v4Session{signingKey: deriveSigningKey("other", "20260101", "us-east-1", "s3"), amzDate: s.amzDate, scope: s.scope, seedSig: s.seedSig}
		_, err := decode(t, other, payloadStreamingSigned, body, len(data), "")
		if !errors.Is(err, errChunkSignature) {
			t.Fatalf("err = %v, want chunk signature error", err)
		}
	})
	t.Run("dropped chunk breaks the chain", func(t *testing.T) {
		full := s.encode(data, 1000, true, "")
		first := bytes.Index(full, []byte("\r\n1000")) // not present; fall back below
		_ = first
		// Re-encode and cut the second chunk out.
		var parts [][]byte
		rest := full
		for len(rest) > 0 {
			hdrEnd := bytes.Index(rest, []byte("\r\n")) + 2
			sizeHex, _, _ := strings.Cut(string(rest[:hdrEnd]), ";")
			n, _ := strconv.ParseInt(sizeHex, 16, 64)
			end := hdrEnd + int(n) + 2
			if n == 0 {
				end = len(rest)
			}
			parts = append(parts, rest[:end])
			rest = rest[end:]
		}
		body := bytes.Join(append(append([][]byte{}, parts[0]), parts[2:]...), nil)
		_, err := decode(t, s, payloadStreamingSigned, body, len(data), "")
		if !errors.Is(err, errChunkSignature) {
			t.Fatalf("err = %v, want chunk signature error", err)
		}
	})
	t.Run("checksum mismatch", func(t *testing.T) {
		body := s.encode(data, 1000, false, "x-amz-checksum-crc32")
		// the unsigned trailer value is the last header line: corrupt one character
		i := bytes.LastIndex(body, []byte("x-amz-checksum-crc32:")) + len("x-amz-checksum-crc32:")
		if body[i] == 'A' {
			body[i] = 'B'
		} else {
			body[i] = 'A'
		}
		_, err := decode(t, s, payloadStreamingUnsignedTrails, body, len(data), "x-amz-checksum-crc32")
		if !errors.Is(err, errBadChunk) {
			t.Fatalf("err = %v, want malformed/checksum error", err)
		}
	})
	t.Run("announced trailer missing", func(t *testing.T) {
		body := s.encode(data, 1000, false, "")
		_, err := decode(t, s, payloadStreamingUnsignedTrails, body, len(data), "x-amz-checksum-crc32")
		if !errors.Is(err, errBadChunk) {
			t.Fatalf("err = %v, want error", err)
		}
	})
	t.Run("truncated body", func(t *testing.T) {
		body := s.encode(data, 1000, true, "")
		_, err := decode(t, s, payloadStreamingSigned, body[:len(body)/2], len(data), "")
		if err == nil {
			t.Fatal("truncated body must fail")
		}
	})
	t.Run("declared length mismatch", func(t *testing.T) {
		body := s.encode(data, 1000, true, "")
		_, err := decode(t, s, payloadStreamingSigned, body, len(data)+1, "")
		if !errors.Is(err, errBadChunk) {
			t.Fatalf("err = %v, want length error", err)
		}
	})
	t.Run("absurd chunk size", func(t *testing.T) {
		body := []byte(fmt.Sprintf("%x;chunk-signature=%s\r\n", uint64(maxChunkSize)+1, strings.Repeat("0", 64)))
		_, err := decode(t, s, payloadStreamingSigned, body, 1, "")
		if !errors.Is(err, errBadChunk) {
			t.Fatalf("err = %v, want error", err)
		}
	})
	t.Run("corruption inside a big chunk is caught at its end", func(t *testing.T) {
		big := payloadOf(3 << 20)
		body := s.encode(big, len(big), true, "") // ONE chunk, like the AWS CLI sends
		body[len(body)/2] ^= 0x01
		got, err := decode(t, s, payloadStreamingSigned, body, len(big), "")
		if !errors.Is(err, errChunkSignature) {
			t.Fatalf("err = %v, want chunk signature error (read %d bytes)", err, len(got))
		}
	})
}

// End to end through AuthMiddleware: a request whose seed signature is valid and
// whose body is aws-chunked reaches the handler decoded, with the right length.
func TestAuthMiddlewareDecodesStreamingBody(t *testing.T) {
	data := payloadOf(150_000)
	now := time.Now().UTC()

	// Compute the seed signature exactly as a client would, using the SDK signer
	// for a request that carries the STREAMING payload hash.
	build := func(secretForChunks string) *http.Request {
		req := httptest.NewRequest(http.MethodPut, "http://example.com/b/key.bin", nil)
		req.Header.Set("X-Amz-Content-Sha256", payloadStreamingSigned)
		req.Header.Set("X-Amz-Decoded-Content-Length", strconv.Itoa(len(data)))
		req.Header.Set("Content-Encoding", "aws-chunked")
		req.Header.Set("Content-Length", "0")
		if err := v4.NewSigner().SignHTTP(context.Background(),
			aws.Credentials{AccessKeyID: testAK, SecretAccessKey: testSK},
			req, payloadStreamingSigned, "s3", "us-east-1", now); err != nil {
			t.Fatal(err)
		}
		auth := parseAuthHeader(req.Header.Get("Authorization"))
		date := now.Format("20060102")
		sess := testSession{&v4Session{
			signingKey: deriveSigningKey(secretForChunks, date, "us-east-1", "s3"),
			amzDate:    req.Header.Get("X-Amz-Date"),
			scope:      date + "/us-east-1/s3/aws4_request",
			seedSig:    auth["Signature"],
		}}
		req.Body = io.NopCloser(bytes.NewReader(sess.encode(data, 65536, true, "")))
		return req
	}

	run := func(req *http.Request) (code int, got []byte, length int64) {
		log := slog.New(slog.NewJSONHandler(io.Discard, nil))
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			length = r.ContentLength
			b, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			got = b
		})
		rec := httptest.NewRecorder()
		AuthMiddleware(testCreds(), nil, log, next).ServeHTTP(rec, req)
		return rec.Code, got, length
	}

	code, got, length := run(build(testSK))
	if code != http.StatusOK || !bytes.Equal(got, data) || length != int64(len(data)) {
		t.Fatalf("valid streaming upload: code=%d equal=%v length=%d", code, bytes.Equal(got, data), length)
	}

	// Valid seed signature but chunks signed with a different key: the handler's read must fail
	code, got, _ = run(build("not-the-secret"))
	if code != http.StatusForbidden || bytes.Equal(got, data) {
		t.Fatalf("forged chunks must be rejected (code=%d)", code)
	}
}

package proxy

import (
	"bufio"
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// Payload hash values (X-Amz-Content-Sha256) that announce an aws-chunked body.
const (
	payloadStreamingSigned         = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	payloadStreamingSignedTrailer  = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
	payloadStreamingUnsignedTrails = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
)

const (
	maxChunkHeaderLen = 4096
	// Sanity bound for a declared chunk size. Chunk data is streamed, never buffered
	// (AWS CLI sends a whole 5 GiB part as ONE chunk), so this is not a memory limit.
	maxChunkSize = 1 << 42
)

var (
	// errChunkSignature is returned when a chunk or trailer signature does not match.
	errChunkSignature = errors.New("chunk signature does not match")
	// errBadChunk is returned for a malformed aws-chunked body or a failed checksum.
	errBadChunk = errors.New("malformed aws-chunked body")
)

// v4Session carries what is needed to verify the chunk signatures of a request
// whose seed signature has just been verified.
type v4Session struct {
	signingKey []byte
	amzDate    string
	scope      string
	seedSig    string
	bucket     string // virtual bucket of the access key
	payload    string // x-amz-content-sha256 (or UNSIGNED-PAYLOAD) the request was signed with
}

// decodeStreamingBody replaces r.Body with a decoder when the request carries an
// aws-chunked body, and fixes up ContentLength to the decoded length. It is a
// no-op for ordinary requests.
func decodeStreamingBody(r *http.Request, sess *v4Session) {
	payload := r.Header.Get("X-Amz-Content-Sha256")
	var signed, trailer bool
	switch payload {
	case payloadStreamingSigned:
		signed = true
	case payloadStreamingSignedTrailer:
		signed, trailer = true, true
	case payloadStreamingUnsignedTrails:
		trailer = true
	default:
		return
	}

	cr := &chunkedReader{
		src:     r.Body,
		br:      bufio.NewReaderSize(r.Body, 64<<10),
		signed:  signed,
		trailer: trailer,
		sess:    sess,
		prevSig: sess.seedSig,
		wantLen: -1,
	}
	if v := r.Header.Get("X-Amz-Decoded-Content-Length"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			cr.wantLen = n
		}
	}
	if trailer {
		cr.trailerNames = trailerNames(r.Header.Get("X-Amz-Trailer"))
		for _, name := range cr.trailerNames {
			if h := checksumHash(name); h != nil {
				cr.sums = append(cr.sums, checksum{header: name, h: h})
			}
		}
	}

	r.Body = cr
	r.ContentLength = cr.wantLen
	r.Header.Set("Content-Length", strconv.FormatInt(cr.wantLen, 10))
	r.Header.Del("X-Amz-Decoded-Content-Length")
	r.Header.Del("Content-Encoding")
}

func trailerNames(v string) []string {
	var names []string
	for _, n := range strings.Split(v, ",") {
		if n = strings.ToLower(strings.TrimSpace(n)); n != "" {
			names = append(names, n)
		}
	}
	return names
}

type checksum struct {
	header string
	h      hash.Hash
}

// checksumHash returns the hash behind an x-amz-checksum-* trailer, or nil if unknown.
func checksumHash(header string) hash.Hash {
	switch header {
	case "x-amz-checksum-crc32":
		return crc32.NewIEEE()
	case "x-amz-checksum-crc32c":
		return crc32.New(crc32.MakeTable(crc32.Castagnoli))
	case "x-amz-checksum-sha1":
		return sha1.New()
	case "x-amz-checksum-sha256":
		return sha256.New()
	}
	return nil
}

// chunkedReader decodes the aws-chunked content encoding:
//
//	<hex size>[;chunk-signature=<sig>]\r\n<data>\r\n ... 0[;chunk-signature=<sig>]\r\n[<trailers>]\r\n
//
// Chunk data is streamed through (a chunk can be gigabytes, so it is never held in
// memory) while its SHA-256 is computed; the chunk signature is checked as soon as
// the last byte of the chunk has been read, and the trailer signature and trailing
// checksums at the end. A failed check surfaces as an error from Read, so consumers
// must not commit the object before they have seen io.EOF. Both upload paths do
// that: buffered PUTs read the whole body first, streamed PUTs abort the multipart
// upload on a read error.
type chunkedReader struct {
	src          io.Closer
	br           *bufio.Reader
	signed       bool
	trailer      bool
	sess         *v4Session
	prevSig      string
	trailerNames []string
	sums         []checksum
	wantLen      int64
	got          int64

	remaining int64     // bytes of the current chunk still to be read
	chunkSig  string    // signature announced for the current chunk
	chunkHash hash.Hash // SHA-256 of the current chunk's data (signed uploads)
	started   bool      // a chunk header has been read
	done      bool
	err       error
}

func (c *chunkedReader) Close() error { return c.src.Close() }

func (c *chunkedReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		if c.err != nil {
			return 0, c.err
		}
		if c.done {
			return 0, io.EOF
		}
		if c.remaining > 0 {
			return c.readData(p)
		}
		if err := c.nextChunk(); err != nil {
			c.err = err
			return 0, err
		}
	}
}

// readData copies chunk data to the caller and, on the last byte of the chunk,
// verifies the chunk (the error, if any, is returned by the next Read).
func (c *chunkedReader) readData(p []byte) (int, error) {
	if int64(len(p)) > c.remaining {
		p = p[:c.remaining]
	}
	n, err := c.br.Read(p)
	if n > 0 {
		if c.chunkHash != nil {
			c.chunkHash.Write(p[:n])
		}
		for _, s := range c.sums {
			s.h.Write(p[:n])
		}
		c.remaining -= int64(n)
		c.got += int64(n)
	}
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		c.err = err
		return n, nil
	}
	if c.remaining == 0 {
		c.err = c.endChunk()
	}
	return n, nil
}

func (c *chunkedReader) readLine() (string, error) {
	var line []byte
	for {
		b, err := c.br.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return "", err
		}
		if b == '\n' {
			break
		}
		line = append(line, b)
		if len(line) > maxChunkHeaderLen {
			return "", fmt.Errorf("%w: chunk header too long", errBadChunk)
		}
	}
	return strings.TrimSuffix(string(line), "\r"), nil
}

// nextChunk reads a chunk header. For a data chunk it arms readData; for the
// final zero-length chunk it verifies the trailers and finishes the stream.
func (c *chunkedReader) nextChunk() error {
	header, err := c.readLine()
	if err != nil {
		return err
	}
	sizeStr, ext, _ := strings.Cut(header, ";")
	size, err := strconv.ParseUint(strings.TrimSpace(sizeStr), 16, 64)
	if err != nil || size > maxChunkSize {
		return fmt.Errorf("%w: invalid chunk size %q", errBadChunk, sizeStr)
	}

	c.chunkSig = ""
	if c.signed {
		name, val, ok := strings.Cut(strings.TrimSpace(ext), "=")
		if !ok || name != "chunk-signature" {
			return fmt.Errorf("%w: missing chunk signature", errBadChunk)
		}
		c.chunkSig = val
		c.chunkHash = sha256.New()
	}
	c.started = true

	if size > 0 {
		c.remaining = int64(size)
		return nil
	}

	// Final (zero-length) chunk: its own signature, then trailers and the closing empty line.
	if err := c.verifyChunkSignature(); err != nil {
		return err
	}
	if err := c.readTrailers(); err != nil {
		return err
	}
	if c.wantLen >= 0 && c.got != c.wantLen {
		return fmt.Errorf("%w: decoded %d bytes, expected %d", errBadChunk, c.got, c.wantLen)
	}
	c.done = true
	return nil
}

// endChunk runs after the last data byte of a chunk: CRLF terminator and signature.
func (c *chunkedReader) endChunk() error {
	var crlf [2]byte
	if _, err := io.ReadFull(c.br, crlf[:]); err != nil || crlf != [2]byte{'\r', '\n'} {
		return fmt.Errorf("%w: missing chunk terminator", errBadChunk)
	}
	return c.verifyChunkSignature()
}

func (c *chunkedReader) verifyChunkSignature() error {
	if !c.signed {
		return nil
	}
	want := c.expectedChunkSignature()
	if !hmac.Equal([]byte(c.chunkSig), []byte(want)) {
		return errChunkSignature
	}
	c.prevSig = c.chunkSig
	return nil
}

func (c *chunkedReader) readTrailers() error {
	trailers := make(map[string]string)
	var canonical bytes.Buffer
	var trailerSig string
	for {
		line, err := c.readLine()
		if err != nil {
			return err
		}
		if line == "" {
			break
		}
		name, val, ok := strings.Cut(line, ":")
		if !ok {
			return fmt.Errorf("%w: malformed trailer %q", errBadChunk, line)
		}
		name = strings.ToLower(strings.TrimSpace(name))
		val = strings.TrimSpace(val)
		if name == "x-amz-trailer-signature" {
			trailerSig = val
			continue
		}
		trailers[name] = val
		canonical.WriteString(name + ":" + val + "\n")
		if !c.trailer {
			return fmt.Errorf("%w: unexpected trailer", errBadChunk)
		}
	}

	if c.trailer && c.signed {
		want := hex.EncodeToString(hmacSHA256(c.sess.signingKey, strings.Join([]string{
			"AWS4-HMAC-SHA256-TRAILER", c.sess.amzDate, c.sess.scope, c.prevSig, hashSHA256(canonical.String()),
		}, "\n")))
		if trailerSig == "" || !hmac.Equal([]byte(trailerSig), []byte(want)) {
			return errChunkSignature
		}
	}

	for _, s := range c.sums {
		got, ok := trailers[s.header]
		if !ok {
			return fmt.Errorf("%w: announced trailer %s is missing", errBadChunk, s.header)
		}
		if base64.StdEncoding.EncodeToString(s.h.Sum(nil)) != got {
			return fmt.Errorf("%w: %s mismatch", errBadChunk, s.header)
		}
	}
	return nil
}

// expectedChunkSignature computes the signature the current chunk must carry.
func (c *chunkedReader) expectedChunkSignature() string {
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256-PAYLOAD",
		c.sess.amzDate,
		c.sess.scope,
		c.prevSig,
		hashSHA256(""),
		hex.EncodeToString(c.chunkHash.Sum(nil)),
	}, "\n")
	return hex.EncodeToString(hmacSHA256(c.sess.signingKey, stringToSign))
}

package proxy

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/spions/s3syncer/internal/replicator"
	"github.com/spions/s3syncer/internal/storage"
)

// isGeeseFS checks if the request is from GeeseFS client
func isGeeseFS(r *http.Request) bool {
	ua := r.UserAgent()
	return strings.HasPrefix(ua, "GeeseFS/")
}

// isS3fs checks if the request is from s3fs client
func isS3fs(r *http.Request) bool {
	ua := r.UserAgent()
	return strings.Contains(ua, "s3fs")
}

// filterMetadataForClient filters metadata based on client type.
// For GeeseFS clients, returns all metadata (including symlink metadata for xattrs support).
// For other clients, filters out symlink metadata to maintain transparent symlink behavior.
func filterMetadataForClient(r *http.Request, metadata map[string]string) map[string]string {
	if metadata == nil {
		return nil
	}

	// GeeseFS and s3fs clients get all metadata
	if isGeeseFS(r) || isS3fs(r) {
		return metadata
	}

	// Other clients: filter out symlink metadata
	filtered := make(map[string]string)
	for k, v := range metadata {
		if k == storage.SymlinkMetaKey || strings.HasSuffix(k, "symlink-target") {
			continue
		}
		// Also filter out s3fs mode if it's a symlink
		if k == storage.S3fsModeKey && v == storage.S3fsSymlinkMode {
			continue
		}
		filtered[k] = v
	}

	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

// resolveSymlink checks if the object is a symlink and resolves it for non-GeeseFS and non-s3fs clients.
// Returns the resolved body, info, and error. If not a symlink or client is GeeseFS/s3fs, returns original values.
func resolveSymlink(ctx context.Context, r *http.Request, rep *replicator.Replicator, body io.ReadCloser, info *storage.ObjectInfo, log *slog.Logger) (io.ReadCloser, *storage.ObjectInfo, error) {
	// GeeseFS and s3fs clients handle symlinks themselves
	if isGeeseFS(r) || isS3fs(r) {
		return body, info, nil
	}

	// Check if this is a symlink
	log.Debug("checking if symlink", "key", info.Key, "is_symlink", info.IsSymlink(), "metadata", info.Metadata)
	if !info.IsSymlink() {
		return body, info, nil
	}

	// Get the target key
	targetKey := info.SymlinkTarget()
	if targetKey == "" {
		// If it's a s3fs style symlink, target is in the body
		if mode, ok := info.Metadata[storage.S3fsModeKey]; ok && mode == storage.S3fsSymlinkMode {
			defer body.Close()
			content, err := io.ReadAll(io.LimitReader(body, 4096))
			if err != nil {
				log.Error("failed to read s3fs symlink body", "key", info.Key, "error", err)
				return nil, info, err
			}
			targetKey = strings.TrimSpace(string(content))
		}
	}

	if targetKey == "" {
		// Symlink without target - return original body
		log.Warn("symlink without target", "key", info.Key)
		return body, info, nil
	}

	log.Debug("following symlink", "from", info.Key, "to", targetKey)

	// Close the symlink body (it's empty anyway for real symlinks)
	if body != nil {
		body.Close()
	}

	// Fetch the target object
	targetBody, targetInfo, err := rep.Get(ctx, targetKey)
	if err != nil {
		// If target doesn't exist, return error
		log.Error("symlink target not found", "key", info.Key, "target", targetKey, "error", err)
		return nil, info, err
	}

	log.Debug("symlink resolved for GET", "key", info.Key, "target", targetKey)

	// Return target's content
	return targetBody, targetInfo, nil
}

// resolveSymlinkHead checks if the object is a symlink and resolves it for non-GeeseFS and non-s3fs clients (HEAD request).
// Returns the resolved info and error. If not a symlink or client is GeeseFS/s3fs, returns original info.
func resolveSymlinkHead(ctx context.Context, r *http.Request, rep *replicator.Replicator, info *storage.ObjectInfo, log *slog.Logger) (*storage.ObjectInfo, error) {
	// GeeseFS and s3fs clients handle symlinks themselves
	if isGeeseFS(r) || isS3fs(r) {
		return info, nil
	}

	// Check if this is a symlink
	if !info.IsSymlink() {
		return info, nil
	}

	// Get the target key
	targetKey := info.SymlinkTarget()
	if targetKey == "" {
		// If it's a s3fs style symlink, target is in the body, but for HEAD we don't have the body.
		// We have to GET the object to find where it points.
		if mode, ok := info.Metadata[storage.S3fsModeKey]; ok && mode == storage.S3fsSymlinkMode {
			body, _, err := rep.Get(ctx, info.Key)
			if err != nil {
				return info, err
			}
			defer body.Close()
			content, err := io.ReadAll(io.LimitReader(body, 4096))
			if err != nil {
				return info, err
			}
			targetKey = strings.TrimSpace(string(content))
		}
	}

	if targetKey == "" {
		return info, nil
	}

	log.Debug("following symlink for HEAD", "from", info.Key, "to", targetKey)

	// Fetch the target object metadata
	targetInfo, err := rep.Head(ctx, targetKey)
	if err != nil {
		// If target doesn't exist, return error
		return info, err
	}

	log.Debug("symlink resolved for HEAD", "key", info.Key, "target", targetKey, "target_size", targetInfo.Size)

	// Return target's metadata
	return targetInfo, nil
}

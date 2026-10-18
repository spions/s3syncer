package taskmanager

import (
	"context"
	"fmt"
	"time"

	"github.com/spions/s3syncer/internal/queue"
	"github.com/spions/s3syncer/internal/storage"
)

const chunkSize = 500

// executeResync runs the resync task with chunked processing
func (tm *TaskManager) executeResync(ctx context.Context, taskID, bucket string) {
	tm.log.Info("resync task started", "task_id", taskID, "bucket", bucket)

	// Find virtual bucket config
	var vbConfig *struct {
		primary storage.StorageBackend
		backups []storage.StorageBackend
	}

	for _, vb := range tm.config.VirtualBuckets {
		if vb.Name == bucket {
			pb, err := vb.PrimaryBackend()
			if err != nil {
				tm.markFailed(taskID, "resync", bucket, err.Error())
				return
			}

			primary := tm.backends[pb.Name]
			if primary == nil {
				tm.markFailed(taskID, "resync", bucket, fmt.Sprintf("primary backend %s not found", pb.Name))
				return
			}

			var backups []storage.StorageBackend
			for _, bb := range vb.BackupBackends() {
				backend := tm.backends[bb.Name]
				if backend != nil {
					backups = append(backups, backend)
				}
			}

			vbConfig = &struct {
				primary storage.StorageBackend
				backups []storage.StorageBackend
			}{primary, backups}
			break
		}
	}

	if vbConfig == nil {
		tm.markFailed(taskID, "resync", bucket, "bucket not found")
		return
	}

	// List and process objects in pages to avoid loading all 645K+ objects into memory
	var processed int64
	var totalSoFar int64
	chunkNum := int64(0)

	totalObjects, listErr := vbConfig.primary.ListObjectsPaginatedFull(ctx, "", chunkSize, func(objects []storage.ObjectInfo) error {
		// Check for cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		chunkNum++
		totalSoFar += int64(len(objects))
		tm.log.Debug("resync processing page", "task_id", taskID, "chunk", chunkNum, "objects", len(objects))

		// Enqueue this page
		for _, obj := range objects {
			for _, backup := range vbConfig.backups {
				if err := tm.queue.Enqueue(bucket, obj.Key, vbConfig.primary.Name(), backup.Name(), queue.PriorityLow); err != nil {
					tm.log.Error("enqueue failed", "task_id", taskID, "key", obj.Key, "error", err)
				}
			}
		}

		// Notify worker about new tasks
		if tm.worker != nil {
			tm.worker.Notify()
		}

		processed += int64(len(objects))
		tm.updateProgress(taskID, "resync", bucket, processed, totalSoFar, chunkNum)

		// Wait for this chunk to be processed before fetching next page
		if !queue.WaitForChunkCompletion(ctx, tm.queue, 500*time.Millisecond) {
			return ctx.Err()
		}

		return nil
	})

	if listErr != nil {
		if ctx.Err() != nil {
			tm.markCancelled(taskID, "resync", bucket)
			tm.log.Info("resync task cancelled", "task_id", taskID)
		} else {
			tm.markFailed(taskID, "resync", bucket, fmt.Sprintf("list objects: %v", listErr))
		}
		return
	}

	tm.log.Info("resync listed objects", "task_id", taskID, "total", totalObjects)

	tm.markCompleted(taskID, "resync", bucket)
	tm.log.Info("resync task completed", "task_id", taskID, "processed", processed)
}

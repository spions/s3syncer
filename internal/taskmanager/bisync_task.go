package taskmanager

import (
	"context"
	"fmt"
	"time"

	"github.com/spions/s3syncer/internal/queue"
	"github.com/spions/s3syncer/internal/storage"
)

type bisyncTask struct {
	virtualBucket string
	objectKey     string
	source        string
	target        string
}

// executeBisync runs the bisync task with chunked processing
func (tm *TaskManager) executeBisync(ctx context.Context, taskID, bucket string) {
	tm.log.Info("bisync task started", "task_id", taskID, "bucket", bucket)

	// Find virtual bucket config
	var vbConfig *struct {
		name    string
		primary storage.StorageBackend
		backups []storage.StorageBackend
	}

	for _, vb := range tm.config.VirtualBuckets {
		if vb.Name == bucket {
			pb, err := vb.PrimaryBackend()
			if err != nil {
				tm.markFailed(taskID, "bisync", bucket, err.Error())
				return
			}

			primary := tm.backends[pb.Name]
			if primary == nil {
				tm.markFailed(taskID, "bisync", bucket, fmt.Sprintf("primary backend %s not found", pb.Name))
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
				name    string
				primary storage.StorageBackend
				backups []storage.StorageBackend
			}{vb.Name, primary, backups}
			break
		}
	}

	if vbConfig == nil {
		tm.markFailed(taskID, "bisync", bucket, "bucket not found")
		return
	}

	// Both listings are walked side by side (sorted by key) and the differences are queued in
	// chunks as they are found, so memory does not grow with the number of objects.
	var processed, found, chunkNum int64
	pending := make([]bisyncTask, 0, chunkSize)

	flush := func() bool {
		if len(pending) == 0 {
			return true
		}
		chunkNum++
		for _, task := range pending {
			if err := tm.queue.Enqueue(task.virtualBucket, task.objectKey, task.source, task.target, queue.PriorityLow); err != nil {
				tm.log.Error("enqueue failed", "task_id", taskID, "key", task.objectKey, "error", err)
			}
		}
		processed += int64(len(pending))
		pending = pending[:0]
		tm.updateProgress(taskID, "bisync", bucket, processed, found, chunkNum)
		if tm.worker != nil {
			tm.worker.Notify()
		}
		return queue.WaitForChunkCompletion(ctx, tm.queue, 500*time.Millisecond)
	}
	add := func(t bisyncTask) error {
		// An object the client deleted must not be copied back from a backend that has not
		// executed that delete yet.
		if ok, err := tm.queue.HasTask(t.virtualBucket, t.objectKey, queue.DeletePrefix+t.source, t.source); err == nil && ok {
			tm.log.Debug("bisync skips an object pending deletion", "task_id", taskID, "key", t.objectKey, "source", t.source)
			return nil
		}
		pending = append(pending, t)
		found++
		if len(pending) >= chunkSize && !flush() {
			return ctx.Err()
		}
		return nil
	}

	for _, backup := range vbConfig.backups {
		ps := newKeyStream(ctx, vbConfig.primary, chunkSize)
		bs := newKeyStream(ctx, backup, chunkSize)
		err := mergeKeys(ps, bs,
			func(o storage.ObjectInfo) error { // primary only -> copy to the backup
				return add(bisyncTask{vbConfig.name, o.Key, vbConfig.primary.Name(), backup.Name()})
			},
			func(o storage.ObjectInfo) error { // backup only -> copy to the primary
				return add(bisyncTask{vbConfig.name, o.Key, backup.Name(), vbConfig.primary.Name()})
			},
			func(p, b storage.ObjectInfo) error {
				if p.Size != b.Size {
					tm.log.Warn("bisync conflict: size mismatch",
						"task_id", taskID, "key", p.Key, "primary_size", p.Size, "backup_size", b.Size)
				}
				return nil
			},
		)
		ps.Close()
		bs.Close()
		if err != nil {
			if ctx.Err() != nil {
				tm.markCancelled(taskID, "bisync", bucket)
				tm.log.Info("bisync task cancelled", "task_id", taskID)
				return
			}
			tm.markFailed(taskID, "bisync", bucket, fmt.Sprintf("listing %s: %v", backup.Name(), err))
			return
		}
	}
	if !flush() {
		tm.markCancelled(taskID, "bisync", bucket)
		tm.log.Info("bisync task cancelled during chunk wait", "task_id", taskID)
		return
	}

	if found == 0 {
		tm.updateProgress(taskID, "bisync", bucket, 0, 0, 0)
		tm.markCompleted(taskID, "bisync", bucket)
		tm.log.Info("bisync task completed (nothing to sync)", "task_id", taskID)
		return
	}
	tm.markCompleted(taskID, "bisync", bucket)
	tm.log.Info("bisync task completed", "task_id", taskID, "processed", processed)
}

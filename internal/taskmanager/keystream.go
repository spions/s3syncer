package taskmanager

import (
	"context"

	"github.com/spions/s3syncer/internal/storage"
)

// keyStream turns the callback-style paginated listing of a backend into something that can
// be read one object at a time, so two listings can be walked side by side. At most one page
// is buffered; the lister waits while the consumer is busy, so memory use stays constant
// however many objects the bucket holds.
type keyStream struct {
	pages  chan []storage.ObjectInfo
	cancel context.CancelFunc
	err    error // valid once pages is closed
	cur    []storage.ObjectInfo
	pos    int
}

func newKeyStream(ctx context.Context, b storage.StorageBackend, pageSize int) *keyStream {
	ctx, cancel := context.WithCancel(ctx)
	ks := &keyStream{pages: make(chan []storage.ObjectInfo), cancel: cancel}
	go func() {
		defer close(ks.pages)
		_, ks.err = b.ListObjectsPaginatedFull(ctx, "", pageSize, func(page []storage.ObjectInfo) error {
			select {
			case ks.pages <- page:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	return ks
}

// peek returns the next object without consuming it; ok is false at the end of the listing
// (check Err to tell the end from a failure).
func (k *keyStream) peek() (storage.ObjectInfo, bool) {
	for k.pos >= len(k.cur) {
		page, open := <-k.pages
		if !open {
			return storage.ObjectInfo{}, false
		}
		k.cur, k.pos = page, 0
	}
	return k.cur[k.pos], true
}

func (k *keyStream) advance() { k.pos++ }

// Err is the listing error, if any. It is only meaningful after peek has reported the end.
func (k *keyStream) Err() error { return k.err }

// Close stops the lister; it must be called when the stream is abandoned early.
func (k *keyStream) Close() {
	k.cancel()
	for range k.pages { // let the lister goroutine finish
	}
}

// mergeKeys walks two listings, sorted by key as every S3 listing is, and reports the keys that
// exist on one side only, and the pairs present on both. Memory use is independent of bucket size.
func mergeKeys(a, b *keyStream, onlyA, onlyB func(storage.ObjectInfo) error, both func(a, b storage.ObjectInfo) error) error {
	for {
		ao, aok := a.peek()
		bo, bok := b.peek()
		if err := firstErr(a, aok, b, bok); err != nil {
			return err
		}
		switch {
		case !aok && !bok:
			return nil
		case !bok || (aok && ao.Key < bo.Key):
			if err := onlyA(ao); err != nil {
				return err
			}
			a.advance()
		case !aok || bo.Key < ao.Key:
			if err := onlyB(bo); err != nil {
				return err
			}
			b.advance()
		default:
			if err := both(ao, bo); err != nil {
				return err
			}
			a.advance()
			b.advance()
		}
	}
}

func firstErr(a *keyStream, aok bool, b *keyStream, bok bool) error {
	if !aok && a.Err() != nil {
		return a.Err()
	}
	if !bok && b.Err() != nil {
		return b.Err()
	}
	return nil
}

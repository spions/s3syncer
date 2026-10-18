package queue

import (
	"context"
	"sync"
)

// MemoryLocker is the in-process KeyLocker, used with the single-node SQLite store.
type MemoryLocker struct {
	held sync.Map
}

func NewMemoryLocker() *MemoryLocker { return &MemoryLocker{} }

func (l *MemoryLocker) TryLock(_ context.Context, key string) (func(), bool, error) {
	if _, loaded := l.held.LoadOrStore(key, struct{}{}); loaded {
		return nil, false, nil
	}
	var once sync.Once
	return func() { once.Do(func() { l.held.Delete(key) }) }, true, nil
}

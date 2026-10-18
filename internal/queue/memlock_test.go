package queue

import "testing"

func TestMemoryLockerContract(t *testing.T) {
	runLockContract(t, func(t *testing.T) lockHarness {
		l := NewMemoryLocker()
		return lockHarness{A: l, B: l} // no TTL: nothing to expire
	})
}

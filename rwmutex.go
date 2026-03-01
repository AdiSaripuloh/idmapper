package idmapper

import "sync"

// rwMutexSize is the size of sync.RWMutex in bytes.
// 24 bytes on all current Go platforms (go1.21+).
const rwMutexSize = 24

// RWMutex is a Mapper backed by sync.RWMutex.
// Reads acquire a shared lock; writes acquire an exclusive lock.
// Suitable for general use when updates happen regularly.
//
// mu is padded to its own cache line to prevent false sharing with the data
// fields (m, next). Without padding, a writer acquiring mu would dirty the
// cache line that concurrent readers also need to access m, forcing a cache
// miss on every contested read.
type RWMutex[K comparable] struct {
	mu sync.RWMutex
	// sync.RWMutex is 24 bytes; pad to a full 64-byte cache line so that m
	// and next land on a separate line from the lock state.
	_ [cacheLineSize - rwMutexSize%cacheLineSize]byte

	m    map[K]uint64
	next uint64
}

// NewRWMutex returns a RWMutex mapper pre-populated with keys.
// Keys are assigned sequential IDs starting from 1 in slice order.
// Duplicates are ignored; the first occurrence wins.
func NewRWMutex[K comparable](keys []K) *RWMutex[K] {
	r := &RWMutex[K]{
		m:    make(map[K]uint64, len(keys)),
		next: 1,
	}
	for i := 0; i < len(keys); i++ {
		if _, ok := r.m[keys[i]]; !ok {
			r.m[keys[i]] = r.next
			r.next++
		}
	}
	return r
}

// Set registers key if not already present and returns its ID.
// Safe for concurrent use.
func (r *RWMutex[K]) Set(key K) uint64 {
	// Fast path: key already exists — shared read lock only.
	r.mu.RLock()
	if id, ok := r.m[key]; ok {
		r.mu.RUnlock()
		return id
	}
	r.mu.RUnlock()

	// Slow path: upgrade to write lock.
	r.mu.Lock()
	// Re-check: another goroutine may have inserted the key between the two locks.
	if id, ok := r.m[key]; ok {
		r.mu.Unlock()
		return id
	}
	id := r.next
	r.m[key] = id
	r.next++
	r.mu.Unlock()
	return id
}

// Sets registers each key that is not already present and returns the IDs in
// input order. The read lock is acquired once to check all keys; the write
// lock is only acquired if at least one key is absent.
// Safe for concurrent use.
func (r *RWMutex[K]) Sets(keys []K) []uint64 {
	ids := make([]uint64, len(keys))

	// Fast path: all keys may already exist — one shared read lock.
	r.mu.RLock()
	missing := false
	for i := 0; i < len(keys); i++ {
		ids[i] = r.m[keys[i]] // 0 if absent
		if ids[i] == 0 {
			missing = true
		}
	}
	r.mu.RUnlock()

	if !missing {
		return ids
	}

	// Slow path: acquire write lock once for the whole batch.
	r.mu.Lock()
	for i := 0; i < len(keys); i++ {
		if id, ok := r.m[keys[i]]; ok {
			ids[i] = id
		} else {
			ids[i] = r.next
			r.m[keys[i]] = r.next
			r.next++
		}
	}
	r.mu.Unlock()
	return ids
}

// Get returns the ID for key and whether it was found.
// Safe for concurrent use.
func (r *RWMutex[K]) Get(key K) (uint64, bool) {
	r.mu.RLock()
	id, ok := r.m[key]
	r.mu.RUnlock()
	return id, ok
}

// Gets returns the IDs for each key in input order; 0 for any key not found.
// The read lock is acquired once for the entire batch.
// Safe for concurrent use.
func (r *RWMutex[K]) Gets(keys []K) []uint64 {
	ids := make([]uint64, len(keys))
	r.mu.RLock()
	for i := 0; i < len(keys); i++ {
		ids[i] = r.m[keys[i]]
	}
	r.mu.RUnlock()
	return ids
}

// Len returns the number of keys registered in the mapper.
// Safe for concurrent use.
func (r *RWMutex[K]) Len() int {
	r.mu.RLock()
	n := len(r.m)
	r.mu.RUnlock()
	return n
}

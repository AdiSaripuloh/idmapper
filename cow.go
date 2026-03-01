package idmapper

import (
	"sync"
	"sync/atomic"
)

// COW is a Mapper that uses copy-on-write for updates.
// Reads are lock-free: they atomically load a pointer to the current snapshot.
// Writes serialize via a mutex, copy the snapshot, apply the change,
// and atomically swap in the new pointer.
//
// Best suited for high read-throughput workloads with infrequent writes.
//
// ptr is isolated on its own cache line so that writers locking mu never
// dirty the cache line that readers are simultaneously loading from ptr.
// Without this separation, every write acquisition of mu would invalidate
// the cache line for all concurrent readers.
type COW[K comparable] struct {
	// readers' hot path — own cache line.
	// atomic.Pointer is 8 bytes; 8 + 56 = 64 bytes (one full cache line).
	ptr atomic.Pointer[map[K]uint64]
	_   [cacheLineSize - 8]byte

	// writers only — separate cache line.
	mu   sync.Mutex
	next uint64 // protected by mu
}

// NewCOW returns a COW mapper pre-populated with keys.
// Keys are assigned sequential IDs starting from 1 in slice order.
// Duplicates are ignored; the first occurrence wins.
func NewCOW[K comparable](keys []K) *COW[K] {
	c := &COW[K]{next: 1}
	m := make(map[K]uint64, len(keys))
	for i := 0; i < len(keys); i++ {
		if _, ok := m[keys[i]]; !ok {
			m[keys[i]] = c.next
			c.next++
		}
	}
	c.ptr.Store(&m)
	return c
}

// Set registers key if not already present and returns its ID.
// Writes serialize via a mutex; a full map copy is made on each new key.
// Safe for concurrent use.
func (c *COW[K]) Set(key K) uint64 {
	// Fast path: lock-free check on the current snapshot.
	if snap := c.ptr.Load(); snap != nil {
		if id, ok := (*snap)[key]; ok {
			return id
		}
	}

	c.mu.Lock()

	// Re-check under the write lock: another goroutine may have raced us.
	old := c.ptr.Load()
	var oldMap map[K]uint64
	if old != nil {
		oldMap = *old
		if id, ok := oldMap[key]; ok {
			// Manual unlock (not defer): write path is perf-sensitive and
			// control flow is simple with no early returns after lock.
			c.mu.Unlock()
			return id
		}
	}

	// Copy the current snapshot and insert the new key.
	newMap := make(map[K]uint64, len(oldMap)+1)
	for k, v := range oldMap {
		newMap[k] = v
	}
	id := c.next
	newMap[key] = id
	c.next++
	c.ptr.Store(&newMap)
	// Manual unlock (not defer): write path is perf-sensitive and
	// control flow is simple with no early returns after lock.
	c.mu.Unlock()
	return id
}

// Sets registers each key that is not already present and returns the IDs in
// input order. Unlike calling Set in a loop, the entire batch of new keys is
// inserted with a single map copy, regardless of how many new keys there are.
// Safe for concurrent use.
func (c *COW[K]) Sets(keys []K) []uint64 {
	ids := make([]uint64, len(keys))

	// Fast path: all keys may already exist — one lock-free snapshot load.
	if snap := c.ptr.Load(); snap != nil {
		m := *snap
		missing := false
		for i := 0; i < len(keys); i++ {
			ids[i] = m[keys[i]] // 0 if absent
			if ids[i] == 0 {
				missing = true
			}
		}
		if !missing {
			return ids
		}
	}

	c.mu.Lock()

	// Re-check under the write lock.
	old := c.ptr.Load()
	var oldMap map[K]uint64
	if old != nil {
		oldMap = *old
	}

	// Count how many keys are genuinely absent.
	anyNew := false
	for i := 0; i < len(keys); i++ {
		if _, ok := oldMap[keys[i]]; !ok {
			anyNew = true
			break
		}
	}

	if !anyNew {
		// Another goroutine inserted all keys between the fast path and the lock.
		for i := 0; i < len(keys); i++ {
			ids[i] = oldMap[keys[i]]
		}
		// Manual unlock (not defer): write path is perf-sensitive and
		// control flow is simple with no early returns after lock.
		c.mu.Unlock()
		return ids
	}

	// One copy for the whole batch — the key advantage over N individual Sets.
	newMap := make(map[K]uint64, len(oldMap)+len(keys))
	for k, v := range oldMap {
		newMap[k] = v
	}
	for i := 0; i < len(keys); i++ {
		if id, ok := newMap[keys[i]]; ok {
			ids[i] = id
			continue
		}
		ids[i] = c.next
		newMap[keys[i]] = c.next
		c.next++
	}
	c.ptr.Store(&newMap)
	// Manual unlock (not defer): write path is perf-sensitive and
	// control flow is simple with no early returns after lock.
	c.mu.Unlock()
	return ids
}

// GetSnapshot returns a shallow copy of the current map as a point-in-time
// snapshot. The returned map is safe to read and mutate without affecting
// the mapper. Always returns a non-nil map (empty if no keys are registered).
// Safe for concurrent use.
func (c *COW[K]) GetSnapshot() map[K]uint64 {
	snap := c.ptr.Load()
	if snap == nil {
		return make(map[K]uint64)
	}
	src := *snap
	dst := make(map[K]uint64, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// Get returns the ID for key and whether it was found.
// Lock-free: atomically loads the current snapshot and performs a map lookup.
// Safe for concurrent use.
func (c *COW[K]) Get(key K) (uint64, bool) {
	snap := c.ptr.Load()
	if snap == nil {
		return 0, false
	}
	id, ok := (*snap)[key]
	return id, ok
}

// Gets returns the IDs for each key in input order; 0 for any key not found.
// Lock-free: atomically loads the snapshot once for the entire batch.
// Safe for concurrent use.
func (c *COW[K]) Gets(keys []K) []uint64 {
	ids := make([]uint64, len(keys))
	snap := c.ptr.Load()
	if snap == nil {
		return ids
	}
	m := *snap
	for i := 0; i < len(keys); i++ {
		ids[i] = m[keys[i]]
	}
	return ids
}

// Len returns the number of keys registered in the mapper.
// Lock-free: atomically loads the current snapshot.
// Safe for concurrent use.
func (c *COW[K]) Len() int {
	snap := c.ptr.Load()
	if snap == nil {
		return 0
	}
	return len(*snap)
}

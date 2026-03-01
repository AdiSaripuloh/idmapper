package idmapper

import "sync/atomic"

// Freeze is a Mapper that allows writes only during a build phase.
// Once Freeze is called, the map is immutable and Get requires no locking.
//
// Intended usage:
//
//	f := NewFreeze(keys)  // or build incrementally with Set
//	f.Freeze()            // seal the map
//	// share f across goroutines — Get is lock-free from here on
//
// The build phase (before Freeze) is not safe for concurrent use.
// Set panics if called after Freeze.
type Freeze[K comparable] struct {
	m      map[K]uint64
	next   uint64
	frozen atomic.Bool
}

// NewFreeze returns a Freeze mapper pre-populated with keys.
// Keys are assigned sequential IDs starting from 1 in slice order.
// Duplicates are ignored; the first occurrence wins.
func NewFreeze[K comparable](keys []K) *Freeze[K] {
	f := &Freeze[K]{
		m:    make(map[K]uint64, len(keys)),
		next: 1,
	}
	for i := 0; i < len(keys); i++ {
		if _, ok := f.m[keys[i]]; !ok {
			f.m[keys[i]] = f.next
			f.next++
		}
	}
	return f
}

// Freeze seals the mapper. After this call, Set panics and Get is lock-free.
// Must be called from the goroutine that performed all Set calls,
// and before the mapper is shared with other goroutines.
func (f *Freeze[K]) Freeze() {
	f.frozen.Store(true)
}

// Set registers key if not already present and returns its ID.
// Panics if called after Freeze. Not safe for concurrent use during the build phase.
func (f *Freeze[K]) Set(key K) uint64 {
	if f.frozen.Load() {
		panic("idmapper: Set called on a frozen Freeze mapper")
	}
	if id, ok := f.m[key]; ok {
		return id
	}
	id := f.next
	f.m[key] = id
	f.next++
	return id
}

// Sets registers each key that is not already present and returns the IDs in
// input order. Not safe for concurrent use during the build phase.
// Panics if called after Freeze.
func (f *Freeze[K]) Sets(keys []K) []uint64 {
	if f.frozen.Load() {
		panic("idmapper: Sets called on a frozen Freeze mapper")
	}
	ids := make([]uint64, len(keys))
	for i := 0; i < len(keys); i++ {
		if id, ok := f.m[keys[i]]; ok {
			ids[i] = id
		} else {
			ids[i] = f.next
			f.m[keys[i]] = f.next
			f.next++
		}
	}
	return ids
}

// Get returns the ID for key and whether it was found.
// Lock-free after Freeze has been called.
//
// The caller must establish a happens-before relationship between Freeze and
// any concurrent Get calls — for example, by starting reader goroutines after
// Freeze returns, or by using a sync.WaitGroup or channel.
func (f *Freeze[K]) Get(key K) (uint64, bool) {
	id, ok := f.m[key]
	return id, ok
}

// Gets returns the IDs for each key in input order; 0 for any key not found.
// Lock-free after Freeze has been called.
// See Get for happens-before requirements.
func (f *Freeze[K]) Gets(keys []K) []uint64 {
	ids := make([]uint64, len(keys))
	for i := 0; i < len(keys); i++ {
		ids[i] = f.m[keys[i]]
	}
	return ids
}

// Len returns the number of keys registered in the mapper.
// Lock-free after Freeze has been called. See Get for happens-before requirements.
func (f *Freeze[K]) Len() int {
	return len(f.m)
}

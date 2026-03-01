package idmapper_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/AdiSaripuloh/idmapper"
)

func TestRWMutex_Behavior(t *testing.T) {
	testBehavior(t, idmapper.NewRWMutex([]string{"a", "b", "c"}))
}

func TestRWMutex_BatchBehavior(t *testing.T) {
	testBatchBehavior(t, idmapper.NewRWMutex[string](nil))
}

// TestRWMutex_ConcurrentReadWrite runs concurrent writers and readers and
// verifies every written key is visible after all goroutines complete.
func TestRWMutex_ConcurrentReadWrite(t *testing.T) {
	const n = 200
	m := idmapper.NewRWMutex[string](nil)
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m.Set(fmt.Sprintf("writer-%d", i))
		}(i)
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m.Get(fmt.Sprintf("writer-%d", i)) // may race with writers, that is fine
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("writer-%d", i)
		if _, ok := m.Get(key); !ok {
			t.Errorf("%q not found after concurrent writes", key)
		}
	}
}

// TestRWMutex_ConcurrentSameKey verifies the double-checked locking path: when
// many goroutines race to Set the same brand-new key, every one of them must
// receive ID 1 and only a single ID should be assigned.
func TestRWMutex_ConcurrentSameKey(t *testing.T) {
	const goroutines = 500
	m := idmapper.NewRWMutex[string](nil)
	results := make([]uint64, goroutines)
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = m.Set("shared")
		}(i)
	}
	wg.Wait()

	for i := 0; i < len(results); i++ {
		if results[i] != 1 {
			t.Errorf("goroutine %d: got ID %d, want 1", i, results[i])
		}
	}
}

// TestRWMutex_UniqueIDs verifies that concurrent inserts of distinct keys
// produce IDs that are unique and form the contiguous range [1, n].
func TestRWMutex_UniqueIDs(t *testing.T) {
	const n = 500
	m := idmapper.NewRWMutex[string](nil)
	results := make([]uint64, n)
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = m.Set(fmt.Sprintf("key-%d", i))
		}(i)
	}
	wg.Wait()

	seen := make(map[uint64]bool, n)
	for i := 0; i < len(results); i++ {
		if seen[results[i]] {
			t.Errorf("duplicate ID %d", results[i])
		}
		seen[results[i]] = true
	}
	for id := uint64(1); id <= n; id++ {
		if !seen[id] {
			t.Errorf("ID %d missing from the assigned range [1, %d]", id, n)
		}
	}
}

// TestRWMutex_SetsConcurrent verifies that concurrent batch Sets produce IDs
// that are unique and contiguous with no duplicates across goroutines.
func TestRWMutex_SetsConcurrent(t *testing.T) {
	const goroutines = 20
	const batchSize = 25 // total unique keys = 500

	m := idmapper.NewRWMutex[string](nil)
	collected := make([][]uint64, goroutines)
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			keys := make([]string, batchSize)
			for i := 0; i < len(keys); i++ {
				keys[i] = fmt.Sprintf("g%d-k%d", g, i)
			}
			collected[g] = m.Sets(keys)
		}(g)
	}
	wg.Wait()

	seen := make(map[uint64]bool, goroutines*batchSize)
	for g := 0; g < len(collected); g++ {
		for j := 0; j < len(collected[g]); j++ {
			if seen[collected[g][j]] {
				t.Errorf("duplicate ID %d across concurrent Sets", collected[g][j])
			}
			seen[collected[g][j]] = true
		}
	}
	total := uint64(goroutines * batchSize)
	for id := uint64(1); id <= total; id++ {
		if !seen[id] {
			t.Errorf("ID %d missing from the assigned range [1, %d]", id, total)
		}
	}
}

// -- Benchmarks ---------------------------------------------------------------

// benchBatch is the number of keys per Gets/Sets call in batch benchmarks.
const benchBatch = 10

func BenchmarkGet_RWMutex(b *testing.B) {
	m := idmapper.NewRWMutex(benchKeys)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.Get(benchKeys[i%len(benchKeys)])
	}
}

func BenchmarkGetParallel_RWMutex(b *testing.B) {
	m := idmapper.NewRWMutex(benchKeys)
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Get(benchKeys[i%len(benchKeys)])
			i++
		}
	})
}

// BenchmarkSetExisting_RWMutex measures the fast path: key already present,
// only a read lock is acquired.
func BenchmarkSetExisting_RWMutex(b *testing.B) {
	m := idmapper.NewRWMutex(benchKeys)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.Set(benchKeys[i%len(benchKeys)])
	}
}

func BenchmarkSetExistingParallel_RWMutex(b *testing.B) {
	m := idmapper.NewRWMutex(benchKeys)
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Set(benchKeys[i%len(benchKeys)])
			i++
		}
	})
}

// BenchmarkSetNew_RWMutex measures the slow path: key is absent, a write lock
// is acquired, and the map grows. The mapper is reset each time the pool is
// exhausted so that every iteration genuinely exercises the slow path.
func BenchmarkSetNew_RWMutex(b *testing.B) {
	b.ReportAllocs()
	m := idmapper.NewRWMutex[string](nil)
	pool := benchKeys
	idx := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if idx == len(pool) {
			b.StopTimer()
			m = idmapper.NewRWMutex[string](nil)
			idx = 0
			b.StartTimer()
		}
		m.Set(pool[idx])
		idx++
	}
}

// BenchmarkNew_RWMutex measures the cost of constructing a pre-populated mapper.
func BenchmarkNew_RWMutex(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		idmapper.NewRWMutex(benchKeys)
	}
}

// BenchmarkGets_RWMutex measures a batch Get of benchBatch keys under one read lock.
func BenchmarkGets_RWMutex(b *testing.B) {
	m := idmapper.NewRWMutex(benchKeys)
	batch := benchKeys[:benchBatch]
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.Gets(batch)
	}
}

func BenchmarkGetsParallel_RWMutex(b *testing.B) {
	m := idmapper.NewRWMutex(benchKeys)
	batch := benchKeys[:benchBatch]
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Gets(batch)
		}
	})
}

// BenchmarkSetsExisting_RWMutex measures the fast path of Sets (all keys present).
func BenchmarkSetsExisting_RWMutex(b *testing.B) {
	m := idmapper.NewRWMutex(benchKeys)
	batch := benchKeys[:benchBatch]
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.Sets(batch)
	}
}

// BenchmarkSetsNew_RWMutex measures the slow path of Sets (all keys are new).
// The mapper is reset each time the pool is exhausted.
func BenchmarkSetsNew_RWMutex(b *testing.B) {
	b.ReportAllocs()
	m := idmapper.NewRWMutex[string](nil)
	pool := benchKeys
	idx := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		end := idx + benchBatch
		if end > len(pool) {
			b.StopTimer()
			m = idmapper.NewRWMutex[string](nil)
			idx = 0
			end = benchBatch
			b.StartTimer()
		}
		m.Sets(pool[idx:end])
		idx = end
	}
}

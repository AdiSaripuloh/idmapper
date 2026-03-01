package idmapper_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/AdiSaripuloh/idmapper"
)

func TestCOW_Behavior(t *testing.T) {
	testBehavior(t, idmapper.NewCOW([]string{"a", "b", "c"}))
}

func TestCOW_BatchBehavior(t *testing.T) {
	testBatchBehavior(t, idmapper.NewCOW[string](nil))
}

// TestCOW_SetsOneCopy verifies that Sets inserts N new keys with a single map
// copy rather than N copies. We confirm this indirectly by checking that the
// resulting snapshot contains exactly the expected keys and IDs.
func TestCOW_SetsOneCopy(t *testing.T) {
	m := idmapper.NewCOW[string](nil)

	keys := []string{"x", "y", "z"}
	ids := m.Sets(keys)

	snap := m.GetSnapshot()
	for i := 0; i < len(keys); i++ {
		if snap[keys[i]] != ids[i] {
			t.Errorf("snapshot[%q] = %d, want %d", keys[i], snap[keys[i]], ids[i])
		}
	}
	if len(snap) != len(keys) {
		t.Errorf("snapshot has %d entries, want %d", len(snap), len(keys))
	}
}

// TestCOW_SetsConcurrent verifies that concurrent batch Sets produce unique,
// contiguous IDs with no duplicates across goroutines.
func TestCOW_SetsConcurrent(t *testing.T) {
	const goroutines = 20
	const batchSize = 25 // total unique keys = 500

	m := idmapper.NewCOW[string](nil)
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

// TestCOW_ConcurrentReadWrite runs concurrent writers and readers and verifies
// every written key is visible after all goroutines complete.
func TestCOW_ConcurrentReadWrite(t *testing.T) {
	const n = 200
	m := idmapper.NewCOW[string](nil)
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
			m.Get(fmt.Sprintf("writer-%d", i)) // may see old snapshot, that is fine
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

// TestCOW_ConcurrentSameKey verifies that when many goroutines race to Set the
// same brand-new key, every caller receives ID 1 and only a single ID is ever
// assigned. This exercises the double-checked locking inside the write path.
func TestCOW_ConcurrentSameKey(t *testing.T) {
	const goroutines = 500
	m := idmapper.NewCOW[string](nil)
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

// TestCOW_UniqueIDs verifies that concurrent inserts of distinct keys produce
// IDs that are unique and form the contiguous range [1, n].
func TestCOW_UniqueIDs(t *testing.T) {
	const n = 500
	m := idmapper.NewCOW[string](nil)
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

// TestCOW_SnapshotConsistency verifies that a single atomic snapshot load is
// internally consistent: if the snapshot contains key K with ID n, then every
// key with ID < n that was written before K must also be present in that same
// snapshot. Because COW serialises writers and each new snapshot is a full copy
// of the previous one, partial state can never be observed.
func TestCOW_SnapshotConsistency(t *testing.T) {
	// Sequentially write n keys so that keys[i] always gets ID i+1.
	const n = 100
	keys := make([]string, n)
	m := idmapper.NewCOW[string](nil)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("k-%d", i)
		m.Set(keys[i])
	}

	// Spin up writers adding extra keys concurrently.
	var wg sync.WaitGroup
	for i := n; i < n+50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m.Set(fmt.Sprintf("k-%d", i))
		}(i)
	}

	// Load one atomic snapshot. Every key[i] visible in it must have
	// id == i+1, and every predecessor key[j] (j < i) must also be present.
	wg.Add(1)
	go func() {
		defer wg.Done()
		snap := m.GetSnapshot() // single atomic load
		for i := 0; i < len(keys); i++ {
			id, ok := snap[keys[i]]
			if !ok {
				continue // not in this snapshot yet — fine
			}
			if want := uint64(i + 1); id != want {
				t.Errorf("snapshot: %q has id=%d, want %d", keys[i], id, want)
			}
			// All predecessors must also be present.
			for j := 0; j < i; j++ {
				if _, found := snap[keys[j]]; !found {
					t.Errorf("snapshot has %q (id=%d) but is missing predecessor %q (id=%d)",
						keys[i], id, keys[j], uint64(j+1))
				}
			}
		}
	}()
	wg.Wait()
}

// -- Benchmarks ---------------------------------------------------------------

func BenchmarkGet_COW(b *testing.B) {
	m := idmapper.NewCOW(benchKeys)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.Get(benchKeys[i%len(benchKeys)])
	}
}

func BenchmarkGetParallel_COW(b *testing.B) {
	m := idmapper.NewCOW(benchKeys)
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

// BenchmarkSetExisting_COW measures the fast path: key is present, only a
// lock-free atomic load + map lookup is performed.
func BenchmarkSetExisting_COW(b *testing.B) {
	m := idmapper.NewCOW(benchKeys)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.Set(benchKeys[i%len(benchKeys)])
	}
}

func BenchmarkSetExistingParallel_COW(b *testing.B) {
	m := idmapper.NewCOW(benchKeys)
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

// BenchmarkSetNew_COW measures the slow path: key is absent, so the mapper
// acquires a mutex, copies the entire map, and atomically swaps the pointer.
// The mapper is reset each time the pool is exhausted so every iteration
// genuinely exercises the copy path.
func BenchmarkSetNew_COW(b *testing.B) {
	b.ReportAllocs()
	m := idmapper.NewCOW[string](nil)
	pool := benchKeys
	idx := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if idx == len(pool) {
			b.StopTimer()
			m = idmapper.NewCOW[string](nil)
			idx = 0
			b.StartTimer()
		}
		m.Set(pool[idx])
		idx++
	}
}

// BenchmarkNew_COW measures the cost of constructing a pre-populated mapper.
func BenchmarkNew_COW(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		idmapper.NewCOW(benchKeys)
	}
}

// BenchmarkGets_COW measures a batch Get of benchBatch keys under one atomic load.
func BenchmarkGets_COW(b *testing.B) {
	m := idmapper.NewCOW(benchKeys)
	batch := benchKeys[:benchBatch]
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.Gets(batch)
	}
}

func BenchmarkGetsParallel_COW(b *testing.B) {
	m := idmapper.NewCOW(benchKeys)
	batch := benchKeys[:benchBatch]
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Gets(batch)
		}
	})
}

// BenchmarkSetsExisting_COW measures the fast path of Sets (all keys present).
func BenchmarkSetsExisting_COW(b *testing.B) {
	m := idmapper.NewCOW(benchKeys)
	batch := benchKeys[:benchBatch]
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.Sets(batch)
	}
}

// BenchmarkSetsNew_COW measures the slow path of Sets (all keys are new).
// The entire batch is inserted with a single map copy — the core advantage of
// Sets over calling Set in a loop for COW.
func BenchmarkSetsNew_COW(b *testing.B) {
	b.ReportAllocs()
	m := idmapper.NewCOW[string](nil)
	pool := benchKeys
	idx := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		end := idx + benchBatch
		if end > len(pool) {
			b.StopTimer()
			m = idmapper.NewCOW[string](nil)
			idx = 0
			end = benchBatch
			b.StartTimer()
		}
		m.Sets(pool[idx:end])
		idx = end
	}
}

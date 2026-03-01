package idmapper_test

import (
	"sync"
	"testing"

	"github.com/AdiSaripuloh/idmapper"
)

func TestFreeze_Behavior(t *testing.T) {
	testBehavior(t, idmapper.NewFreeze([]string{"a", "b", "c"}))
}

func TestFreeze_BatchBehavior(t *testing.T) {
	testBatchBehavior(t, idmapper.NewFreeze[string](nil))
}

// TestFreeze_BuildPhase verifies that Set can be called incrementally during
// the build phase, assigning IDs in call order and deduplicating repeats.
func TestFreeze_BuildPhase(t *testing.T) {
	m := idmapper.NewFreeze[string](nil)

	tests := []struct {
		key    string
		wantID uint64
	}{
		{"x", 1},
		{"y", 2},
		{"x", 1}, // duplicate — same ID as first call
		{"z", 3},
	}

	got := make([]uint64, len(tests))
	for i := 0; i < len(tests); i++ {
		got[i] = m.Set(tests[i].key)
	}
	m.Freeze()

	for i := 0; i < len(tests); i++ {
		if got[i] != tests[i].wantID {
			t.Errorf("call %d Set(%q): got %d, want %d", i, tests[i].key, got[i], tests[i].wantID)
		}
	}
}

// TestFreeze_PanicsAfterFreeze verifies that both Set and Sets panic once
// the mapper is sealed.
func TestFreeze_PanicsAfterFreeze(t *testing.T) {
	tests := []struct {
		name string
		call func(m *idmapper.Freeze[string])
	}{
		{"Set", func(m *idmapper.Freeze[string]) { m.Set("b") }},
		{"Sets", func(m *idmapper.Freeze[string]) { m.Sets([]string{"a", "b"}) }},
	}
	for i := 0; i < len(tests); i++ {
		tc := tests[i]
		t.Run(tc.name, func(t *testing.T) {
			m := idmapper.NewFreeze([]string{"a"})
			m.Freeze()
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("expected panic on %s after Freeze, got none", tc.name)
				}
			}()
			tc.call(m)
		})
	}
}

// TestFreeze_GetMissingAfterFreeze verifies that Get returns (0, false) for
// unknown keys after the mapper has been frozen.
func TestFreeze_GetMissingAfterFreeze(t *testing.T) {
	m := idmapper.NewFreeze([]string{"a", "b"})
	m.Freeze()

	tests := []struct {
		key string
	}{
		{"missing"},
		{"c"},
		{""}, // empty string was never registered
	}
	for i := 0; i < len(tests); i++ {
		if id, ok := m.Get(tests[i].key); ok || id != 0 {
			t.Errorf("Get(%q) after Freeze: got (%d, %v), want (0, false)", tests[i].key, id, ok)
		}
	}
}

// TestFreeze_GetAllKeysAfterFreeze verifies that all pre-populated keys return
// correct IDs after the mapper has been frozen.
func TestFreeze_GetAllKeysAfterFreeze(t *testing.T) {
	keys := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	m := idmapper.NewFreeze(keys)
	m.Set("zeta")
	m.Sets([]string{"eta", "theta"})
	m.Freeze()

	for i := 0; i < len(keys); i++ {
		id, ok := m.Get(keys[i])
		if !ok {
			t.Errorf("Get(%q): not found after Freeze", keys[i])
		}
		if want := uint64(i + 1); id != want {
			t.Errorf("Get(%q): got %d, want %d", keys[i], id, want)
		}
	}
	// Keys added via Set/Sets after construction.
	extra := []struct {
		key    string
		wantID uint64
	}{
		{"zeta", 6},
		{"eta", 7},
		{"theta", 8},
	}
	for _, tc := range extra {
		id, ok := m.Get(tc.key)
		if !ok || id != tc.wantID {
			t.Errorf("Get(%q): got (%d, %v), want (%d, true)", tc.key, id, ok, tc.wantID)
		}
	}
	if got := m.Len(); got != 8 {
		t.Errorf("Len(): got %d, want 8", got)
	}
}

// TestFreeze_DoubleFreezeIdempotent verifies that calling Freeze a second time
// does not panic and leaves the mapper in a consistent state.
func TestFreeze_DoubleFreezeIdempotent(t *testing.T) {
	m := idmapper.NewFreeze([]string{"a", "b"})
	m.Freeze()
	m.Freeze() // must not panic

	if id, ok := m.Get("a"); !ok || id != 1 {
		t.Errorf("Get(%q) after double Freeze: got (%d, %v), want (1, true)", "a", id, ok)
	}
}

// TestFreeze_ConcurrentReadsAfterFreeze spawns many goroutines that all call
// Get simultaneously after sealing. The -race detector will surface any data
// races introduced by the lock-free read path.
func TestFreeze_ConcurrentReadsAfterFreeze(t *testing.T) {
	m := idmapper.NewFreeze([]string{"a", "b", "c"})
	m.Freeze()

	const goroutines = 500
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if id, ok := m.Get("b"); !ok || id != 2 {
				t.Errorf("Get(%q): got (%d, %v), want (2, true)", "b", id, ok)
			}
		}()
	}
	wg.Wait()
}

// -- Benchmarks ---------------------------------------------------------------

func BenchmarkGet_Freeze(b *testing.B) {
	m := idmapper.NewFreeze(benchKeys)
	m.Freeze()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.Get(benchKeys[i%len(benchKeys)])
	}
}

func BenchmarkGetParallel_Freeze(b *testing.B) {
	m := idmapper.NewFreeze(benchKeys)
	m.Freeze()
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

// BenchmarkNew_Freeze measures the cost of constructing and sealing a mapper.
func BenchmarkNew_Freeze(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m := idmapper.NewFreeze(benchKeys)
		m.Freeze()
	}
}

// BenchmarkGets_Freeze measures a batch Get of benchBatch keys with no locking.
func BenchmarkGets_Freeze(b *testing.B) {
	m := idmapper.NewFreeze(benchKeys)
	m.Freeze()
	batch := benchKeys[:benchBatch]
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.Gets(batch)
	}
}

func BenchmarkGetsParallel_Freeze(b *testing.B) {
	m := idmapper.NewFreeze(benchKeys)
	m.Freeze()
	batch := benchKeys[:benchBatch]
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Gets(batch)
		}
	})
}

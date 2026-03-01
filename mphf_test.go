package idmapper_test

import (
	"sync"
	"testing"

	"github.com/AdiSaripuloh/idmapper"
)

func TestMPHF_Behavior(t *testing.T) {
	testBehavior(t, idmapper.NewMPHF([]string{"a", "b", "c"}, idmapper.StringHasher))
}

func TestMPHF_BatchBehavior(t *testing.T) {
	testBatchBehavior(t, idmapper.NewMPHF[string](nil, idmapper.StringHasher))
}

// TestMPHF_BuildPhase verifies that Set can be called incrementally during
// the build phase, assigning IDs in call order and deduplicating repeats.
func TestMPHF_BuildPhase(t *testing.T) {
	m := idmapper.NewMPHF[string](nil, idmapper.StringHasher)

	tests := []struct {
		key    string
		wantID uint64
	}{
		{"x", 1},
		{"y", 2},
		{"x", 1}, // duplicate — returns existing ID
		{"z", 3},
	}
	got := make([]uint64, len(tests))
	for i := 0; i < len(tests); i++ {
		got[i] = m.Set(tests[i].key)
	}
	m.Freeze()

	for i := 0; i < len(tests); i++ {
		if got[i] != tests[i].wantID {
			t.Errorf("Set(%q): got %d, want %d", tests[i].key, got[i], tests[i].wantID)
		}
	}
}

// TestMPHF_FrozenGet verifies that Get and Gets return correct insertion-order
// IDs via the MPHF path (after Freeze) for every registered key.
func TestMPHF_FrozenGet(t *testing.T) {
	keys := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	m := idmapper.NewMPHF(keys, idmapper.StringHasher)
	m.Freeze()

	for i := 0; i < len(keys); i++ {
		want := uint64(i + 1)
		id, ok := m.Get(keys[i])
		if !ok {
			t.Errorf("Get(%q): not found after Freeze", keys[i])
		}
		if id != want {
			t.Errorf("Get(%q): got %d, want %d", keys[i], id, want)
		}
	}

	// Batch path.
	ids := m.Gets(keys)
	for i := 0; i < len(ids); i++ {
		if want := uint64(i + 1); ids[i] != want {
			t.Errorf("Gets[%d] (%q): got %d, want %d", i, keys[i], ids[i], want)
		}
	}
}

// TestMPHF_PanicsAfterFreeze verifies that both Set and Sets panic once
// the mapper is sealed.
func TestMPHF_PanicsAfterFreeze(t *testing.T) {
	tests := []struct {
		name string
		call func(m *idmapper.MPHF[string])
	}{
		{"Set", func(m *idmapper.MPHF[string]) { m.Set("b") }},
		{"Sets", func(m *idmapper.MPHF[string]) { m.Sets([]string{"a", "b"}) }},
	}
	for i := 0; i < len(tests); i++ {
		tc := tests[i]
		t.Run(tc.name, func(t *testing.T) {
			m := idmapper.NewMPHF([]string{"a"}, idmapper.StringHasher)
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

// TestMPHF_GetMissingAfterFreeze verifies that Get returns (0, false) for
// unknown keys after the mapper has been frozen.
func TestMPHF_GetMissingAfterFreeze(t *testing.T) {
	m := idmapper.NewMPHF([]string{"a", "b"}, idmapper.StringHasher)
	m.Freeze()

	tests := []struct{ key string }{
		{"missing"},
		{"c"},
		{""}, // empty string was never registered
	}
	for i := 0; i < len(tests); i++ {
		if id, ok := m.Get(tests[i].key); ok || id != 0 {
			t.Errorf("Get(%q) after Freeze: got (%d, %v), want (0, false)",
				tests[i].key, id, ok)
		}
	}
}

// TestMPHF_ZeroValueKeyNotFound verifies that Get returns (0, false) for the
// zero value of K when it was never inserted. This guards against false
// positives on unoccupied MPHF slots whose zero-value key matches the query.
func TestMPHF_ZeroValueKeyNotFound(t *testing.T) {
	// With n=1 and slotN=2 (smallest prime >= 1), one slot is unoccupied
	// and contains the zero value "". Get("") must not match it.
	m := idmapper.NewMPHF([]string{"k0"}, idmapper.StringHasher)
	m.Freeze()
	if id, ok := m.Get(""); ok || id != 0 {
		t.Errorf("Get(\"\") on {\"k0\"}: got (%d, %v), want (0, false)", id, ok)
	}

	// Also test Gets for the same bug.
	ids := m.Gets([]string{""})
	if ids[0] != 0 {
		t.Errorf("Gets([\"\"]): got %d, want 0", ids[0])
	}

	// Test with int keys: 0 is the zero value.
	intHasher := func(k int) (h1, h2 uint64) {
		h1 = uint64(k) * 0x9e3779b97f4a7c15
		h2 = uint64(k)*0xc4ceb9fe1a85ec53 | 1
		return h1, h2
	}
	m2 := idmapper.NewMPHF([]int{1}, intHasher)
	m2.Freeze()
	if id, ok := m2.Get(0); ok || id != 0 {
		t.Errorf("Get(0) on {1}: got (%d, %v), want (0, false)", id, ok)
	}
}

// TestMPHF_GetsRemainderPath exercises the scalar remainder loop in Gets
// (for len(keys) not a multiple of 4) by testing batch sizes 1, 2, 3, 5.
func TestMPHF_GetsRemainderPath(t *testing.T) {
	keys := []string{"a", "b", "c", "d", "e", "f", "g"}
	m := idmapper.NewMPHF(keys, idmapper.StringHasher)
	m.Freeze()

	for _, size := range []int{1, 2, 3, 5} {
		batch := keys[:size]
		ids := m.Gets(batch)
		for j := 0; j < len(ids); j++ {
			if want := uint64(j + 1); ids[j] != want {
				t.Errorf("Gets(%d keys)[%d]: got %d, want %d", size, j, ids[j], want)
			}
		}
	}
}

// TestMPHF_LenAfterFreeze verifies Len returns the correct count after Freeze.
func TestMPHF_LenAfterFreeze(t *testing.T) {
	m := idmapper.NewMPHF([]string{"a", "b", "c"}, idmapper.StringHasher)
	m.Set("d")
	if got := m.Len(); got != 4 {
		t.Errorf("Len() before Freeze: got %d, want 4", got)
	}
	m.Freeze()
	if got := m.Len(); got != 4 {
		t.Errorf("Len() after Freeze: got %d, want 4", got)
	}
}

// TestMPHF_DoubleFreezeIdempotent verifies that calling Freeze a second time
// does not panic and leaves the mapper in a consistent state.
func TestMPHF_DoubleFreezeIdempotent(t *testing.T) {
	m := idmapper.NewMPHF([]string{"a", "b"}, idmapper.StringHasher)
	m.Freeze()
	m.Freeze() // must not panic

	if id, ok := m.Get("a"); !ok || id != 1 {
		t.Errorf("Get(%q) after double Freeze: got (%d, %v), want (1, true)", "a", id, ok)
	}
}

// TestMPHF_ConcurrentReadsAfterFreeze spawns many goroutines that all call
// Get simultaneously after sealing. The -race detector will surface any data
// races introduced by the lock-free read path.
func TestMPHF_ConcurrentReadsAfterFreeze(t *testing.T) {
	m := idmapper.NewMPHF([]string{"a", "b", "c"}, idmapper.StringHasher)
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

// TestMPHF_EmptyFreeze verifies that a mapper frozen with no keys returns
// (0, false) for any key without panicking.
func TestMPHF_EmptyFreeze(t *testing.T) {
	m := idmapper.NewMPHF[string](nil, idmapper.StringHasher)
	m.Freeze()

	if id, ok := m.Get("anything"); ok || id != 0 {
		t.Errorf("Get on empty frozen mapper: got (%d, %v), want (0, false)", id, ok)
	}
	if ids := m.Gets([]string{"a", "b"}); ids[0] != 0 || ids[1] != 0 {
		t.Errorf("Gets on empty frozen mapper: got %v, want [0 0]", ids)
	}
}

// -- Benchmarks ---------------------------------------------------------------

func BenchmarkGet_MPHF(b *testing.B) {
	m := idmapper.NewMPHF(benchKeys, idmapper.StringHasher)
	m.Freeze()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.Get(benchKeys[i%len(benchKeys)])
	}
}

func BenchmarkGetParallel_MPHF(b *testing.B) {
	m := idmapper.NewMPHF(benchKeys, idmapper.StringHasher)
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

// BenchmarkNew_MPHF measures the total cost of NewMPHF + Freeze (CHD build).
func BenchmarkNew_MPHF(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m := idmapper.NewMPHF(benchKeys, idmapper.StringHasher)
		m.Freeze()
	}
}

// BenchmarkGets_MPHF measures a batch Get of benchBatch keys using the
// 4-wide software-pipelined loop.
func BenchmarkGets_MPHF(b *testing.B) {
	m := idmapper.NewMPHF(benchKeys, idmapper.StringHasher)
	m.Freeze()
	batch := benchKeys[:benchBatch]
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.Gets(batch)
	}
}

func BenchmarkGetsParallel_MPHF(b *testing.B) {
	m := idmapper.NewMPHF(benchKeys, idmapper.StringHasher)
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

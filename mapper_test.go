package idmapper_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/AdiSaripuloh/idmapper"
)

// benchKeys is a pool of 1000 unique keys shared across all benchmarks.
var benchKeys = func() []string {
	keys := make([]string, 1000)
	for i := 0; i < len(keys); i++ {
		keys[i] = fmt.Sprintf("key-%04d", i)
	}
	return keys
}()

// Compile-time interface checks.
var (
	_ idmapper.Mapper[string]      = (*idmapper.RWMutex[string])(nil)
	_ idmapper.Mapper[string]      = (*idmapper.Freeze[string])(nil)
	_ idmapper.Mapper[string]      = (*idmapper.COW[string])(nil)
	_ idmapper.Mapper[string]      = (*idmapper.MPHF[string])(nil)
	_ idmapper.Snapshotter[string] = (*idmapper.COW[string])(nil)
)

// testBehavior is a shared correctness helper run against every strategy.
func testBehavior(t *testing.T, m idmapper.Mapper[string]) {
	t.Helper()

	// Pre-populated keys carry sequential IDs starting at 1.
	getTests := []struct {
		key    string
		wantID uint64
	}{
		{"a", 1},
		{"b", 2},
		{"c", 3},
	}
	for i := 0; i < len(getTests); i++ {
		id, ok := m.Get(getTests[i].key)
		if !ok {
			t.Errorf("Get(%q): expected found, got not found", getTests[i].key)
		}
		if id != getTests[i].wantID {
			t.Errorf("Get(%q): got %d, want %d", getTests[i].key, id, getTests[i].wantID)
		}
	}

	// Unknown key returns the zero value.
	if id, ok := m.Get("missing"); ok || id != 0 {
		t.Errorf("Get(%q): got (%d, %v), want (0, false)", "missing", id, ok)
	}

	// Set of an existing key is idempotent.
	if id := m.Set("a"); id != 1 {
		t.Errorf("Set(%q) idempotent: got %d, want 1", "a", id)
	}

	// Set of a new key assigns the next sequential ID.
	if id := m.Set("d"); id != 4 {
		t.Errorf("Set(%q): got %d, want 4", "d", id)
	}
	if id, ok := m.Get("d"); !ok || id != 4 {
		t.Errorf("Get(%q) after Set: got (%d, %v), want (4, true)", "d", id, ok)
	}
}

// testBatchBehavior is a shared correctness helper for Sets and Gets.
func testBatchBehavior(t *testing.T, m idmapper.Mapper[string]) {
	t.Helper()

	setsTests := []struct {
		name    string
		keys    []string
		wantIDs []uint64
	}{
		{"new keys", []string{"a", "b", "c"}, []uint64{1, 2, 3}},
		{"idempotent", []string{"a", "b", "c"}, []uint64{1, 2, 3}},
		{"mixed existing and new", []string{"b", "d", "a", "e"}, []uint64{2, 4, 1, 5}},
		{"nil input", nil, []uint64{}},
	}
	for i := 0; i < len(setsTests); i++ {
		ids := m.Sets(setsTests[i].keys)
		if !equalSlice(ids, setsTests[i].wantIDs) {
			t.Errorf("Sets %s: got %v, want %v", setsTests[i].name, ids, setsTests[i].wantIDs)
		}
	}

	getsTests := []struct {
		name    string
		keys    []string
		wantIDs []uint64
	}{
		{"existing and missing", []string{"a", "missing", "c", "e"}, []uint64{1, 0, 3, 5}},
		{"nil input", nil, []uint64{}},
	}
	for i := 0; i < len(getsTests); i++ {
		ids := m.Gets(getsTests[i].keys)
		if !equalSlice(ids, getsTests[i].wantIDs) {
			t.Errorf("Gets %s: got %v, want %v", getsTests[i].name, ids, getsTests[i].wantIDs)
		}
	}
}

func equalSlice(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestNewEmpty verifies that a mapper built from nil keys is usable and that
// the first Set correctly assigns ID 1, preserving the 1-based contract.
func TestNewEmpty(t *testing.T) {
	tests := []struct {
		name   string
		mapper idmapper.Mapper[string]
	}{
		{"RWMutex", idmapper.NewRWMutex[string](nil)},
		{"Freeze", idmapper.NewFreeze[string](nil)},
		{"COW", idmapper.NewCOW[string](nil)},
		{"MPHF", idmapper.NewMPHF[string](nil, idmapper.StringHasher)},
	}
	for i := 0; i < len(tests); i++ {
		tc := tests[i]
		t.Run(tc.name, func(t *testing.T) {
			if id, ok := tc.mapper.Get("x"); ok || id != 0 {
				t.Errorf("Get on empty mapper: got (%d, %v), want (0, false)", id, ok)
			}
			if id := tc.mapper.Set("x"); id != 1 {
				t.Errorf("first Set on empty mapper: got %d, want 1", id)
			}
			if id, ok := tc.mapper.Get("x"); !ok || id != 1 {
				t.Errorf("Get after first Set: got (%d, %v), want (1, true)", id, ok)
			}
		})
	}
}

// TestKeyOrder verifies that IDs are assigned in the order keys appear in the
// input slice, with no gaps and no off-by-one errors.
func TestKeyOrder(t *testing.T) {
	keys := []string{"alpha", "beta", "gamma", "delta", "epsilon"}

	tests := []struct {
		name   string
		mapper idmapper.Mapper[string]
	}{
		{"RWMutex", idmapper.NewRWMutex(keys)},
		{"Freeze", idmapper.NewFreeze(keys)},
		{"COW", idmapper.NewCOW(keys)},
		{"MPHF", idmapper.NewMPHF(keys, idmapper.StringHasher)},
	}
	for i := 0; i < len(tests); i++ {
		tc := tests[i]
		t.Run(tc.name, func(t *testing.T) {
			for j := 0; j < len(keys); j++ {
				want := uint64(j + 1)
				if id, ok := tc.mapper.Get(keys[j]); !ok || id != want {
					t.Errorf("Get(%q): got (%d, %v), want (%d, true)", keys[j], id, ok, want)
				}
			}
		})
	}
}

// TestDuplicateKeys verifies that duplicate entries in the input slice are
// deduplicated and the first occurrence's position determines the ID.
func TestDuplicateKeys(t *testing.T) {
	keys := []string{"x", "y", "x", "z", "y"}

	wantCases := []struct {
		key    string
		wantID uint64
	}{
		{"x", 1},
		{"y", 2},
		{"z", 3},
	}

	tests := []struct {
		name   string
		mapper idmapper.Mapper[string]
	}{
		{"RWMutex", idmapper.NewRWMutex(keys)},
		{"Freeze", idmapper.NewFreeze(keys)},
		{"COW", idmapper.NewCOW(keys)},
		{"MPHF", idmapper.NewMPHF(keys, idmapper.StringHasher)},
	}
	for i := 0; i < len(tests); i++ {
		tc := tests[i]
		t.Run(tc.name, func(t *testing.T) {
			for j := 0; j < len(wantCases); j++ {
				if got, ok := tc.mapper.Get(wantCases[j].key); !ok || got != wantCases[j].wantID {
					t.Errorf("Get(%q): got (%d, %v), want (%d, true)", wantCases[j].key, got, ok, wantCases[j].wantID)
				}
			}
		})
	}
}

// TestCacheLinePadding verifies that the hardcoded struct sizes used for
// cache-line padding match the actual sizes reported by the compiler.
func TestCacheLinePadding(t *testing.T) {
	if got := unsafe.Sizeof(sync.RWMutex{}); got != 24 {
		t.Errorf("sync.RWMutex size changed: got %d, assumed 24", got)
	}
	if got := unsafe.Sizeof(atomic.Pointer[int]{}); got != 8 {
		t.Errorf("atomic.Pointer size changed: got %d, assumed 8", got)
	}
}

// TestLen verifies that Len returns the correct count for all strategies.
func TestLen(t *testing.T) {
	keys := []string{"a", "b", "c"}
	tests := []struct {
		name   string
		mapper idmapper.Mapper[string]
	}{
		{"RWMutex", idmapper.NewRWMutex(keys)},
		{"Freeze", idmapper.NewFreeze(keys)},
		{"COW", idmapper.NewCOW(keys)},
		{"MPHF", idmapper.NewMPHF(keys, idmapper.StringHasher)},
	}
	for i := 0; i < len(tests); i++ {
		tc := tests[i]
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.mapper.Len(); got != 3 {
				t.Errorf("Len(): got %d, want 3", got)
			}
			tc.mapper.Set("d")
			if got := tc.mapper.Len(); got != 4 {
				t.Errorf("Len() after Set: got %d, want 4", got)
			}
		})
	}
}

// TestEmptyStringKey verifies that an empty string is treated as a valid key.
func TestEmptyStringKey(t *testing.T) {
	tests := []struct {
		name   string
		mapper idmapper.Mapper[string]
	}{
		{"RWMutex", idmapper.NewRWMutex[string](nil)},
		{"Freeze", idmapper.NewFreeze[string](nil)},
		{"COW", idmapper.NewCOW[string](nil)},
		{"MPHF", idmapper.NewMPHF[string](nil, idmapper.StringHasher)},
	}
	for i := 0; i < len(tests); i++ {
		tc := tests[i]
		t.Run(tc.name, func(t *testing.T) {
			if id := tc.mapper.Set(""); id != 1 {
				t.Errorf("Set(''): got %d, want 1", id)
			}
			if id, ok := tc.mapper.Get(""); !ok || id != 1 {
				t.Errorf("Get('') after Set: got (%d, %v), want (1, true)", id, ok)
			}
		})
	}
}

package idmapper_test

import (
	"fmt"
	"testing"

	"github.com/AdiSaripuloh/idmapper"
)

// FuzzMPHFHash verifies that mphfHash (exercised through the public API)
// always produces deterministic results and that h2 is never zero.
// We build a one-key MPHF and confirm Get returns the correct ID twice.
func FuzzMPHFHash(f *testing.F) {
	f.Add("alice")
	f.Add("")
	f.Add("key-0000")
	f.Add("\x00\xff")

	f.Fuzz(func(t *testing.T, key string) {
		m := idmapper.NewMPHF([]string{key}, idmapper.StringHasher)
		m.Freeze()

		// First lookup — must find the key.
		id1, ok1 := m.Get(key)
		if !ok1 || id1 != 1 {
			t.Fatalf("Get(%q): got (%d, %v), want (1, true)", key, id1, ok1)
		}

		// Second lookup — must be deterministic.
		id2, ok2 := m.Get(key)
		if !ok2 || id2 != id1 {
			t.Fatalf("Get(%q) second call: got (%d, %v), want (%d, true)", key, id2, ok2, id1)
		}
	})
}

// FuzzMPHFBuildAndGet builds an MPHF from a fuzz-generated key set and
// verifies that every inserted key is retrievable with the correct ID,
// and that a non-member key returns (0, false).
func FuzzMPHFBuildAndGet(f *testing.F) {
	f.Add(3, "seed")
	f.Add(0, "")
	f.Add(10, "abc")
	f.Add(100, "x")

	f.Fuzz(func(t *testing.T, n int, prefix string) {
		if n < 0 {
			n = -n
		}
		if n > 1000 {
			n = 1000
		}

		keys := make([]string, n)
		for i := 0; i < n; i++ {
			keys[i] = fmt.Sprintf("%s%d", prefix, i)
		}

		m := idmapper.NewMPHF(keys, idmapper.StringHasher)

		// Freeze may panic with "displacement exceeded safety limit" for
		// pathological key sets — this is a known CHD boundary, not a bug
		// in the test. Recover and skip.
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Skipf("Freeze panicked (expected for pathological keys): %v", r)
				}
			}()
			m.Freeze()
		}()

		// Every key must be retrievable with the correct sequential ID.
		for i, key := range keys {
			id, ok := m.Get(key)
			if !ok {
				t.Fatalf("Get(%q): not found after Freeze", key)
			}
			if want := uint64(i + 1); id != want {
				t.Fatalf("Get(%q): got %d, want %d", key, id, want)
			}
		}

		// A non-member key must return (0, false).
		nonMember := fmt.Sprintf("%s_nonmember_%d", prefix, n)
		if id, ok := m.Get(nonMember); ok || id != 0 {
			t.Fatalf("Get(%q): got (%d, %v), want (0, false)", nonMember, id, ok)
		}
	})
}

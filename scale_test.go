package idmapper_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/AdiSaripuloh/idmapper"
)

// scaleCounts lists the (n, g) pairs used by both scale tests and benchmarks.
// n is the initial population via constructor; g is the number of new keys
// added after construction via Set/Sets — kept small and fixed, matching real
// workloads where growth is incremental.
// Entries with short=true are skipped under -test.short; benchmarks ignore it
// since they only run with -bench.
var scaleCounts = []struct {
	n     int // initial keys — pre-populated via constructor
	g     int // grown keys (distinct from initial keys)
	short bool
}{
	{1, 10, false},
	{10, 1, false},
	{10, 10, false},
	{10, 100, false},
	{100, 1, false},
	{100, 1_000, false},
	{1_000, 1, false},
	{1_000, 10, false},
	{1_000, 1_000, false},
	{10_000, 1, true},
	{10_000, 10, true},
	{10_000, 1_000, true},
	{100_000, 1, true},
	{100_000, 10, true},
	{100_000, 1_000, true},
	{1_000_000, 10, true},
	{1_000_000, 1_000, true},
}

// makeScaleKeys returns n unique, deterministic keys with the given prefix.
// Using distinct prefixes for initial and grown keys guarantees no overlap
// between the two sets.
func makeScaleKeys(prefix string, n int) []string {
	keys := make([]string, n)
	for i := 0; i < len(keys); i++ {
		keys[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	return keys
}

// -- Scale Tests -------------------------------------------------------------

// TestScaleGet pre-populates each strategy with n initial keys (prefix "k"),
// grows by adding g more keys (prefix "g") via a single batch Sets call, then
// verifies every key in both sets via Get.
//
// Freeze and MPHF are pre-seeded with both sets at construction time and then
// sealed; they have no separate growth phase since writes are forbidden after
// Freeze.
func TestScaleGet(t *testing.T) {
	for i := 0; i < len(scaleCounts); i++ {
		tc := scaleCounts[i]
		t.Run(fmt.Sprintf("n=%d_g=%d", tc.n, tc.g), func(t *testing.T) {
			if tc.short && testing.Short() {
				t.Skip("skipping large scale test under -short")
			}

			initialKeys := makeScaleKeys("k", tc.n)
			grownKeys := makeScaleKeys("g", tc.g)

			type factory struct {
				name string
				new  func() idmapper.Mapper[string]
			}
			factories := []factory{
				{"RWMutex", func() idmapper.Mapper[string] {
					m := idmapper.NewRWMutex(initialKeys)
					m.Sets(grownKeys)
					return m
				}},
				{"Freeze", func() idmapper.Mapper[string] {
					// Freeze is write-once: seed both sets in the constructor.
					allKeys := make([]string, tc.n+tc.g)
					copy(allKeys, initialKeys)
					copy(allKeys[tc.n:], grownKeys)
					m := idmapper.NewFreeze(allKeys)
					m.Freeze()
					return m
				}},
				{"COW", func() idmapper.Mapper[string] {
					m := idmapper.NewCOW(initialKeys)
					m.Sets(grownKeys)
					return m
				}},
				{"MPHF", func() idmapper.Mapper[string] {
					// MPHF is write-once: seed both sets in the constructor, then freeze.
					allKeys := make([]string, tc.n+tc.g)
					copy(allKeys, initialKeys)
					copy(allKeys[tc.n:], grownKeys)
					m := idmapper.NewMPHF(allKeys, idmapper.StringHasher)
					m.Freeze()
					return m
				}},
			}

			for f := 0; f < len(factories); f++ {
				fac := factories[f]
				t.Run(fac.name, func(t *testing.T) {
					m := fac.new()

					// Initial keys → IDs 1..n.
					for j := 0; j < len(initialKeys); j++ {
						id, ok := m.Get(initialKeys[j])
						if !ok {
							t.Fatalf("Get(%q): not found", initialKeys[j])
						}
						if want := uint64(j + 1); id != want {
							t.Fatalf("Get(%q): got %d, want %d", initialKeys[j], id, want)
						}
					}

					// Grown keys → IDs n+1..n+g.
					for j := 0; j < len(grownKeys); j++ {
						id, ok := m.Get(grownKeys[j])
						if !ok {
							t.Fatalf("Get(%q): not found", grownKeys[j])
						}
						if want := uint64(tc.n + j + 1); id != want {
							t.Fatalf("Get(%q): got %d, want %d", grownKeys[j], id, want)
						}
					}
				})
			}
		})
	}
}

// TestScaleSet creates each strategy with n initial keys via the constructor,
// grows by g keys via individual Set() calls, then verifies all keys via Get
// and confirms negative lookups for unknown keys.
func TestScaleSet(t *testing.T) {
	for i := 0; i < len(scaleCounts); i++ {
		tc := scaleCounts[i]
		t.Run(fmt.Sprintf("n=%d_g=%d", tc.n, tc.g), func(t *testing.T) {
			if tc.short && testing.Short() {
				t.Skip("skipping large scale test under -short")
			}

			initialKeys := makeScaleKeys("k", tc.n)
			grownKeys := makeScaleKeys("g", tc.g)

			type factory struct {
				name string
				new  func() idmapper.Mapper[string]
			}
			factories := []factory{
				{"RWMutex", func() idmapper.Mapper[string] { return idmapper.NewRWMutex(initialKeys) }},
				{"Freeze", func() idmapper.Mapper[string] { return idmapper.NewFreeze(initialKeys) }},
				{"COW", func() idmapper.Mapper[string] { return idmapper.NewCOW(initialKeys) }},
				{"MPHF", func() idmapper.Mapper[string] { return idmapper.NewMPHF(initialKeys, idmapper.StringHasher) }},
			}

			for f := 0; f < len(factories); f++ {
				fac := factories[f]
				t.Run(fac.name, func(t *testing.T) {
					m := fac.new()

					// Grow by g keys via individual Set() calls → IDs n+1..n+g.
					for j := 0; j < len(grownKeys); j++ {
						id := m.Set(grownKeys[j])
						if want := uint64(tc.n + j + 1); id != want {
							t.Fatalf("Set(%q): got %d, want %d", grownKeys[j], id, want)
						}
					}

					// Seal write-once strategies before switching to read path.
					if fm, ok := m.(*idmapper.Freeze[string]); ok {
						fm.Freeze()
					}
					if mm, ok := m.(*idmapper.MPHF[string]); ok {
						mm.Freeze()
					}

					// Verify initial keys → IDs 1..n.
					for j := 0; j < len(initialKeys); j++ {
						id, ok := m.Get(initialKeys[j])
						if !ok {
							t.Fatalf("Get(%q): not found", initialKeys[j])
						}
						if want := uint64(j + 1); id != want {
							t.Fatalf("Get(%q): got %d, want %d", initialKeys[j], id, want)
						}
					}

					// Verify grown keys → IDs n+1..n+g.
					for j := 0; j < len(grownKeys); j++ {
						id, ok := m.Get(grownKeys[j])
						if !ok {
							t.Fatalf("Get(%q): not found", grownKeys[j])
						}
						if want := uint64(tc.n + j + 1); id != want {
							t.Fatalf("Get(%q): got %d, want %d", grownKeys[j], id, want)
						}
					}

					// Negative lookup for unknown key.
					if id, ok := m.Get("unknown"); ok || id != 0 {
						t.Fatalf("Get(%q): expected not found, got (%d, %v)", "unknown", id, ok)
					}
				})
			}
		})
	}
}

// TestScaleSets creates each strategy with n initial keys via the constructor,
// grows by g keys via a single batch Sets() call, then verifies all keys via
// Get and confirms negative lookups for unknown keys.
func TestScaleSets(t *testing.T) {
	for i := 0; i < len(scaleCounts); i++ {
		tc := scaleCounts[i]
		t.Run(fmt.Sprintf("n=%d_g=%d", tc.n, tc.g), func(t *testing.T) {
			if tc.short && testing.Short() {
				t.Skip("skipping large scale test under -short")
			}

			initialKeys := makeScaleKeys("k", tc.n)
			grownKeys := makeScaleKeys("g", tc.g)

			type factory struct {
				name string
				new  func() idmapper.Mapper[string]
			}
			factories := []factory{
				{"RWMutex", func() idmapper.Mapper[string] { return idmapper.NewRWMutex(initialKeys) }},
				{"Freeze", func() idmapper.Mapper[string] { return idmapper.NewFreeze(initialKeys) }},
				{"COW", func() idmapper.Mapper[string] { return idmapper.NewCOW(initialKeys) }},
				{"MPHF", func() idmapper.Mapper[string] { return idmapper.NewMPHF(initialKeys, idmapper.StringHasher) }},
			}

			for f := 0; f < len(factories); f++ {
				fac := factories[f]
				t.Run(fac.name, func(t *testing.T) {
					m := fac.new()

					// Grow by g keys via batch Sets() → IDs n+1..n+g.
					growIDs := m.Sets(grownKeys)
					if len(growIDs) != tc.g {
						t.Fatalf("growth Sets: got %d IDs, want %d", len(growIDs), tc.g)
					}
					for j := 0; j < len(growIDs); j++ {
						if want := uint64(tc.n + j + 1); growIDs[j] != want {
							t.Fatalf("growth Sets[%d] (%q): got %d, want %d",
								j, grownKeys[j], growIDs[j], want)
						}
					}

					// Seal write-once strategies before switching to read path.
					if fm, ok := m.(*idmapper.Freeze[string]); ok {
						fm.Freeze()
					}
					if mm, ok := m.(*idmapper.MPHF[string]); ok {
						mm.Freeze()
					}

					// Verify initial keys → IDs 1..n.
					for j := 0; j < len(initialKeys); j++ {
						id, ok := m.Get(initialKeys[j])
						if !ok {
							t.Fatalf("Get(%q): not found", initialKeys[j])
						}
						if want := uint64(j + 1); id != want {
							t.Fatalf("Get(%q): got %d, want %d", initialKeys[j], id, want)
						}
					}

					// Verify grown keys → IDs n+1..n+g.
					for j := 0; j < len(grownKeys); j++ {
						id, ok := m.Get(grownKeys[j])
						if !ok {
							t.Fatalf("Get(%q): not found", grownKeys[j])
						}
						if want := uint64(tc.n + j + 1); id != want {
							t.Fatalf("Get(%q): got %d, want %d", grownKeys[j], id, want)
						}
					}

					// Negative lookup for unknown key.
					if id, ok := m.Get("unknown"); ok || id != 0 {
						t.Fatalf("Get(%q): expected not found, got (%d, %v)", "unknown", id, ok)
					}
				})
			}
		})
	}
}

// -- Scale Benchmarks --------------------------------------------------------

// BenchmarkScaleNew measures the time to construct a mapper pre-populated
// with n keys. For Freeze and MPHF, Freeze() is included because it is part
// of the required build phase before the mapper is readable.
// Only distinct n values are benchmarked (g is irrelevant for construction).
func BenchmarkScaleNew(b *testing.B) {
	prevN := -1
	for s := 0; s < len(scaleCounts); s++ {
		n := scaleCounts[s].n
		if n == prevN {
			continue
		}
		prevN = n
		initialKeys := makeScaleKeys("k", n)

		type factory struct {
			name string
			fn   func() idmapper.Mapper[string]
		}
		factories := []factory{
			{"RWMutex", func() idmapper.Mapper[string] { return idmapper.NewRWMutex(initialKeys) }},
			{"Freeze", func() idmapper.Mapper[string] {
				m := idmapper.NewFreeze(initialKeys)
				m.Freeze()
				return m
			}},
			{"COW", func() idmapper.Mapper[string] { return idmapper.NewCOW(initialKeys) }},
			{"MPHF", func() idmapper.Mapper[string] {
				m := idmapper.NewMPHF(initialKeys, idmapper.StringHasher)
				m.Freeze()
				return m
			}},
		}
		for f := 0; f < len(factories); f++ {
			fac := factories[f]
			b.Run(
				fmt.Sprintf("n=%d/%s", n, fac.name),
				func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						fac.fn()
					}
				})
		}
	}
}

// BenchmarkScaleSet measures the cost of growing a mapper by g keys via
// individual Set() calls. Constructor cost is included because StopTimer/
// StartTimer per iteration causes hangs when the timed work is cheap.
// For Freeze and MPHF, Freeze() is called after Set to seal the mapper.
func BenchmarkScaleSet(b *testing.B) {
	for s := 0; s < len(scaleCounts); s++ {
		bc := scaleCounts[s]
		initialKeys := makeScaleKeys("k", bc.n)
		grownKeys := makeScaleKeys("g", bc.g)

		type factory struct {
			name string
			fn   func() idmapper.Mapper[string]
		}
		factories := []factory{
			{"RWMutex", func() idmapper.Mapper[string] { return idmapper.NewRWMutex(initialKeys) }},
			{"Freeze", func() idmapper.Mapper[string] { return idmapper.NewFreeze(initialKeys) }},
			{"COW", func() idmapper.Mapper[string] { return idmapper.NewCOW(initialKeys) }},
			{"MPHF", func() idmapper.Mapper[string] { return idmapper.NewMPHF(initialKeys, idmapper.StringHasher) }},
		}
		for f := 0; f < len(factories); f++ {
			fac := factories[f]
			b.Run(fmt.Sprintf("n=%d_g=%d/%s", bc.n, bc.g, fac.name), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					m := fac.fn()
					for j := 0; j < len(grownKeys); j++ {
						m.Set(grownKeys[j])
					}
					if fm, ok := m.(*idmapper.Freeze[string]); ok {
						fm.Freeze()
					}
					if mm, ok := m.(*idmapper.MPHF[string]); ok {
						mm.Freeze()
					}
				}
			})
		}
	}
}

// BenchmarkScaleSets measures the cost of growing a mapper by g keys via a
// single batch Sets() call. Constructor cost is included because StopTimer/
// StartTimer per iteration causes hangs when the timed work is cheap.
// For Freeze and MPHF, Freeze() is called after Sets to seal the mapper.
func BenchmarkScaleSets(b *testing.B) {
	for s := 0; s < len(scaleCounts); s++ {
		bc := scaleCounts[s]
		initialKeys := makeScaleKeys("k", bc.n)
		grownKeys := makeScaleKeys("g", bc.g)

		type factory struct {
			name string
			fn   func() idmapper.Mapper[string]
		}
		factories := []factory{
			{"RWMutex", func() idmapper.Mapper[string] { return idmapper.NewRWMutex(initialKeys) }},
			{"Freeze", func() idmapper.Mapper[string] { return idmapper.NewFreeze(initialKeys) }},
			{"COW", func() idmapper.Mapper[string] { return idmapper.NewCOW(initialKeys) }},
			{"MPHF", func() idmapper.Mapper[string] { return idmapper.NewMPHF(initialKeys, idmapper.StringHasher) }},
		}
		for f := 0; f < len(factories); f++ {
			fac := factories[f]
			b.Run(fmt.Sprintf("n=%d_g=%d/%s", bc.n, bc.g, fac.name), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					m := fac.fn()
					m.Sets(grownKeys)
					if fm, ok := m.(*idmapper.Freeze[string]); ok {
						fm.Freeze()
					}
					if mm, ok := m.(*idmapper.MPHF[string]); ok {
						mm.Freeze()
					}
				}
			})
		}
	}
}

// BenchmarkScaleGet measures Get throughput on a mapper with n+g keys, cycling
// through all keys in sequence.
func BenchmarkScaleGet(b *testing.B) {
	for s := 0; s < len(scaleCounts); s++ {
		bc := scaleCounts[s]
		initialKeys := makeScaleKeys("k", bc.n)
		grownKeys := makeScaleKeys("g", bc.g)
		allKeys := make([]string, bc.n+bc.g)
		copy(allKeys, initialKeys)
		copy(allKeys[bc.n:], grownKeys)
		total := len(allKeys)

		freezeMapper := idmapper.NewFreeze(initialKeys)
		freezeMapper.Sets(grownKeys)
		freezeMapper.Freeze()

		mphfMapper := idmapper.NewMPHF(initialKeys, idmapper.StringHasher)
		mphfMapper.Sets(grownKeys)
		mphfMapper.Freeze()

		rwmMapper := idmapper.NewRWMutex(initialKeys)
		rwmMapper.Sets(grownKeys)

		cowMapper := idmapper.NewCOW(initialKeys)
		cowMapper.Sets(grownKeys)

		type factory struct {
			name string
			m    idmapper.Mapper[string]
		}
		factories := []factory{
			{"RWMutex", rwmMapper},
			{"Freeze", freezeMapper},
			{"COW", cowMapper},
			{"MPHF", mphfMapper},
		}
		for f := 0; f < len(factories); f++ {
			fac := factories[f]
			b.Run(fmt.Sprintf("n=%d_g=%d/%s", bc.n, bc.g, fac.name), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					fac.m.Get(allKeys[i%total])
				}
			})
		}
	}
}

// BenchmarkScaleGetParallel measures concurrent Get throughput on a mapper
// with n+g keys. Highlights how RWMutex's reader-count atomic degrades under
// parallelism compared to the lock-free Freeze, COW, and MPHF strategies.
func BenchmarkScaleGetParallel(b *testing.B) {
	for s := 0; s < len(scaleCounts); s++ {
		bc := scaleCounts[s]
		initialKeys := makeScaleKeys("k", bc.n)
		grownKeys := makeScaleKeys("g", bc.g)
		allKeys := make([]string, bc.n+bc.g)
		copy(allKeys, initialKeys)
		copy(allKeys[bc.n:], grownKeys)
		total := len(allKeys)

		freezeMapper := idmapper.NewFreeze(initialKeys)
		freezeMapper.Sets(grownKeys)
		freezeMapper.Freeze()

		mphfMapper := idmapper.NewMPHF(initialKeys, idmapper.StringHasher)
		mphfMapper.Sets(grownKeys)
		mphfMapper.Freeze()

		rwmMapper := idmapper.NewRWMutex(initialKeys)
		rwmMapper.Sets(grownKeys)

		cowMapper := idmapper.NewCOW(initialKeys)
		cowMapper.Sets(grownKeys)

		type factory struct {
			name string
			m    idmapper.Mapper[string]
		}
		factories := []factory{
			{"RWMutex", rwmMapper},
			{"Freeze", freezeMapper},
			{"COW", cowMapper},
			{"MPHF", mphfMapper},
		}
		for f := 0; f < len(factories); f++ {
			fac := factories[f]
			b.Run(fmt.Sprintf("n=%d_g=%d/%s", bc.n, bc.g, fac.name), func(b *testing.B) {
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						fac.m.Get(allKeys[i%total])
						i++
					}
				})
			})
		}
	}
}

// BenchmarkScaleMemory reports the live heap bytes consumed by each strategy
// after building a mapper with n initial keys and g grown keys.
func BenchmarkScaleMemory(b *testing.B) {
	for s := 0; s < len(scaleCounts); s++ {
		bc := scaleCounts[s]
		initialKeys := makeScaleKeys("k", bc.n)
		grownKeys := makeScaleKeys("g", bc.g)

		type factory struct {
			name string
			fn   func() idmapper.Mapper[string]
		}
		factories := []factory{
			{"RWMutex", func() idmapper.Mapper[string] {
				m := idmapper.NewRWMutex(initialKeys)
				m.Sets(grownKeys)
				return m
			}},
			{"Freeze", func() idmapper.Mapper[string] {
				m := idmapper.NewFreeze(initialKeys)
				m.Sets(grownKeys)
				m.Freeze()
				return m
			}},
			{"COW", func() idmapper.Mapper[string] {
				m := idmapper.NewCOW(initialKeys)
				m.Sets(grownKeys)
				return m
			}},
			{"MPHF", func() idmapper.Mapper[string] {
				m := idmapper.NewMPHF(initialKeys, idmapper.StringHasher)
				m.Sets(grownKeys)
				m.Freeze()
				return m
			}},
		}
		for f := 0; f < len(factories); f++ {
			fac := factories[f]
			b.Run(fmt.Sprintf("n=%d_g=%d/%s", bc.n, bc.g, fac.name), func(b *testing.B) {
				b.ReportAllocs()
				// Keep all mappers alive so GC cannot reclaim them
				// between iterations; HeapInuse then reflects the
				// cumulative live set and dividing by b.N gives the
				// per-mapper footprint.
				sinks := make([]idmapper.Mapper[string], b.N)
				runtime.GC()
				var before runtime.MemStats
				runtime.ReadMemStats(&before)
				for i := 0; i < b.N; i++ {
					sinks[i] = fac.fn()
				}
				runtime.GC()
				var after runtime.MemStats
				runtime.ReadMemStats(&after)
				heapDelta := float64(after.HeapInuse) - float64(before.HeapInuse)
				if heapDelta < 0 {
					heapDelta = 0
				}
				b.ReportMetric(heapDelta/float64(b.N), "heap-bytes/op")
				runtime.KeepAlive(sinks)
			})
		}
	}
}

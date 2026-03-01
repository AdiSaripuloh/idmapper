# idmapper

[![Go Reference](https://pkg.go.dev/badge/github.com/AdiSaripuloh/idmapper.svg)](https://pkg.go.dev/github.com/AdiSaripuloh/idmapper)
[![Go Report Card](https://goreportcard.com/badge/github.com/AdiSaripuloh/idmapper)](https://goreportcard.com/report/github.com/AdiSaripuloh/idmapper)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A generic Go library that assigns stable, sequential integer IDs to a set of keys — designed to serve as an ID mapper for bitsets. Works with any `comparable` key type (`string`, `int`, `uuid.UUID`, etc.).

## Overview

`idmapper` converts a slice of identifiers into a mapping where each key is assigned a unique, 1-based sequential ID:

```
[]K{"key1", "key2", ..., "keyN"}
    ↓
map[K]uint64{"key1": 1, "key2": 2, ..., "keyN": N}
```

You can then look up any key's ID:

```go
id, ok := mapper.Get("key1") // 1, true
id, ok  = mapper.Get("key2") // 2, true
```

The returned IDs are intended to be used as **bit positions** in a bitset, enabling efficient set membership tests, unions, intersections, and other bitwise operations over large collections of named entities.

All mapper types are generic over `[K comparable]`, so you can use `string`, `int`, `uuid.UUID`, or any comparable type as keys.

## Use Case

When working with bitsets, each element in a universe must be mapped to a fixed bit index. `idmapper` handles this mapping layer:

```
universe: ["alice", "bob", "carol", "dave"]
                ↓
idmapper: {"alice": 1, "bob": 2, "carol": 3, "dave": 4}
                ↓
bitset(alice, carol) → bit 1 and bit 3 are set → 0b1010
```

This is useful for:

- Permission/role systems using bitsets
- Feature flag sets
- Efficient set operations over named entities
- Any scenario where named members of a fixed universe need compact integer representation

## Strategies

`idmapper` provides four strategies. Choose based on your read/write access pattern:

| Strategy | Read | Write | Best For |
|---|---|---|---|
| `RWMutex` | shared lock | exclusive lock | general purpose, frequent writes |
| `Freeze` | zero-lock | build phase only | write-once, read-many |
| `COW` | zero-lock | atomic pointer swap | rare writes, high-throughput reads |
| `MPHF` | zero-lock, no map | build phase only | static keys, maximum read throughput |

### RWMutex

The standard strategy using `sync.RWMutex`. Multiple concurrent reads are allowed; writes are exclusive. `mu` is padded to its own cache line to prevent false sharing with the data fields under high read concurrency.

Suitable for general use when updates happen regularly.

### Freeze

The mapper is built and populated during a single-goroutine setup phase, then **frozen**. After the freeze, the internal map is never mutated again, so reads require no locking whatsoever — not even an atomic load.

Best choice when the universe of keys is known upfront and never changes at runtime.

### COW (Copy-on-Write)

On every write, the current map is copied, the new key is inserted into the copy, and the internal pointer is atomically swapped. Readers atomically load the pointer and look up the key with no lock. Writers are serialised via a mutex and pay the cost of a full map copy.

`ptr` is isolated on its own cache line so that writers locking the mutex never dirty the cache line that readers load from.

Best choice when writes are infrequent but reads must be as fast as possible.

### MPHF

A write-once mapper backed by a **Minimal Perfect Hash Function** built with the CHD (Compress, Hash, Displace) algorithm — entirely in pure Go with no external dependencies.

During the build phase, keys are collected in a regular map (identical to Freeze). Calling `Freeze` constructs the CHD structure: it assigns every key a unique slot in a flat array using two hash functions and a per-bucket displacement table, then discards the build-phase map.

Because the CHD algorithm requires two independent 64-bit hash values per key, `NewMPHF` accepts a `Hasher[K]` function. A built-in `StringHasher` is provided for `string` keys; for other key types, supply your own hasher.

After `Freeze`, `Get` performs:
1. Two hash computations (h1, h2) via the supplied hasher — pure arithmetic
2. One displacement table lookup: `d[h1 % r]` — small array, nearly always in L1 cache
3. One slot lookup: `keys[slot] == key` — flat array, no pointer chasing

No map buckets, no pointer indirection, no locking. `Gets` uses a 4-wide software-pipelined loop to overlap hash computation with memory loads, hiding cache-miss latency on large batches.

The slot array is sized to the smallest prime >= n rather than exactly n. This ensures `gcd(h2, slotN) = 1` for essentially all keys, guaranteeing every key can reach any slot during CHD construction regardless of how composite n is.

**Trade-offs vs Freeze:**
- Parallel `Get` is ~15% faster (2.0 vs 2.3 ns/op) — no map bucket indirection
- Sequential `Get` is comparable (~9 vs 8 ns/op)
- Memory is ~2.3x lower: flat arrays at ~24 bytes/key vs a Go map at ~56 bytes/key
- Build cost is ~40x higher: CHD construction from 1,000 keys takes ~740 us vs ~18 us; amortised over millions of reads this is negligible
- Writes after `Freeze` panic, same as Freeze

Best choice when the key universe is fully known at startup and read throughput or memory footprint are the primary concern.

## Installation

```sh
go get github.com/AdiSaripuloh/idmapper
```

Requires Go 1.19+.

## Usage

### RWMutex

```go
m := idmapper.NewRWMutex([]string{"alice", "bob", "carol"})

// Single key operations.
m.Get("alice") // 1, true
m.Get("x")    // 0, false — unknown key
m.Set("dave")  // 4 — registers a new key; safe to call concurrently
m.Set("alice") // 1 — idempotent; returns the existing ID

// Batch operations — one lock acquisition for the entire slice.
ids := m.Sets([]string{"eve", "frank", "dave"}) // [5, 6, 4] — dave already existed
ids  = m.Gets([]string{"alice", "eve", "x"})    // [1, 5, 0] — 0 for unknown
```

### Freeze

```go
// Build phase — single goroutine, no locking required.
m := idmapper.NewFreeze([]string{"alice", "bob", "carol"})
m.Set("dave")                          // 4
m.Sets([]string{"eve", "frank"})       // [5, 6]

m.Freeze() // seal — no further writes allowed

// Share m freely across goroutines — Get and Gets are lock-free from here on.
m.Get("alice")                         // 1, true
m.Gets([]string{"alice", "carol"})     // [1, 3]
m.Set("x")                             // panics: Set called on a frozen Freeze mapper
```

### COW

```go
m := idmapper.NewCOW([]string{"alice", "bob", "carol"})

// Single key operations — Get is lock-free; Set copies the map on a new key.
m.Get("alice") // 1, true  — atomic load + map lookup, no lock
m.Set("dave")  // 4        — copies the map, atomically swaps the pointer

// Batch operations — Sets makes a single map copy for the whole batch.
ids := m.Sets([]string{"eve", "frank", "dave"}) // [5, 6, 4]
ids  = m.Gets([]string{"alice", "eve", "x"})    // [1, 5, 0]

// Capture a point-in-time snapshot (defensive copy — safe to mutate).
snap := m.GetSnapshot()

m.Len() // 6 — number of registered keys
```

`*COW[K]` also implements `Snapshotter[K]`, which embeds `Mapper[K]` and adds `GetSnapshot()`. Use this interface when you need to accept a mapper that supports snapshots.

### MPHF

```go
// Build phase — single goroutine, no locking required.
// StringHasher is the built-in hasher for string keys.
m := idmapper.NewMPHF([]string{"alice", "bob", "carol"}, idmapper.StringHasher)
m.Set("dave")                          // 4
m.Sets([]string{"eve", "frank"})       // [5, 6]

m.Freeze() // build CHD; seal — no further writes allowed

// Share m freely across goroutines — Get and Gets are lock-free with no map.
m.Get("alice")                         // 1, true
m.Gets([]string{"alice", "carol"})     // [1, 3]
m.Get("unknown")                       // 0, false
m.Set("x")                             // panics: Set called on a frozen MPHF mapper
```

### Custom Key Types

All strategies work with any `comparable` type. For RWMutex, Freeze, and COW, no extra setup is needed:

```go
m := idmapper.NewRWMutex([]int{100, 200, 300})
m.Get(200) // 2, true
```

For MPHF, provide a `Hasher[K]` that returns two independent 64-bit hash values:

```go
intHasher := func(k int) (h1, h2 uint64) {
    h1 = uint64(k) * 0x9e3779b97f4a7c15
    h2 = uint64(k)*0xc4ceb9fe1a85ec53 | 1 // ensure h2 is never zero
    return h1, h2
}
m := idmapper.NewMPHF([]int{100, 200, 300}, intHasher)
m.Freeze()
m.Get(200) // 2, true
```

## ID Assignment

- IDs start at **1** (not 0), so that the zero value of `uint64` naturally represents "not found".
- IDs are assigned in the order keys appear in the input slice.
- Duplicate keys retain the ID of their first occurrence.
- Calling `Set` or `Sets` on an existing key is always a no-op and returns the existing ID.
- Freeze and MPHF **panic** if `Set` or `Sets` is called after `Freeze()`.

## Benchmark Results

Measured on Apple M2 (arm64, 8 cores), Go 1.21, `-count=3 -benchmem`.

### Get — sequential

| Strategy | ns/op |
|---|---|
| COW | 7.9 |
| Freeze | 8.0 |
| MPHF | 9.2 |
| RWMutex | 11.8 |

### Get — parallel (8 goroutines)

| Strategy | ns/op |
|---|---|
| MPHF | 2.0 |
| COW | 2.5 |
| Freeze | 2.5 |
| RWMutex | 82 |

RWMutex is ~40x slower under parallel read pressure because all cores contend on the `readerCount` atomic counter inside `sync.RWMutex`. COW, Freeze, and MPHF avoid it entirely. MPHF edges out COW and Freeze because its flat key/ID arrays have no map bucket indirection, reducing cache pressure.

### Set — fast path (key already exists)

| Strategy | ns/op (sequential) | ns/op (parallel) |
|---|---|---|
| COW | 9 | 2 |
| RWMutex | 12 | 75 |

COW's fast path (atomic load + map lookup, no lock) scales linearly. RWMutex still acquires a shared lock, which contends at scale. MPHF panics on any write after `Freeze` and has no meaningful Set fast path.

### Set — slow path (new key)

| Strategy | ns/op |
|---|---|
| RWMutex | 80 |
| COW | 9 600 |

COW's write cost is ~120x higher than RWMutex when inserting into a 1 000-key map — it copies the entire map on every new key. Use `Sets` to amortise this cost when inserting multiple keys at once. MPHF and Freeze do not support writes after `Freeze`.

### Construction (`New` from 1 000 keys)

| Strategy | time |
|---|---|
| Freeze | ~18 us |
| RWMutex | ~18 us |
| COW | ~19 us |
| MPHF | ~740 us |

RWMutex, Freeze, and COW are all a single-threaded map insertion loop and are effectively identical. MPHF is ~40x slower because `Freeze` runs the CHD displacement search on top of the insertion phase. For a 1 000-key universe the absolute cost is still sub-millisecond, and it is amortised over all subsequent reads.

## Development

```sh
make test          # run unit tests (-short: scale tests capped at n=1 000/g=1 000)
make test-race     # run unit tests with the race detector
make test-scale    # run all scale tests (n up to 1 000 000; can be very slow)
make lint          # run golangci-lint (vet, staticcheck, errcheck, gofmt, …)
make check         # lint + test-race (run before committing)

make bench         # all benchmarks, 3 runs, -benchmem
make bench-scale   # scale benchmarks: New/Set/Sets/Get/GetParallel/Memory by n=1..1M
```

### Scale Tests

`scale_test.go` verifies correctness across the full growth lifecycle. Each entry starts with **n** initial keys (pre-populated via constructor) and adds **g** distinct grown keys via Set/Sets. Growth counts are small and fixed, matching real workloads where growth is incremental:

| n | g | default |
|---|---|---|
| 1 | 10 | ✓ |
| 10 | 1 | ✓ |
| 10 | 10 | ✓ |
| 10 | 100 | ✓ |
| 100 | 1 | ✓ |
| 100 | 1 000 | ✓ |
| 1 000 | 1 | ✓ |
| 1 000 | 10 | ✓ |
| 1 000 | 1 000 | ✓ |
| 10 000 | 1 | skipped (`-short`) |
| 10 000 | 10 | skipped (`-short`) |
| 10 000 | 1 000 | skipped (`-short`) |
| 100 000 | 1 | skipped (`-short`) |
| 100 000 | 10 | skipped (`-short`) |
| 100 000 | 1 000 | skipped (`-short`) |
| 1 000 000 | 10 | skipped (`-short`) |
| 1 000 000 | 1 000 | skipped (`-short`) |

`make test` and `make test-race` pass `-short`, so only the first nine entries run by default. Use `make test-scale` to run all entries explicitly.

### Scale Benchmarks

`scale_test.go` also contains six parameterised benchmarks over the same `(n, g)` pairs listed above, run across all four strategies (RWMutex, Freeze, COW, MPHF):

| Benchmark | What it measures |
|---|---|
| `BenchmarkScaleNew` | Construction time: `NewXxx(n initial keys)` + `Freeze()` for write-once strategies |
| `BenchmarkScaleSet` | Cost of growing by g keys via individual `Set` calls, including constructor |
| `BenchmarkScaleSets` | Cost of growing by g keys via a single `Sets` call, including constructor |
| `BenchmarkScaleGet` | Sequential `Get` throughput on a mapper holding n + g keys |
| `BenchmarkScaleGetParallel` | Concurrent `Get` throughput — exposes RWMutex lock contention at scale |
| `BenchmarkScaleMemory` | Live heap bytes consumed by a fully-built mapper, reported as `heap-bytes/op` |

```sh
make bench-scale   # run all six, 3 rounds, -benchmem
```

`BenchmarkScaleMemory` uses `runtime.ReadMemStats` to capture `HeapInuse` before and after construction (with `runtime.GC()` between) and surfaces the delta via `b.ReportMetric`.

## License

MIT — see [LICENSE](LICENSE).

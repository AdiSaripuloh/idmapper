package idmapper

import (
	"sort"
	"sync/atomic"
)

// mphfHashPair holds pre-computed hash values for CHD construction.
type mphfHashPair struct{ h1, h2 uint64 }

// CHD tuning constants.
const (
	mphfLambda  = 5       // target average bucket size (CHD parameter)
	mphfMaxDisp = 1 << 24 // displacement safety cap — never reached with good hashing
)

// MPHF is a write-once Mapper backed by a Minimal Perfect Hash Function built
// with the CHD (Compress, Hash, Displace) algorithm.
//
// During the build phase (before Freeze), keys are collected in a regular map.
// Freeze constructs the CHD structure, discards the build-phase map, and seals
// the mapper. After Freeze, Get and Gets use the MPHF directly:
//
//	hash(key) → slot in [0, slotN)  →  keys[slot] == key?  →  return ids[slot]
//
// slotN is the smallest prime ≥ n (number of keys). Using a prime slot count
// guarantees gcd(h2, slotN) = 1 for essentially all h2 values, ensuring every
// key can reach any slot during displacement search. Without this, a highly
// composite n (e.g. 10010 = 2×5×7×11×13) produces small-cycle h2 values that
// prevent CHD from placing some keys regardless of the displacement chosen.
//
// This is one hash computation + one array access + one key comparison with
// zero allocations and no locking — better cache behaviour than a hash map at
// large n because the flat key/ID arrays have no bucket indirection.
//
// Build phase is not safe for concurrent use.
// Set and Sets panic if called after Freeze.
// Freeze must be called before sharing the mapper across goroutines.
type MPHF[K comparable] struct {
	// Build phase — non-nil until Freeze.
	tmp  map[K]uint64
	next uint64

	frozen atomic.Bool
	hash   Hasher[K]

	// Read phase — populated by Freeze; zero until then.
	d     []uint32 // CHD displacement table; d[b] = displacement for bucket b
	r     uint64   // number of buckets = ceil(n / mphfLambda)
	n     uint64   // number of registered keys
	slotN uint64   // prime slot-array size; slotN ≥ n; modulus for slot arithmetic
	keys  []K      // flat key array indexed by slot; len = slotN
	ids   []uint64 // flat ID array indexed by slot;  len = slotN
}

// NewMPHF returns an MPHF mapper pre-populated with keys.
// Keys are assigned sequential IDs starting from 1 in slice order.
// Duplicates are ignored; the first occurrence wins.
// Call Freeze to build the MPHF and seal the mapper for concurrent reads.
func NewMPHF[K comparable](keys []K, h Hasher[K]) *MPHF[K] {
	m := &MPHF[K]{
		tmp:  make(map[K]uint64, len(keys)),
		next: 1,
		hash: h,
	}
	for i := 0; i < len(keys); i++ {
		if _, ok := m.tmp[keys[i]]; !ok {
			m.tmp[keys[i]] = m.next
			m.next++
		}
	}
	return m
}

// Freeze builds the CHD structure from all collected keys and seals the mapper.
// After Freeze, Get and Gets use the MPHF; Set and Sets panic.
// Calling Freeze on an already-frozen mapper is a no-op.
// Must be called before sharing the mapper across goroutines.
//
// Panics if the CHD displacement search exceeds the internal safety limit
// (extremely unlikely with well-distributed hash functions).
func (m *MPHF[K]) Freeze() {
	if m.frozen.Load() {
		return
	}
	n := uint64(len(m.tmp))
	if n > 0 {
		// Extract (key, id) pairs from the build-phase map.
		entryKeys := make([]K, n)
		entryIDs := make([]uint64, n)
		i := uint64(0)
		for k, id := range m.tmp {
			entryKeys[i] = k
			entryIDs[i] = id
			i++
		}
		m.n = n
		m.slotN = nextPrime(n) // prime ≥ n; ensures gcd(h2, slotN) = 1
		m.r = (n + mphfLambda - 1) / mphfLambda
		m.buildCHD(entryKeys, entryIDs)
	}
	m.tmp = nil // release build-phase map to GC
	m.frozen.Store(true)
}

// Set registers key if not already present and returns its ID.
// Not safe for concurrent use during the build phase.
// Panics if called after Freeze.
func (m *MPHF[K]) Set(key K) uint64 {
	if m.frozen.Load() {
		panic("idmapper: Set called on a frozen MPHF mapper")
	}
	if id, ok := m.tmp[key]; ok {
		return id
	}
	id := m.next
	m.tmp[key] = id
	m.next++
	return id
}

// Sets registers each key that is not already present and returns the IDs in
// input order. Not safe for concurrent use during the build phase.
// Panics if called after Freeze.
func (m *MPHF[K]) Sets(keys []K) []uint64 {
	if m.frozen.Load() {
		panic("idmapper: Sets called on a frozen MPHF mapper")
	}
	ids := make([]uint64, len(keys))
	for i := 0; i < len(keys); i++ {
		if id, ok := m.tmp[keys[i]]; ok {
			ids[i] = id
		} else {
			ids[i] = m.next
			m.tmp[keys[i]] = m.next
			m.next++
		}
	}
	return ids
}

// mphfH2 calls m.hash and enforces h2 != 0.
func (m *MPHF[K]) mphfH2(key K) (h1, h2 uint64) {
	h1, h2 = m.hash(key)
	if h2 == 0 {
		h2 = 1
	}
	return h1, h2
}

// Get returns the ID for key and whether it was found.
// Before Freeze: uses the build-phase map; not safe for concurrent use.
// After Freeze: uses the MPHF — one hash + one array access + one key compare;
// zero allocations, no lock, safe for concurrent use.
func (m *MPHF[K]) Get(key K) (uint64, bool) {
	if !m.frozen.Load() {
		id, ok := m.tmp[key]
		return id, ok
	}
	if m.n == 0 {
		return 0, false
	}
	h1, h2 := m.mphfH2(key)
	slot := (h1 + uint64(m.d[h1%m.r])*h2) % m.slotN
	if m.ids[slot] == 0 || m.keys[slot] != key {
		return 0, false
	}
	return m.ids[slot], true
}

// Gets returns the IDs for each key in input order; 0 for any key not found.
// Before Freeze: not safe for concurrent use (build phase).
// After Freeze: safe for concurrent use. Uses a 4-wide software-pipelined loop
// to overlap hash computation with memory loads, hiding cache-miss latency.
func (m *MPHF[K]) Gets(keys []K) []uint64 {
	ids := make([]uint64, len(keys))
	if !m.frozen.Load() {
		for i := 0; i < len(keys); i++ {
			ids[i] = m.tmp[keys[i]]
		}
		return ids
	}
	if m.n == 0 {
		return ids
	}
	i := 0
	for ; i+4 <= len(keys); i += 4 {
		// Phase 1: compute all four hashes (pure arithmetic, no m.keys access).
		h1a, h2a := m.mphfH2(keys[i])
		h1b, h2b := m.mphfH2(keys[i+1])
		h1c, h2c := m.mphfH2(keys[i+2])
		h1d, h2d := m.mphfH2(keys[i+3])
		// Phase 2: resolve slots via the displacement table (d is small, likely cached).
		sa := (h1a + uint64(m.d[h1a%m.r])*h2a) % m.slotN
		sb := (h1b + uint64(m.d[h1b%m.r])*h2b) % m.slotN
		sc := (h1c + uint64(m.d[h1c%m.r])*h2c) % m.slotN
		sd := (h1d + uint64(m.d[h1d%m.r])*h2d) % m.slotN
		// Phase 3: verify and collect IDs (reads m.keys/m.ids — may cache-miss).
		// ids[slot]==0 guards against false positives on unoccupied slots
		// whose zero-value key matches the query (IDs are 1-based).
		if m.ids[sa] != 0 && m.keys[sa] == keys[i] {
			ids[i] = m.ids[sa]
		}
		if m.ids[sb] != 0 && m.keys[sb] == keys[i+1] {
			ids[i+1] = m.ids[sb]
		}
		if m.ids[sc] != 0 && m.keys[sc] == keys[i+2] {
			ids[i+2] = m.ids[sc]
		}
		if m.ids[sd] != 0 && m.keys[sd] == keys[i+3] {
			ids[i+3] = m.ids[sd]
		}
	}
	for ; i < len(keys); i++ {
		h1, h2 := m.mphfH2(keys[i])
		slot := (h1 + uint64(m.d[h1%m.r])*h2) % m.slotN
		if m.ids[slot] != 0 && m.keys[slot] == keys[i] {
			ids[i] = m.ids[slot]
		}
	}
	return ids
}

// Len returns the number of keys registered in the mapper.
// Before Freeze: not safe for concurrent use (build phase).
// After Freeze: lock-free, safe for concurrent use.
func (m *MPHF[K]) Len() int {
	if !m.frozen.Load() {
		return len(m.tmp)
	}
	return int(m.n)
}

// buildCHD constructs the CHD displacement table.
//
// Algorithm outline:
//  1. Hash all n keys to r buckets using h1(key) % r.
//  2. Sort buckets largest-first (large buckets are harder to place; handling
//     them first yields smaller expected displacements overall).
//  3. For each bucket, search for the smallest displacement d such that
//     slot(key, d) = (h1 + d*h2) % slotN lands every key on a distinct,
//     unoccupied position.
//  4. Store d in the displacement table and mark those slots as finalized.
//
// slotN (a prime ≥ n) is used as the modulus so that gcd(h2, slotN) = 1 for
// essentially all keys, giving every key a full-length cycle over all slotN
// slots and preventing the build from getting stuck.
func (m *MPHF[K]) buildCHD(entryKeys []K, entryIDs []uint64) {
	n, r, slotN := m.n, m.r, m.slotN

	// Pre-compute (h1, h2) for every key once.
	hashes := make([]mphfHashPair, n)
	for i := uint64(0); i < n; i++ {
		hashes[i].h1, hashes[i].h2 = m.mphfH2(entryKeys[i])
	}

	// Count per-bucket sizes.
	bucketSize := make([]uint32, r)
	for i := uint64(0); i < n; i++ {
		bucketSize[hashes[i].h1%r]++
	}

	// Prefix-sum → bucket start offsets inside the members array.
	starts := make([]uint32, r+1)
	for i := uint64(0); i < r; i++ {
		starts[i+1] = starts[i] + bucketSize[i]
	}

	// Fill per-bucket member index arrays (indices into entryKeys/hashes).
	pos := make([]uint32, r)
	for i := uint64(0); i < r; i++ {
		pos[i] = starts[i]
	}
	members := make([]uint32, n)
	for i := uint64(0); i < n; i++ {
		b := hashes[i].h1 % r
		members[pos[b]] = uint32(i)
		pos[b]++
	}

	// Processing order: largest bucket first.
	order := make([]uint32, r)
	for i := uint32(0); i < uint32(r); i++ {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool {
		return bucketSize[order[i]] > bucketSize[order[j]]
	})

	// Output arrays sized to slotN (prime ≥ n); extra slots stay empty.
	m.d = make([]uint32, r)
	m.keys = make([]K, slotN)
	m.ids = make([]uint64, slotN)

	// finalized[slot] = true  once a slot has been permanently assigned.
	finalized := make([]bool, slotN)
	// tentOccupied uses a generation counter to detect within-bucket
	// collisions in O(1) without clearing the array between attempts.
	tentOccupied := make([]uint64, slotN)
	var tentGen uint64 // incremented once per displacement attempt

	for _, bi := range order {
		bStart := starts[bi]
		bSize := bucketSize[bi]
		if bSize == 0 {
			continue
		}

		var disp uint64
		for {
			if disp > mphfMaxDisp {
				panic("idmapper: MPHF build: displacement exceeded safety limit")
			}
			tentGen++
			ok := true

			for j := uint32(0); j < bSize; j++ {
				ei := members[bStart+j]
				slot := (hashes[ei].h1 + disp*hashes[ei].h2) % slotN
				if finalized[slot] || tentOccupied[slot] == tentGen {
					ok = false
					break
				}
				tentOccupied[slot] = tentGen
			}

			if ok {
				m.d[bi] = uint32(disp)
				for j := uint32(0); j < bSize; j++ {
					ei := members[bStart+j]
					slot := (hashes[ei].h1 + disp*hashes[ei].h2) % slotN
					m.keys[slot] = entryKeys[ei]
					m.ids[slot] = entryIDs[ei]
					finalized[slot] = true
				}
				break
			}
			disp++
		}
	}
}

// mphfHash returns two independent 64-bit hash values for key.
// h1 determines the CHD bucket; h2 is the per-key displacement multiplier.
// The two streams use different primes so their outputs are decorrelated even
// for keys that share long common prefixes (e.g. "g0"…"g9999").
// Using the same prime for both streams creates a linear relationship between
// h1 and h2 that degrades hash independence for structured key sets.
// h2 is guaranteed non-zero to prevent fixed-point displacement (when h2 = 0
// every displacement d gives the same slot).
func mphfHash(key string) (h1, h2 uint64) {
	h1 = 0xcbf29ce484222325                  // FNV-1a 64-bit offset basis
	h2 = 0x9e3779b97f4a7c15                  // Fibonacci hashing constant (Knuth)
	const prime1 uint64 = 1099511628211      // FNV-1a 64-bit prime
	const prime2 uint64 = 0xc4ceb9fe1a85ec53 // splitmix64 mixing constant
	for i := 0; i < len(key); i++ {
		c := uint64(key[i])
		h1 = (h1 ^ c) * prime1
		h2 = (h2 ^ c) * prime2
	}
	if h2 == 0 {
		h2 = 1
	}
	return h1, h2
}

// nextPrime returns the smallest prime p such that p >= n.
// Uses trial division; fast for n up to ~10^7 (sqrt cost ≈ 3162 iterations).
func nextPrime(n uint64) uint64 {
	if n <= 2 {
		return 2
	}
	if n%2 == 0 {
		n++
	}
	for !isPrime(n) {
		n += 2
	}
	return n
}

// isPrime reports whether n is prime via trial division.
func isPrime(n uint64) bool {
	if n < 2 {
		return false
	}
	if n == 2 {
		return true
	}
	if n%2 == 0 {
		return false
	}
	for i := uint64(3); i*i <= n; i += 2 {
		if n%i == 0 {
			return false
		}
	}
	return true
}

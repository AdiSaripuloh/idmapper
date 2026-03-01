// Package idmapper assigns stable, sequential integer IDs to comparable keys,
// designed to serve as an ID mapper for bitsets. It provides four concurrency
// strategies: RWMutex, Freeze, COW, and MPHF.
package idmapper

// cacheLineSize is the CPU cache line size used for struct padding.
// 64 bytes is correct for amd64 and arm64. Some architectures (e.g. s390x,
// some POWER) use larger cache lines; adjust if targeting those platforms.
const cacheLineSize = 64

// Mapper is the common interface implemented by all concurrency strategies.
type Mapper[K comparable] interface {
	// Set registers key if not already present and returns its assigned ID.
	// If key already exists, it is a no-op and returns the existing ID.
	Set(key K) uint64

	// Sets registers each key in the slice that is not already present and
	// returns the assigned IDs in the same order as the input.
	// Existing keys are left unchanged and their current IDs are returned.
	// More efficient than calling Set in a loop: RWMutex acquires the lock
	// once for the batch; COW makes a single map copy for all new keys.
	Sets(keys []K) []uint64

	// Get returns the ID assigned to key and true, or 0 and false if not found.
	Get(key K) (uint64, bool)

	// Gets returns the IDs for each key in the slice, in the same order.
	// Unknown keys are represented by 0, consistent with the zero-value contract.
	// More efficient than calling Get in a loop: the lock (or atomic load) is
	// acquired once for the entire batch.
	Gets(keys []K) []uint64

	// Len returns the number of keys registered in the mapper.
	Len() int
}

// Snapshotter is implemented by strategies that can return a point-in-time
// copy of the full key→ID mapping. Currently only COW supports this.
type Snapshotter[K comparable] interface {
	Mapper[K]
	GetSnapshot() map[K]uint64
}

// Hasher produces two independent 64-bit hash values for a key.
// h2 must be non-zero; the MPHF enforces this if the caller's implementation
// does not guarantee it.
type Hasher[K comparable] func(K) (h1, h2 uint64)

// StringHasher is the built-in Hasher for string keys (FNV-1a dual-stream).
var StringHasher Hasher[string] = mphfHash

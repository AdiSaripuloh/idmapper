# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [1.0.0] — 2026-03-01

### Added
- Four mapping strategies: `RWMutex`, `Freeze`, `COW`, `MPHF`
- Common `Mapper[K]` interface generic over `[K comparable]` key types
- `Hasher[K comparable]` type — user-provided hash function for MPHF with non-string keys
- `StringHasher` variable — built-in `Hasher[string]` backed by FNV-1a dual-stream hash
- Batch operations: `Sets` and `Gets` for amortised lock/copy cost
- `COW.GetSnapshot()` for atomic point-in-time views
- MPHF strategy with CHD algorithm — lock-free, map-free reads
- 4-wide software-pipelined `Gets` for MPHF cache-miss hiding
- Cache-line padding to prevent false sharing (`RWMutex`, `COW`)
- Comprehensive benchmarks: sequential, parallel, scale (n up to 1M)
- Memory profiling benchmarks via `runtime.ReadMemStats`
- Scale tests across 17 (n, g) pairs
- Fuzz tests for MPHF hash determinism and CHD correctness
- Godoc example tests for all four strategies
- "Custom Key Types" section in README with examples for `int` keys and custom hashers
- `golangci-lint` configuration
- `Makefile` with 8 targets (test, test-race, test-scale, bench, bench-scale, lint, check, help)
- GitHub Actions CI workflow

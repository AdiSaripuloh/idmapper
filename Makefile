.DEFAULT_GOAL := help

GO          := go
TEST_FLAGS  := -count=1 -short
BENCH_FLAGS := -bench=. -benchmem -count=3

# ---------------------------------------------------------------------------
# Testing
# ---------------------------------------------------------------------------

.PHONY: test
test: ## Run unit tests (scale entries with n ≥ 1 000 skipped via -short)
	$(GO) test $(TEST_FLAGS) ./...

.PHONY: test-race
test-race: ## Run unit tests with the race detector
	$(GO) test $(TEST_FLAGS) -race ./...

.PHONY: test-scale
test-scale: ## Run all scale tests without -short (n up to 1 000 000; can be slow)
	$(GO) test -count=1 -run=TestScale ./...

# ---------------------------------------------------------------------------
# Benchmarks  (-run=^$$ skips unit tests so only benchmarks execute)
# ---------------------------------------------------------------------------

.PHONY: bench
bench: ## Run all benchmarks (benchmem, 3 runs)
	$(GO) test $(BENCH_FLAGS) -run=^$$ ./...

.PHONY: bench-scale
bench-scale: ## Run scale benchmarks (New/Set/Sets/Get/GetParallel/Memory by n=1..1M)
	$(GO) test $(BENCH_FLAGS) -run=^$$ -bench=BenchmarkScale ./...

# ---------------------------------------------------------------------------
# Code quality
# ---------------------------------------------------------------------------

.PHONY: lint
lint: ## Run golangci-lint (vet, staticcheck, errcheck, gofmt, …)
	golangci-lint run ./...

# ---------------------------------------------------------------------------
# Composite
# ---------------------------------------------------------------------------

.PHONY: check
check: lint test-race ## Full pre-commit check (lint + race tests)

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------

.PHONY: help
help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-16s %s\n", $$1, $$2}'

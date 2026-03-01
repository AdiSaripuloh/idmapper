# Contributing

Thanks for your interest in contributing to idmapper!

## Getting Started

1. Fork and clone the repository.
2. Make sure you have Go 1.19+ installed.
3. Run `make check` to verify everything passes before making changes.

## Development Workflow

```sh
make lint      # run golangci-lint (vet, staticcheck, errcheck, gofmt, …)
make test      # run unit tests (scale tests capped under -short)
make test-race # run unit tests with the race detector
make check     # lint + test-race (run before committing)
make bench     # run all benchmarks
```

## Pull Requests

- Keep changes focused — one logical change per PR.
- Add or update tests for any new or changed behaviour.
- Run `make check` before pushing; CI runs the same checks.
- If your change affects performance, include benchmark results (`make bench`).

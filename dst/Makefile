# Build for dst. `make` / `make build` is the full gate:
#   fmt, ascii, dstgen freshness, vet, lint, modernize, tests, determinism.
# Toolchain: `mise install` at the repo root (pins in the root mise.toml). The commit hook is
# installed by the root Makefile (make install-hooks).

.PHONY: build test fmt-check vet lint modernize-check modernize ascii-check determinism generate gen-check

build: fmt-check ascii-check gen-check vet lint modernize-check test determinism

test:
	go test -race ./...

# Regenerate sum-type markers, tags, folds and state hashes (dst/cmd/dstgen).
generate:
	go generate ./...

# Generated files and dst.tags must match the annotations, and dst.tags must not have
# lost or renumbered a line since DSTGEN_BASE.
DSTGEN_BASE ?= mainline
gen-check:
	go run ./cmd/dstgen -check -base $(DSTGEN_BASE) ./...

# Determinism gate for the single-goroutine packages (rng, sched, clock, ...).
determinism:
	bash ../ops/dst/scripts/ci-determinism.sh

include ../ops/make/common.mk

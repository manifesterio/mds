# Build

Get a green local build, and look up what the root and module `make` targets run.

The modules are `dst/`, `shared/`, `controlplane/`, and `broker/`. Each has a `go.mod` and a `Makefile`. Shared rules live in `ops/make/common.mk`.

## Get a green build

1. Install [mise](https://mise.jdx.dev).
2. Start Podman. On macOS run `podman machine start`. Control plane and broker integration tests need Podman and never skip.
3. From the repo root:

```sh
mise install      # pins in mise.toml; postinstall runs make install-hooks
make generate     # *.pb.go and contract gen files are not in git
make build        # full gate, then bin/mdscontrolplane and bin/mdsbroker
```

`mise install` pins Go, golangci-lint, buf, Java, and minio. Broker `make build` runs the Kafka CLI e2e, which needs Java and minio.

Without mise, put Go, golangci-lint, and buf on `PATH`, then run `make install-hooks` and `make build`. You still need Podman for IT, and Java plus minio for broker e2e.

## What `make build` runs

Root order:

```text
check-deps
test-deps-guard
dst build
shared build
controlplane build
broker build
check-modules-standalone
bin/mdscontrolplane
bin/mdsbroker
```

| Module | `make build` steps |
|--------|--------------------|
| `dst` | fmt-check, ascii-check, vet, lint, modernize-check, `go test -race ./...`, determinism |
| `shared` | fmt-check, ascii-check, vet, lint, modernize-check, `go test -race ./...` |
| `controlplane` | generate, fmt-check, ascii-check, vet, lint, modernize-check, compile, unit tests, determinism, sim, sim-campaign, it |
| `broker` | fmt-check, ascii-check, vet, lint, modernize-check, compile, unit tests, sim (includes sim-campaign), it, e2e-kafka-cli, risk |

Broker has no module `determinism` target. Root `make determinism` runs `ops/broker/scripts/ci-determinism.sh`. See [determinism.md](determinism.md).

## Other root targets

| Target | What it does |
|--------|----------------|
| `make all` | `check-deps`, `test-deps-guard`, `generate`, `check-modules-standalone`, `build`, `test`, `sim`, `sim-campaign`, `determinism` |
| `make generate` | `make -C controlplane generate` (buf) |
| `make test` | unit tests in every module |
| `make it` | controlplane then broker IT. Needs Podman. |
| `make sim` | controlplane and broker sim |
| `make sim-campaign` | controlplane and broker seed campaigns |
| `make determinism` | dst, controlplane, then broker CI script |
| `make check-deps` | module import order via `ops/scripts/check-module-deps.sh` |
| `make check-modules-standalone` | each module tidy and build with `GOWORK=off` |
| `make install-hooks` | symlink `ops/githooks/pre-commit` into `.git/hooks` |
| `make image-cp`, `image-broker`, `image-all` | Podman images. See `deploy/container/README.md`. |

## Commit hook

The pre-commit hook runs `make build`. With mise present it runs under `mise exec`. Bypass once:

```sh
SKIP_BUILD_HOOK=1 git commit ...
```

## Standalone modules (`GOWORK=off`)

```sh
make check-modules-standalone
```

For a single module outside the workspace, run root `make generate`, then `make -C broker build` or `make -C controlplane build`.

## Broker risk gate

Broker `make build` ends with `ops/tools/riskscore`. The tool is stdlib only. It fails when a function is hard to change safely.

```text
risk = comp^2 * (1 - cov)^3 + comp    # cov in 0..1
```

Default threshold is 20. Override with `make -C broker build RISK_THRESHOLD=15`.

```sh
make -C broker risk           # gate on merged unit + sim + it coverprofiles
make -C broker risk-strict    # ignore baseline; fail on all offenders
make -C broker risk-baseline  # rewrite ops/broker/risk-baseline.json (review the diff)
```

The baseline file is `ops/broker/risk-baseline.json`. The gate fails on new offenders over the threshold, or on baselined scores that rose past tolerance (default 0.5). Improvements never fail. Commit baseline diffs on purpose.

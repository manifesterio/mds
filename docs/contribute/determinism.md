# Determinism gates

Run the import and construct checks that keep the sans-I/O core replayable. Package layout and how to write a machine live in `dst/README.md`. Example: `dst/examples/kv`.

## Run the gates

From the repo root:

```sh
make determinism
```

That runs, in order:

```text
make -C dst determinism
make -C controlplane determinism
bash ops/broker/scripts/ci-determinism.sh
```

Module shortcuts:

```sh
make -C dst determinism
make -C controlplane determinism
# broker: no make target
bash ops/broker/scripts/ci-determinism.sh
```

`dst` and `controlplane` also run their gate inside `make build`. Broker `make build` does not. Root `make determinism` is the broker gate.

The checker is `dst/cmd/determinismcheck`. The dst gate covers every package under `core/` and `sim/` except paths listed in `ops/dst/determinism-exclude.txt`, plus `verify/check` and `verify/check/faulty`.

## Allow-list and audit records

Deterministic packages may import only what their module allow-list permits. Third-party modules those packages actually run need a reviewed audit file at the exact `module@version` from `go.mod`. CI fails when a record is missing, so a version bump forces a re-audit.

| Module | Allow-list | Audit records | Gate script |
|--------|------------|---------------|-------------|
| broker | `ops/broker/determinism-allow.txt` | `docs/broker/dep-audits/` | `ops/broker/scripts/ci-determinism.sh` |
| controlplane | `ops/controlplane/determinism-allow.txt` | `docs/controlplane/dep-audits/` | `ops/controlplane/scripts/ci-determinism.sh` |
| dst | `ops/dst/determinism-allow.txt` | allow-list only | `ops/dst/scripts/ci-determinism.sh` |

Do not move the `docs/*/dep-audits/` paths. The CI scripts open them by path.

## Add or bump a third-party dependency

1. Confirm the gated packages need the module at runtime, not only through the generated API edge.
2. Produce a record:

```sh
ops/broker/scripts/audit-dep.sh <module>@<version>
```

Control plane uses the same script. Follow `.claude/skills/audit-go-dependency/SKILL.md`, then land the file under `docs/broker/dep-audits/` or `docs/controlplane/dep-audits/` as `<module path>@<version>.md`. See the `README.md` in those directories for the template.

3. Add any new import paths to the module allow-list.
4. Re-run the module or root determinism gate.

## Replay a failed seed

A failed campaign prints the command that re-runs the seed. Re-pin golden or trace hashes only when the behaviour change is intentional.

```sh
# control plane (default 300 seeds per scenario)
make -C controlplane sim-campaign
# or: go run ./cmd/cpsim -seeds 300 -scenario all

# broker (kafka scenario; default 300 seeds)
make -C broker sim-campaign
# or: go run ./cmd/mdssimulator -scenario kafka -seeds 300

# dst library example
# see dst/examples/kv and dst/README.md
```

See `dst/README.md` for more campaign flags and env vars.

## What the gate enforces

Product logic under test is a `machine.Machine`: one input in, outputs out, no real network, disk, wall clock, or goroutines. The `dst` kernel owns the seeded RNG, virtual clock, faults, assertions, and trace hash. Same seed, same run.

Rules the checker and reviews enforce:

- One RNG per run. No `math/rand` or wall-time seeds inside the subject.
- Virtual time only through the scheduler heap. No `time.Now` in core machines.
- Issue-time faults latched per site so drops and crashes replay stably.
- No map-range control flow in deterministic packages. Go randomizes map order.
- No goroutines or channels in gated packages. `runtime/live` and campaign runners stay outside the gate on purpose.

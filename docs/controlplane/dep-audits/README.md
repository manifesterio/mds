# Dependency audit records

Reference for third-party modules that the controlplane determinism gate may reach.

## Gate

`ops/controlplane/scripts/ci-determinism.sh` walks the import closure of:

```
./internal/runtime/...
./contract/...
./internal/controlplane/metadb/engine
./internal/controlplane/domain/topic
./internal/controlplane/domain/group
./internal/controlplane/domain/broker
./internal/controlplane/domain/rebalance
./internal/controlplane/replica
./spec
./internal/verify/simulator
```

For every third-party module in that closure, CI requires a file at exactly:

```
docs/controlplane/dep-audits/<module path>@<version>.md
```

The version string must match `controlplane/go.mod`. A version bump without a
matching record fails CI.

First-party modules under `github.com/manifesterio/mds/` need no record.

Test-only imports (`pgregory.net/rapid` in `_test.go`) are outside the gate.

## Edge cut

The walk stops at the generated API packages:

```
github.com/manifesterio/mds/controlplane/api/controlplane/shared/v1
github.com/manifesterio/mds/controlplane/api/controlplane/v1
github.com/manifesterio/mds/controlplane/api/controlplane/log/v1
```

Imports only used through that edge (grpc, genproto, `golang.org/x/{net,sys,text}`)
need no controlplane record. Deterministic packages may import the API for type
definitions.

Exception: `internal/controlplane/metadb` encodes and decodes log entries with
`google.golang.org/protobuf`. The simulator imports `metadb`, so the gate reaches
protobuf and requires a record.

Allow-list: `ops/controlplane/determinism-allow.txt`.

## Records in this directory

| File | In `controlplane/go.mod` | In gate closure |
|---|---|---|
| `google.golang.org/protobuf@v1.35.2.md` | yes (`v1.35.2`) | yes |
| `github.com/cockroachdb/pebble@v1.1.5.md` | yes (`v1.1.5`) | no (edge) |
| `pebble-indirect-modules.md` | indirect of Pebble | no |
| `github.com/prometheus/client_golang@v1.20.5.md` | yes (`v1.20.5`) | no (edge) |
| `github.com/twmb/franz-go/pkg/kmsg@v1.14.0.md` | no | no |

As of the current tree, the gate prints only
`google.golang.org/protobuf@v1.35.2`.

## New record

```bash
ops/broker/scripts/audit-dep.sh <module>@<version>
```

Follow `.claude/skills/audit-go-dependency/SKILL.md`. Template:

```markdown
# <module>@<version>

- Date / auditor:
- Requires Go:
- Verdict: ACCEPTED | ACCEPTED WITH RULES | REJECTED
- Import paths allowed (ops/controlplane/determinism-allow.txt):

## Findings
| Finding (file:line) | Matters? | Why |
|---|---|---|

## Rules for our code
1. ...

## Evidence
- determinismcheck output summary
- encode/decode twice, separate processes: identical?
```

# Dependency audit records

Reference for third-party modules that the broker determinism gate may reach.

## Gate

`ops/broker/scripts/ci-determinism.sh` walks the import closure of:

```
./internal/core/...
./internal/verify/simulator/...
./internal/wire/segmentformat/...
./internal/wire/kafkawire
./internal/broker/...
./spec
./internal/verify/speccheck
./internal/verify/speccheck/specbridge
```

For every third-party module in that closure, CI requires a file at exactly:

```
docs/broker/dep-audits/<module path>@<version>.md
```

The version string must match `broker/go.mod`. A version bump without a matching record fails CI.

First-party modules under `github.com/manifesterio/mds/` need no record.

## Edge cut

The walk stops at the generated gRPC API package
`github.com/manifesterio/mds/controlplane/api/controlplane/v1`. Imports only used
through that edge (grpc, protobuf, genproto, `golang.org/x/{net,sys,text}`) need
no broker record. Deterministic packages may import the API for type definitions.

Allow-list: `ops/broker/determinism-allow.txt`.

## Records in this directory

| File | In `broker/go.mod` | In gate closure |
|---|---|---|
| `github.com/twmb/franz-go/pkg/kmsg@v1.14.0.md` | yes (`v1.14.0`) | yes |
| `github.com/prometheus/client_golang@v1.20.5.md` | yes (`v1.20.5`) | no (edge) |
| `github.com/cockroachdb/pebble@v1.1.5.md` | no | no |
| `pebble-indirect-modules.md` | no | no |

As of the current tree, the gate prints only
`github.com/twmb/franz-go/pkg/kmsg@v1.14.0`.

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
- Import paths allowed (ops/broker/determinism-allow.txt):

## Findings
| Finding (file:line) | Matters? | Why |
|---|---|---|

## Rules for our code
1. ...

## Evidence
- determinismcheck output summary
- encode/decode twice, separate processes: identical?
```

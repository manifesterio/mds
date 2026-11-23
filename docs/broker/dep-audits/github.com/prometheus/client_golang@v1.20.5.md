# github.com/prometheus/client_golang@v1.20.5

- Date / auditor: 2026-10-18. Imports reviewed by hand after
  `ops/broker/scripts/audit-dep.sh` stopped on missing go.sum entries in its
  throwaway module.
- `broker/go.mod`: `github.com/prometheus/client_golang v1.20.5`
- Requires Go: 1.22.0 (module). Repo `go` directive: 1.26.0.
- Verdict: **REJECTED** for gated packages. **ACCEPTED** at the non-deterministic
  edge only.
- Import paths allowed (`ops/broker/determinism-allow.txt`): none.

## Import sites

```
broker/internal/system/production/metrics.go
broker/cmd/mdsbroker/main.go
```

`production.Metrics` turns broker facts into collectors. Gated packages emit facts
and do not import Prometheus.

## Findings

| Finding | Matters? | Why |
|---|---|---|
| `prometheus` imports `time`, `sync`, `sync/atomic`, `os`, `runtime`, `runtime/debug`, `runtime/metrics`, `expvar`, `encoding/json` | Yes | Collectors are global-mutable, lock-based, and read the clock and process. Banned in the gate. |
| `promhttp` serves HTTP (`net/http`) | Yes | Network I/O. Edge only. |
| Transitive: `beorn7/perks`, `cespare/xxhash/v2`, `munnerz/goautoneg`, `prometheus/client_model`, `prometheus/common`, `prometheus/procfs`, `google.golang.org/protobuf` | No | Exposition and process collectors. Outside the gate closure. |

## Rules for our code

1. Import only from `internal/system/production` and `cmd/`.
2. Keep Prometheus off the allow-list. If a gated package needs a metric, emit a fact.

## Evidence

- `ops/broker/scripts/ci-determinism.sh` does not list this module in the audit
  step. It is in `broker/go.mod` for the edge only.

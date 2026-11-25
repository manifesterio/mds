# github.com/cockroachdb/pebble@v1.1.5

- Date / auditor: 2026-10-20. `ops/broker/scripts/audit-dep.sh github.com/cockroachdb/pebble@v1.1.5`
  completed; determinismcheck exited 1, as expected for a storage engine.
- `controlplane/go.mod`: `github.com/cockroachdb/pebble v1.1.5`
- Requires Go: 1.22.0 (module). Repo `go` directive: 1.26.0.
- Verdict: **REJECTED** for gated packages. **ACCEPTED** at the edge only.
- Import paths allowed (`ops/controlplane/determinism-allow.txt`): none.

## Import sites

```
controlplane/internal/controlplane/metadb/engine/pebble
```

That package is the only production importer. Domain and replica code talk to
`engine.Engine`, not to Pebble. Tests may import the package under
`metadb/engine/pebble` for fixtures.

## Findings

| Finding (file:line) | Matters? | Why |
|---|---|---|
| `go` and channels in `table_cache.go:375`, `table_stats.go:46`, `compaction.go`, `db.go` | Yes | Background flush, compaction, table stats. Scheduling is nondeterministic. Gated packages must not import Pebble. |
| Imports of `os`, `sync`, `sync/atomic`, `time`, `runtime`, `runtime/pprof`, `unsafe`; package-level mutable vars; map ranges in `version_set.go` | Yes | Same. A fixed sequence of `Set`/`Delete` in one batch and an ordered iterator read still returns the same bytes regardless of compaction timing. |
| Transitive modules: `cockroachdb/{errors,fifo,logtags,redact,tokenbucket}`, `getsentry/sentry-go`, `gogo/protobuf`, `golang/snappy`, `kr/{pretty,text}`, `pkg/errors`, `rogpeppe/go-internal`, `stretchr/testify`, `golang.org/x/{exp,text}`, `DataDog/zstd` | No | Outside the gate closure. See `pebble-indirect-modules.md`. `DataDog/zstd` is cgo; `CGO_ENABLED=0 go build ./...` still passes. |

## Rules for our code

1. Import Pebble only from `internal/controlplane/metadb/engine/pebble`.
2. Commits use `pebble.NoSync`. Never trust the store after a crash. Restart restores
   a checkpoint into an empty directory and replays the log. Do not reuse a directory
   across incarnations.
3. Iterator key and value bytes are valid only until the next iterator call.
   The seam copies in `Get` and `Floor` (`append([]byte{}, ...)`). `Ascend` passes
   iterator buffers to the callback; callers must copy before the next iteration.
4. Logger is `silentLogger`. Store errors surface through `View`/`Update`.

## Evidence

- `ops/controlplane/scripts/ci-determinism.sh` does not list this module. It is outside
  the gated closure.
- Seam commit: `b.Commit(pebble.NoSync)` in `metadb/engine/pebble/pebble.go`.

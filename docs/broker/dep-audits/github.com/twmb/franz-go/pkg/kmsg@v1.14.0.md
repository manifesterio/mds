# github.com/twmb/franz-go/pkg/kmsg@v1.14.0

- Date / auditor: 2026-10-05. Claude subagent, then `ops/broker/scripts/audit-dep.sh`.
- `broker/go.mod`: `github.com/twmb/franz-go/pkg/kmsg v1.14.0`
- Requires Go: 1.24.0 (module). Repo `go` directive: 1.26.0.
- Verdict: **ACCEPTED WITH RULES**. Encode and decode only. No clock, randomness,
  goroutines, channels, sync, os, or net on the codec path.
- Import paths allowed (`ops/broker/determinism-allow.txt`):
  `github.com/twmb/franz-go/pkg/kmsg` (exact). `kmsg/internal/kbin` is covered by
  this review and is not importable by our code.

## Findings

| Finding (file:line) | Matters? | Why |
|---|---|---|
| `internal/kbin/primitives.go:647` `Reader.Span` returns `b.Src[:l:l]` | Yes | Decoded `[]byte` fields alias the input in `ReadFrom` and `UnsafeReadFrom` (`Record.Key`/`Value`, `Header.Value`, produce/fetch record bytes, tags). Capacity is capped, so appends do not grow into the input, but writes through either view change both. |
| `internal/kbin/primitives.go:850-856` `UnsafeString` (`unsafe` + `reflect.StringHeader`) | Yes, avoidable | Used only by `UnsafeReadFrom`. `ReadFrom` copies strings. |
| `api.go:405` range over `Tags.keyvals` map | No | Keys are collected and sorted with `sort.Slice` before encode. Map order never reaches the wire. |
| `generated.go` `reflect.DeepEqual` | No | Compares tagged-field structs to defaults to decide emission. |
| `api.go:30`, `generated.go:4` import `context` | No | Only on the `Requestor` interface. |
| `api.go:462` package-level var `ListClientMetricsResources` | No | A `Key` held in a `var`. Never written at runtime. |
| `RecordBatch.AppendTo` | Note | Writes `FirstTimestamp`, `CRC`, and `Length` exactly as supplied. |

## Rules for our code

1. `kafkawire.Framer` returns a fresh slice per frame and does not reuse or mutate it.
   Decoded messages alias that frame for as long as the message lives.
2. Call `ReadFrom` only. Never `UnsafeReadFrom`.
3. Supply every field kmsg writes verbatim: record-batch CRC (crc32c Castagnoli),
   lengths, timestamps (from `Event.Wall`).
4. Do not set unknown tags on tag keys the protocol already defines.

## Evidence

- determinismcheck findings match the table (reflect/unsafe/context, sorted map
  range, one package-level var).
- ProduceRequest v9/v11 and FetchResponse v11/v12/v16: encode byte-identical twice;
  decode twice is `reflect.DeepEqual`; re-encode round-trips; identical across
  separate processes (15/15).

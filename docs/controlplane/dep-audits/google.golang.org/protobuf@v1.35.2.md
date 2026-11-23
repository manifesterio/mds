# google.golang.org/protobuf@v1.35.2

- Date / auditor: 2026-11-05 (log entry codec in protobuf).
- `controlplane/go.mod`: `google.golang.org/protobuf v1.35.2`
- Requires Go: 1.21 (module). Repo `go` directive: 1.26.0.
- Verdict: **ACCEPTED WITH RULES** for the log entry codec in
  `internal/controlplane/metadb` (`entry.go`) only.
- Import paths allowed (`ops/controlplane/determinism-allow.txt`):
  `google.golang.org/protobuf/proto`,
  `google.golang.org/protobuf/reflect/protoreflect`.
  `internal/controlplane/architecture_test.go` permits `metadb` these two
  third-party packages and no others.

## Why this record exists

The gate does not follow imports into the generated API edge. `metadb.Encode` and
`metadb.Decode` call protobuf on every log entry. `internal/verify/simulator`
imports `metadb`, so `ops/controlplane/scripts/ci-determinism.sh` requires this
record.

## API surface we use

```go
proto.MarshalOptions{Deterministic: true}.Marshal(...)
proto.UnmarshalOptions{}.Unmarshal(...)
protoreflect.Message.GetUnknown / Range  // refuse unknown fields
```

Messages: `controlplane.log.v1.Entry` and nested messages from `log.proto` /
`shared.proto`. None has a map field, an extension range, or a `lazy` field.

## Findings

Source tree: `google.golang.org/protobuf@v1.35.2/`.
`determinismcheck` on `proto`, `internal/impl`, `encoding/protowire`,
`internal/detrand`, `internal/order`, `internal/strs`, `internal/errors`,
`runtime/protoiface`, `reflect/protoreflect`, `internal/flags`, `internal/filedesc`,
`internal/filetype`, `reflect/protoregistry`: 0 `go` statements, 0 channels or
`select`, 19 map ranges, 2 reflect map iterations, 225 package-level variables,
plus imports of `os`, `sync`, `sync/atomic`, `unsafe`, `reflect`, `io`,
`hash/fnv`, `compress/gzip`, `encoding/json`, `go/token`, `unicode`.

| Finding (file:line) | Matters? | Why |
|---|---|---|
| `internal/detrand/rand.go:38` `randSeed = binaryHash()` reads `os.Executable` at init | Yes, handled | Seed is fixed per binary, differs across builds. No user on the binary marshal path with `Deterministic: true`. Probe: builds with other seeds wrote the same entry bytes. |
| `internal/impl/message_reflect.go:110` detrand swaps neighbours in `Message.Range` order | No | Marshal does not use `Range`. Our code calls `Range` only in `metadb.hasUnknown`, which is a boolean and order-independent. |
| `proto/encode.go:176-177` generated messages take `impl.MessageInfo.marshal` | No | Fast path below. |
| `internal/impl/codec_message.go:148-174` sorts coder fields by number, oneofs last (`LegacyFieldOrder`) | No | Fixed order from the descriptor. For `Entry`: `id`, then the command. |
| `proto/encode.go:246-252` slow path uses `AnyFieldOrder` unless `Deterministic` | No, rule 1 | Generated messages skip this path. `Deterministic: true` forces `LegacyFieldOrder`. |
| `internal/impl/codec_map.go` map fields unsorted unless `Deterministic` | No, rule 2 | Log messages have no map field. |
| `internal/impl/encode.go:188-235` extensions | No | Log messages have no extensions. |
| `internal/impl/encode.go:66,94,99` size cache via `atomic.StoreInt32` | No | Encode builds a fresh message per call. |
| `MessageInfo.init`, `filedesc` lazy init, legacy `sync.Map` caches | No | Values depend on descriptor or Go type only. Ours are protoc-gen-go generated. |
| `internal/order/range.go` `sync.Pool` scratch slices | No | Sorted result. |
| `internal/impl/pointer_unsafe.go` `UnsafeEnabled = true` | No | Layout, not order or time. |
| Decode copies strings and `[]byte` (`codec_gen.go`) | No | Replica may reuse its input buffer. |
| `appendStringValidateUTF8`: proto3 strings must be valid UTF-8 | Yes, rule 3 | Marshal fails on invalid UTF-8. `metadb.Encode` panics (no error return). Decode returns `ErrCorrupt`. |
| Unmarshal keeps unknown fields; Marshal writes them back | Yes, handled | `metadb.Decode` refuses unknown fields at any depth (`hasUnknown`). |
| `GOLANG_PROTOBUF_REGISTRATION_CONFLICT` at registry init | No | Name conflict only. Our files do not conflict. |
| detrand spaces in protobuf error text and text/JSON formatters | No | Decode returns our `ErrCorrupt`. We never format messages as text. |
| Other map ranges in registry, `Equal`, `merge`, MessageSet | No | Not on Marshal/Unmarshal of our messages. |
| Deterministic marshaling not guaranteed stable across library versions (`proto/encode.go:34-55`) | Yes, rule 4 | Fine inside one build. Across versions, bytes may change. |
| Hidden defaults (timestamps, random ids, checksums) | No | None in the binary wire format. |

## Rules for our code

1. Marshal log entries only with `proto.MarshalOptions{Deterministic: true}`
   (`metadb.Encode` is the one site).
2. No `map` field, extension, or `lazy` field on messages under
   `controlplane.log.v1.Entry`. No other writer of log bytes. Keep `Entry` field
   numbers below 16 so the first byte is never the replica group marker (`0xC5`)
   or envelope marker (`0xC8`).
3. Every string in a logged command is valid UTF-8. gRPC checks inbound strings;
   simulator and tests use ASCII. `metadb.Encode` panics on invalid UTF-8.
4. Never compare or hash log entry bytes across builds, and never feed them into
   the state hash. State records keep hand-written codecs.
5. `metadb.Decode` refuses unknown fields and never panics on bad bytes
   (`FuzzDecode`).

## Evidence

- `go run github.com/manifesterio/mds/dst/cmd/determinismcheck` over the packages
  listed above: findings match the table. No goroutine, channel, time, or random
  number on the marshal path with `Deterministic: true`.
- Probe (temporary `main`, not committed): six builds with different
  `-ldflags -X` strings, one `-trimpath`, one `-race`, each run twice
  (16 processes). Same SHA-256 of entry bytes (`09174ce6e2e224a2...`),
  `stable=true`. Race build had a different detrand seed; entry bytes matched.
- `TestEntryBytesAreDeterministic` (metadb): every kind, 200 encodes, reverse
  field set order, deterministic and plain Marshal: same bytes.
- `TestEntryBytesPinned` fails if a protobuf bump changes pinned bytes.
- Simulator: `go run ./cmd/cpsim -seeds 300 -scenario all` kept the same five
  hashes after the codec change. Trace hashes fold the state hash and step
  counts, not entry bytes.

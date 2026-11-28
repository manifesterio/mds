# Architecture

MDS has two runtime pieces: a Kafka-protocol broker (`mdsbroker`) and a control
plane (`mdscontrolplane`). Clients speak Kafka to the broker. The broker stores
record batches in an object store and asks the control plane for metadata,
offsets, and classic consumer-group state.

## Runtime picture

    Kafka client
         |
         v
      mdsbroker  ----object get/put---->  object store (mem / S3-compatible)
         |
         | gRPC
         v
    mdscontrolplane  ----append/read---->  log store (mem / DynamoDB / Postgres)
         |
         +---- metadata / topics / groups / committed offsets

- Broker. Kafka wire, Produce and Fetch, object I/O. Forwards group and topic
  RPCs to the control plane. Every live broker names itself leader for every
  partition. Clients may still connect to the FindCoordinator target.
- Control plane. Replicated command log, topic and group state, segment commit
  and lookup, broker liveness leases.
- Stores. Demo `mds-all` uses in-memory log and objects. Production shape uses
  separate images and real backends. See [operations.md](operations.md).

## Module DAG

Four Go modules in one repo. A module may import only modules to its left.
`make check-deps` enforces that.

    dst <- shared <- controlplane <- broker

| Module | Role |
|---|---|
| `dst/` | Deterministic simulation library (no product words) |
| `shared/` | Cross-product helpers with no product words |
| `controlplane/` | Control plane service and client |
| `broker/` | Kafka-protocol broker |

## Core vs edge

Deterministic core packages take events and emit actions. No wall clock, no
goroutine scheduling, no Prometheus. Simulation and production share that core.

The edge (cmd, production adapters, metrics, real gRPC and object I/O) talks to
the outside world and turns core facts into logs and metrics. That split is why
DST can drive the same rules the binaries run. See
[contribute/determinism.md](contribute/determinism.md).

## Kafka protocol surface

If this table disagrees with
`broker/internal/wire/kafkawire/supported.go`, the code wins.

| API | Key | Versions |
|---|---|---|
| Produce | 0 | 3 to 9 |
| Fetch | 1 | 4 to 12 |
| ListOffsets | 2 | 1 to 7 |
| Metadata | 3 | 1 to 12 |
| OffsetCommit | 8 | 0 to 8 |
| OffsetFetch | 9 | 0 to 8 |
| FindCoordinator | 10 | 0 to 4 |
| JoinGroup | 11 | 0 to 7 |
| Heartbeat | 12 | 0 to 4 |
| LeaveGroup | 13 | 0 to 5 |
| SyncGroup | 14 | 0 to 5 |
| DescribeGroups | 15 | 0 to 5 |
| ListGroups | 16 | 0 to 4 |
| ApiVersions | 18 | 0 to 3 |
| CreateTopics | 19 | 0 to 7 |
| DescribeConfigs | 32 | 0 to 4 |

FindCoordinator serves group keys only. DescribeConfigs serves topics only and
returns an empty config list. Classic groups only (no KIP-848). Compression is
opaque bytes. gzip, snappy, lz4, and zstd are accepted. The broker never
decompresses. An unsupported API key or version closes the connection with no
error body. ApiVersions still answers `UNSUPPORTED_VERSION`.

CreateTopics ignores replication factor and configs. Fetch honors MinBytes and
MaxWait. No fetch sessions. ListOffsets supports earliest and latest only.
Earliest offset is 0. No retention delete or compaction in 0.1.0.

### Classic groups

Group state lives in the control plane. FindCoordinator hashes the group name
(FNV-1a 32-bit) over the live broker list sorted by node id. Transaction keys
get `COORDINATOR_NOT_AVAILABLE`. Manual OffsetCommit (generation `-1`, no
member) works only when the group has no members. `MEMBER_ID_REQUIRED` is not
used.

Intentional diffs vs Apache Kafka classic:

| Topic | Apache Kafka | MDS 0.1.0 |
|---|---|---|
| Unchanged follower rejoin in Stable / CompletingRebalance | Often answered at once. Leader rejoin starts a round | Any JoinGroup opens a new rebalance immediately |
| Empty round | Generation advances when Empty | Generation kept |
| Protocol choice | Vote among shared protocols | First protocol in the leader's list that everyone has |
| Protocol mismatch | Refuse immediately | Park, then end round with `INCONSISTENT_GROUP_PROTOCOL` |
| SyncGroup | Missing members get empty assignment | Unknown or duplicate assignment is an error |
| Manual / simple groups | Can create a "simple group" | No group created. Describe shows Dead. List omits |
| `MEMBER_ID_REQUIRED` | Challenge then second join | Empty id joins in one step |
| Static membership | Supported | Instance id stored, unused |

Smoke check with `ops/broker/scripts/e2e-kafka-cli.sh`.

## Terms (control plane)

Plain one-liners. Code map at `controlplane/internal/controlplane/doc.go`.

Layering is API -> Core -> Loglet: API is server + wire + api/; Core is package
`core` (SMR entrypoint; metadb materializes logged state); Loglet is executor.
See `controlplane/internal/controlplane/doc.go`.

- command / event. Client change stored in the log vs fact `Decide` makes at
  apply time (never stored).
- log / slot / entry. Ordered shared commands. Numbered place written once.
  Protobuf payload (or heartbeat/fence) at a slot.
- Decide / Apply / view. Pure `Decide(view, command) -> events`. `Apply` writes.
  View is read-only state.
- metadb / engine / schema / table. State DB on an ordered KV engine. Schemas own
  key prefixes.
- checkpoint / state hash / trim. Object-store snapshot at a slot. 64-bit digest
  of applied state. Delete covered log slots.
- segment / commit key / idempotency window / offset. Object-stored batches.
  Client key for CommitSegment dedupe. Remembered keys. Partition position.
- replica / role / epoch / fence / takeover. Node copy of the log. Writer vs
  standby. Writer generation. Stop old writer. Standby claim after silence.
- lease. Broker liveness heartbeat (memory only).
- group / member / generation / leader / assignment. Classic Kafka consumer group
  vocabulary. Rebalance machine is `domain/rebalance`.
- seed / campaign / oracle / trace hash. One number for a sim run. Many seeds.
  Post-run checks. Digest of every step.

# spec: the reference model of the control plane

`spec.ControlPlane` is a small program that gives the correct answer to every
control plane operation (topics, segments, consumer groups). It keeps plain maps
and slices, runs one operation at a time, and has no faults. It is a small
part of the code it checks. The idea is from the paper "Using Lightweight Formal
Methods to Validate a Key-Value Storage Node in Amazon S3" (Bornholt et al.,
SOSP 2021).

## The files

- `doc.go`: the package doc and the `Model` interface.
- `topics.go`: `ControlPlane`, and the rules for topics and segments.
- `groups.go`: the rules for the logged half of consumer groups (`GroupInstall`,
  `GroupAssign`, `GroupRemove`, `OffsetCommit`, `OffsetFetch`, describe, list).
- `hash.go`: `StateHash`, a hash of the committed segments and the topics. The
  broker simulator compares it with the hash of its simulated control plane.
  It keeps a prefix cache, so a commit near the end of the key order is cheap.
- `rebalance.go`: `spec.Rebalance`, the model of the classic group protocol
  (join, sync, heartbeat, leave, timers, role changes). It holds a
  `ControlPlane` as its `Store` for the logged half. The rules are in
  `docs/architecture.md` and the `domain/rebalance` package.
- `topics_test.go`, `hash_test.go`, `rebalance_test.go`: small examples of each rule.

## The independence rule

The model shares types with the code (`contract`), and never logic.
It imports none of `domain`, `metadb`, `engine` or `replica`, and no program of
the product imports it: `go list -deps ./cmd/mdscontrolplane` does not list it.
A test that compared the code with itself would prove nothing. Only tests and
the simulators import the model. The control plane simulator reaches it through
one file, `internal/verify/simulator/oracle.go`. The broker reaches it through
`broker/internal/verify/speccheck/specbridge` (C4 and the broker simulator) and
`speccheck/specsystem` (in-memory runs). This is the one control plane spec of
the repo.

The model is deterministic: no clock, network, goroutines or files, and no map
order reaches an answer. It is in the determinism gate (`make determinism`).

```go
m := spec.New()
done := m.Apply(contract.CreateTopic{Name: "t", Partitions: 2}) // spec.Model
```

## The tests that use the model

- The model's own tests (`topics_test.go`, `hash_test.go`, `rebalance_test.go`)
  check each rule with small examples.
- The control plane simulator (`internal/verify/simulator`, `make sim`, and
  `cmd/cpsim` for the campaign) runs the replicated control plane with faults
  and compares it with the model through `oracle.go`.
- The broker reaches the model through `speccheck/specbridge` and
  `speccheck/specsystem` (see above).

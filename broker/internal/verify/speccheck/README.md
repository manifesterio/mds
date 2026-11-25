# speccheck: bridge the broker to its specs

A **spec** is a small, correct model of a thing our code talks to. Specs are
not in this folder. Each one lives with the thing it describes (see the table
below). This folder holds the bridge and adapters: convert broker calls to the
control plane spec, and run production code on top of the specs as test fakes.

Terms: a **spec** is a short, fault-free program that shows the correct
behavior. An **oracle** is a checker we trust to say if a result is correct.
A **seed** fixes random choices so the same seed gives the same test.

## The three specs

| Spec | Where | What it models | Real code it checks |
| --- | --- | --- | --- |
| `objectstore.Spec` | `shared/objectstore/spec.go` | S3: put, get a byte range, delete, list | simulated object store; Podman IT for the S3 adapter |
| `spec.ControlPlane` | `controlplane/spec` | topics, segments (commit, find), consumer groups | the control plane, the simulated control plane |
| `spec.Broker` | `broker/spec/broker.go` | one Kafka partition: append, read | histories for the Kafka oracle (C4) |

There is one control plane spec in the repo. The broker kernel speaks
`core.IOCall`; `specbridge` converts a call to a `contract.Operation`, asks the
spec, and converts the answer back. It has no rule of its own.

The rules of each spec are in the comment above its type. Read that first.

## The remaining check

| Check | What is tested | Compared with | Real or simulated |
| --- | --- | --- | --- |
| C4 | the Kafka oracle in `internal/verify/simulator/kafka` | `spec.Broker`, `spec.ControlPlane` | simulation side |

Former lockstep suites (C1 broker answers, C3 S3 adapter vs `objectstore.Spec`,
and the `conformance` / `injectedbug` packages) were removed. Live backends are
covered by Podman integration tests elsewhere.

## Run the tests

```bash
go test ./spec ./internal/verify/speccheck/... -count=1     # broker spec, bridge, system adapters
(cd ../controlplane && go test ./spec -count=1)             # control plane spec
go test ./internal/verify/simulator/fakeservices/... -count=1   # fault tests of the simulated services
go test ./internal/verify/simulator/kafka -run C4 -count=1      # C4
```

Campaign-style env vars for the simulator use the `MDS` prefix via
`dst/explore/campaign` (`campaign.ReadEnv`):

| Variable | Meaning |
| --- | --- |
| `MDS_SEEDS=200` | run 200 checks |
| `MDS_SEED=17` | run only rapid seed 17 (replay a failure) |
| `MDS_CAMPAIGN=30s` | keep running new seeds for 30 seconds |

## Where things live

| Path | Purpose |
| --- | --- |
| `specbridge/` | the control plane spec behind `core.IOCall` |
| `specsystem/` | adapters that run production code on top of the specs |
| `../../../spec/` (`broker/spec`) | the broker partition spec |

A spec never lets the order of a Go map decide a result, and it uses no clock,
network, goroutines or files.

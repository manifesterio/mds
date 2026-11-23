# MDS docs

Start here when you join the repo.

## Run and operate

- [Getting started](getting-started.md). First Produce and Fetch with the `mds-all` demo image.
- [Architecture](architecture.md). Runtime pieces, module DAG, Kafka API surface, classic groups, terms.
- [Operations](operations.md). Local binaries, flags, metrics, container images.

## Contribute

- [Build](contribute/build.md). `mise`, `make build`, gates.
- [Determinism](contribute/determinism.md). DST, allow-list, audits, campaigns.

Determinism CI reads audit records under `docs/broker/dep-audits/` and
`docs/controlplane/dep-audits/`. Do not move those paths without updating the
scripts.

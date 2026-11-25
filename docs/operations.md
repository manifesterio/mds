# Operations

Run binaries, read metrics and logs, and build container images. For the demo
image, see [getting-started.md](getting-started.md). For the build gate, see
[contribute/build.md](contribute/build.md).

## Local binaries

```sh
mise install && make generate && make build
# bin/mdscontrolplane  bin/mdsbroker
```

Minimal mem pair (state lost on stop):

```sh
bin/mdscontrolplane -store mem -addr :8080 -metrics-addr :9101

bin/mdsbroker \
  -objects mem \
  -cp-url http://127.0.0.1:8080 \
  -listen 127.0.0.1:9092 \
  -advertise 127.0.0.1:9092 \
  -metrics-addr :9100
```

Give both the same `-run-id` (or `$MDS_RUN_ID`) when you join logs later.

### Control plane stores (`-store`)

| Value | Role |
|-------|------|
| `mem` | In-process. Demo and tests. Default. |
| `dynamolog` | DynamoDB log. Optional S3 checkpoints. |
| `postgres` | PostgreSQL log. Optional S3 checkpoints. |

Shared flags: `-addr` (`:8080`), `-metrics-addr` (`:9101`), `-log-level`,
`-run-id`, `-manifest-out`, `-broker-lease-ttl` (default 5s).

**dynamolog.** `-dynamo-endpoint` (empty means AWS), `-region`, `-table-prefix`,
`-table`, `-create-tables`, `-checkpoint-bucket`, `-checkpoint-prefix`,
`-node-name`, `-rank` (0 is the preferred appender), `-takeover-after`,
`-heartbeat`, `-pebble-dir`.

**postgres.** DSN via `$MDS_POSTGRES_DSN` or `-postgres-dsn` (never logged).
`-postgres-prefix` (default `mds_`), `-create-tables`. Checkpoint and rank flags
match dynamolog. All nodes share one DB. Do not point at a read replica.
`make -C broker it-postgres` needs Podman.

### Broker objects (`-objects`)

| Value | Role |
|-------|------|
| `s3` | Default. Real S3 or compatible. |
| `mem` | In-process. Single broker. |

S3 flags: `-s3-endpoint`, `-s3-bucket`, `-s3-prefix`, `-s3-region`,
`-s3-path-style`, `-s3-create-bucket`. Other flags: `-listen`, `-advertise`,
`-broker-id`, `-cp-url` (comma list for failover), `-metrics-addr` (`:9100`),
`-lease-interval` (keep CP `-broker-lease-ttl` about 3x this).

Production uses separate processes or images, a durable store, and a real
object store. Not `mem` or `mds-all`.

## Observability

| Service | Port flag | Default | Metric prefix |
|---------|-----------|---------|---------------|
| `mdsbroker` | `-metrics-addr` | `:9100` | `mdsbroker_` |
| `mdscontrolplane` | `-metrics-addr` | `:9101` | `mdscontrolplane_` |

Empty `-metrics-addr` disables the port. `/metrics` and `/healthz` on each.

`-run-id` / `$MDS_RUN_ID` stamps logs, `*_build_info`, and optional
`-manifest-out`. JSON logs on stderr (`run_id`, `service`, `version`,
`git_sha`). The broker writes about one line per segment event. The control
plane writes `cp_summary` each second at INFO while traffic is present, and
`cp_call` at DEBUG. Under high load, use `-log-level warn`.

Useful broker events are `segment_sealed`, `segment_put_done`,
`segment_committed`, `segment_failed`, `backpressure_on`, and
`backpressure_off`. Useful control-plane events are `cp_call`, `cp_summary`,
and `aws_attempt_failed`.

## Containers and cluster

| Image | Use |
|-------|-----|
| `mdscontrolplane:dev` | CP only (prod shape) |
| `mdsbroker:dev` | Broker only (prod shape) |
| `mds-all:dev` | CP + broker, mem (demo only) |

From repo root with Podman, run `make image-cp`, `make image-broker`, or
`make image-all`. See `deploy/container/README.md` for image detail,
`deploy/helm/mdscontrolplane` for the chart, and `deploy/README.md` for the
deploy layout.

kind + helm sketch:

```sh
helm upgrade --install cp deploy/helm/mdscontrolplane \
  --kube-context kind-mds --wait --timeout 3m
```

`deploy/container/broker-compose.yml` starts DynamoDB-compatible and S3
backends for integration tests. It is not a product deploy.

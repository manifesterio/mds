# Indirect modules pulled in by Pebble

- `broker/go.mod`: Pebble and its CockroachDB transitive set are absent.
- Canonical record:
  [`docs/controlplane/dep-audits/pebble-indirect-modules.md`](../../controlplane/dep-audits/pebble-indirect-modules.md)

Those modules appear under `controlplane/go.mod` as `// indirect` of
`github.com/cockroachdb/pebble v1.1.5`. None is linked into `mdsbroker`. This
path is kept for stable lookup.

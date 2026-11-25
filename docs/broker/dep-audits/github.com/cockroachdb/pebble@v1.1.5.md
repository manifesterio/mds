# github.com/cockroachdb/pebble@v1.1.5

- `broker/go.mod`: absent. No broker package imports `github.com/cockroachdb/pebble`.
- Verdict: **not applicable** to the broker module.
- Canonical record:
  [`docs/controlplane/dep-audits/github.com/cockroachdb/pebble@v1.1.5.md`](../../controlplane/dep-audits/github.com/cockroachdb/pebble@v1.1.5.md)

Pebble is a direct require of `controlplane/go.mod` (`v1.1.5`). The production
seam is `controlplane/internal/controlplane/metadb/engine/pebble`. This path is
kept so the filename stays stable; the broker determinism gate does not require it.

# github.com/twmb/franz-go/pkg/kmsg@v1.14.0

- `controlplane/go.mod`: absent. No controlplane package imports
  `github.com/twmb/franz-go/pkg/kmsg`.
- Verdict: **not applicable** to the controlplane module.
- Canonical record:
  [`docs/broker/dep-audits/github.com/twmb/franz-go/pkg/kmsg@v1.14.0.md`](../../broker/dep-audits/github.com/twmb/franz-go/pkg/kmsg@v1.14.0.md)

kmsg is a direct require of `broker/go.mod` (`v1.14.0`) and is on the broker
allow-list. This path is kept so the filename stays stable; the controlplane
determinism gate does not require it.

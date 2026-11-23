# Indirect modules pulled in by Pebble

Date: 2026-10-21 (versions checked against `controlplane/go.mod` at rewrite).

Scope: modules that appear as `// indirect` when
`github.com/cockroachdb/pebble v1.1.5` is required. They are edge dependencies.
Only `internal/controlplane/metadb/engine/pebble` (and therefore
`cmd/mdscontrolplane`) imports Pebble. Gated packages do not reach them, so none
needs a per-module file under `docs/controlplane/dep-audits/<module>@<version>.md`.
Pebble itself: `github.com/cockroachdb/pebble@v1.1.5.md`.

## Modules (from `controlplane/go.mod`)

| module | version in go.mod | notes |
|---|---|---|
| github.com/DataDog/zstd | v1.4.5 | cgo; not linked with `CGO_ENABLED=0` |
| github.com/cockroachdb/errors | v1.11.3 | linked in release `mdscontrolplane` |
| github.com/cockroachdb/fifo | v0.0.0-20240606204812-0bbfbd93a7ce | linked |
| github.com/cockroachdb/logtags | v0.0.0-20230118201751-21c54148d20b | linked |
| github.com/cockroachdb/redact | v1.1.5 | linked |
| github.com/cockroachdb/tokenbucket | v0.0.0-20230807174530-cc333fc44b06 | linked |
| github.com/getsentry/sentry-go | v0.27.0 | linked; no DSN, no `sentry.Init` in our code |
| github.com/gogo/protobuf | v1.3.2 | linked via cockroachdb/errors |
| github.com/golang/snappy | v0.0.4 | linked |
| github.com/kr/pretty | v0.3.1 | linked |
| github.com/kr/text | v0.2.0 | linked |
| github.com/pkg/errors | v0.9.1 | linked |
| github.com/rogpeppe/go-internal | v1.14.1 | linked (small part) |
| github.com/stretchr/testify | v1.11.1 | test graph; not linked in release |
| golang.org/x/exp | v0.0.0-20230626212559-97b1e661b5df | linked |
| golang.org/x/text | v0.43.0 | linked via sentry-go |

Licenses observed in module cache: Apache-2.0, MIT, BSD. Only `DataDog/zstd` uses cgo.

Related version bumps also present in `controlplane/go.mod`:
`golang.org/x/mod v0.42.0`, `golang.org/x/sync v0.24.0`,
`golang.org/x/tools v0.52.0`.

## Import chain

```
metadb/engine/pebble
  -> github.com/cockroachdb/pebble
  -> github.com/cockroachdb/errors
  -> errors/report, withstack
  -> github.com/getsentry/sentry-go
  -> golang.org/x/text
```

`sentry-go` is a client library. Without `sentry.Init` and a DSN it makes no
network call and starts no goroutine. Do not import these modules from gated
packages. Do not import them directly from our edge code either.

## Advisory note

`golang.org/x/text` is `v0.43.0` in `controlplane/go.mod`. That is above
`v0.39.0`, which fixed GO-2026-5970 (`norm.Form.Properties`). Re-run
`govulncheck` when Pebble or these indirects change.

## Evidence commands

```bash
cd controlplane
go list -m github.com/cockroachdb/pebble github.com/DataDog/zstd \
  github.com/cockroachdb/errors github.com/getsentry/sentry-go \
  github.com/rogpeppe/go-internal github.com/stretchr/testify golang.org/x/text
go list -deps ./cmd/mdscontrolplane | grep -E 'cockroachdb|sentry|gogo|snappy|x/text'
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o /tmp/mdscontrolplane-linux ./cmd/mdscontrolplane
go version -m /tmp/mdscontrolplane-linux
go run golang.org/x/vuln/cmd/govulncheck@latest -mode=binary /tmp/mdscontrolplane-linux
```

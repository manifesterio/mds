# DST redesign baseline

Recorded state before the DST API redesign. Nothing in this file is a target;
it is the measured starting point that later steps are compared against.

## 1. Commit

- `git rev-parse HEAD`: `674219bb8ce0bcc66d1a4be4f34227a726937a14`
- Branch: `slim/verification`

## 2. Gate results (GATES-FULL)

Each line was run from the repo root, one at a time, with its output captured to
`scratchpad/p0/gateN.log`. Durations are wall-clock seconds, rounded. The
`make -C dst build` line reused cached `go test` results for some packages.
No golden-hash failure was seen, so the run was not stopped early. Every line
passed (exit code 0). No `FAIL`, `--- FAIL` or `panic` text appeared in any log.

| # | Line | Result | Exit | Duration (s) |
|---|------|--------|------|--------------|
| 1 | `make -C dst build` | PASS | 0 | 14 |
| 2 | `make -C shared fmt-check ascii-check vet lint modernize-check test` | PASS | 0 | 10 |
| 3 | `make -C controlplane fmt-check ascii-check vet lint modernize-check compile test determinism sim` | PASS | 0 | 44 |
| 4 | `make -C broker fmt-check ascii-check vet lint modernize-check compile test sim` | PASS | 0 | 66 |
| 5 | `(cd broker && bash ../ops/broker/scripts/ci-determinism.sh)` | PASS | 0 | 3 |
| 6 | `make -C controlplane sim-campaign SIM_SEEDS=200` | PASS | 0 | 10 |
| 7 | `make -C broker sim-campaign SIM_SEEDS=200` | PASS | 0 | 7 |

### Gate 1: `make -C dst build`

Last 5 lines of `scratchpad/p0/gate1.log`:

```text
ok  	github.com/manifesterio/mds/dst/syncclock	(cached)
ok  	github.com/manifesterio/mds/dst/trace	(cached)
bash ../ops/dst/scripts/ci-determinism.sh
== determinismcheck
determinism gate passed
```

### Gate 2: `make -C shared fmt-check ascii-check vet lint modernize-check test`

Last 5 lines of `scratchpad/p0/gate2.log`:

```text
ok  	github.com/manifesterio/mds/shared/objectstore	1.903s
ok  	github.com/manifesterio/mds/shared/objectstore/s3	1.826s
?   	github.com/manifesterio/mds/shared/objectstore/s3/fakes3	[no test files]
ok  	github.com/manifesterio/mds/shared/runinfo	1.724s
?   	github.com/manifesterio/mds/shared/status	[no test files]
```

### Gate 3: `make -C controlplane fmt-check ascii-check vet lint modernize-check compile test determinism sim`

Last 5 lines of `scratchpad/p0/gate3.log`:

```text
== dependency audit records
ok       google.golang.org/protobuf@v1.35.2
determinism gate passed
go test ./internal/verify/simulator/...
ok  	github.com/manifesterio/mds/controlplane/internal/verify/simulator	(cached)
```

### Gate 4: `make -C broker fmt-check ascii-check vet lint modernize-check compile test sim`

Last 5 lines of `scratchpad/p0/gate4.log`:

```text
ok  	github.com/manifesterio/mds/broker/internal/verify/simulator/kafka	32.531s	coverage: 95.8% of statements
?   	github.com/manifesterio/mds/broker/internal/verify/simulator/truth	[no test files]
go run ./cmd/mdssimulator -scenario kafka -seeds 300
kafka            300 seeds    6027888 steps      897 crashes      443 exits   10.8s  hash 8ecf3fb5  ok
coverage: all 47 expected paths were reached
```

### Gate 5: `(cd broker && bash ../ops/broker/scripts/ci-determinism.sh)`

Last 5 lines of `scratchpad/p0/gate5.log`:

```text
ok  	github.com/manifesterio/mds/broker/internal/verify/simulator/internal/session	0.065s [no tests to run]
ok  	github.com/manifesterio/mds/broker/internal/verify/simulator/internal/span	0.063s [no tests to run]
ok  	github.com/manifesterio/mds/broker/internal/verify/simulator/kafka	1.038s
?   	github.com/manifesterio/mds/broker/internal/verify/simulator/truth	[no test files]
determinism gate passed
```

### Gate 6: `make -C controlplane sim-campaign SIM_SEEDS=200`

Last 5 lines of `scratchpad/p0/gate6.log`:

```text
log-faults       200 seeds    1123232 steps    20613 slots   12187 acked writes    2.1s  hash 5f631d74  ok
crash-restart    200 seeds    1192346 steps    15358 slots    6120 acked writes    1.4s  hash af4ff1fe  ok
leader-fencing   200 seeds    1130085 steps    21368 slots   10781 acked writes    1.7s  hash 11483154  ok
chaos            200 seeds    1322104 steps    12690 slots    4494 acked writes    1.8s  hash a230761c  ok
coverage: all 27 expected paths were reached
```

### Gate 7: `make -C broker sim-campaign SIM_SEEDS=200`

Last 5 lines of `scratchpad/p0/gate7.log`:

```text
go run ./cmd/mdssimulator -scenario kafka -seeds 200
kafka            200 seeds    4047060 steps      561 crashes      288 exits    6.6s  hash fff35c89  ok
coverage: all 47 expected paths were reached
```

## 3. Counts

All counts cover non-test `.go` files only (`*_test.go` excluded) under
`dst shared controlplane broker`, with `slim/` paths removed. Pattern counts are
matching lines. Commands use `/usr/bin/grep` (the shell's `grep` is a ugrep
wrapper), run from the repo root, and can be re-run as written.

Common prefix used below:

```sh
/usr/bin/grep -rE --include='*.go' --exclude='*_test.go' '<PATTERN>' dst shared controlplane broker | /usr/bin/grep -v '/slim/' | wc -l
```

| metric | count | command |
|--------|-------|---------|
| `) Fold(h *trace.Hash)` methods | 120 | pattern `\) Fold\(h \*trace\.Hash\)` with the prefix above |
| `) Tag() uint64` methods | 112 | pattern `\) Tag\(\) uint64` with the prefix above |
| marker methods `func (x) isX() {}` | 27 | pattern `func \([A-Za-z_]*\) is[A-Z][A-Za-z]*\(\) \{\}` with the prefix above |
| `StateHash() uint64` implementations (func declarations) | 30 | pattern `func .*StateHash\(\) uint64` with the prefix above |
| timer generation fields/checks | 39 | pattern `[gG]en(\+\+|\b.*TimerVersion)` with the prefix above |
| request-ID bookkeeping lines | 71 | pattern `nextReq|map\[machine\.RequestID\]` with the prefix above |
| `Route` functions / `Route:` fields | 18 | pattern `Route *func|Route: ` with the prefix above |
| product output case type declarations (SetTimer, CancelTimer, SendSelf, StopForRestart, ShutdownDone, RecordFact) | 12 | see per-file list below |
| lines of code, `dst/examples/kv` | 243 | see LOC command below |
| lines of code, `broker/internal/system` | 1240 | see LOC command below |
| lines of code, `controlplane/internal/controlplane/server` | 675 | see LOC command below |

All three LOC directories exist, so no substitute directory was used.

Product output case declarations (per-file list, matched by
`grep -rnE --include='*.go' --exclude='*_test.go' '^\s*(type\s+)?(SetTimer|CancelTimer|SendSelf|StopForRestart|ShutdownDone|RecordFact)\s+struct' dst shared controlplane broker | grep -v /slim/`):

- `controlplane/internal/runtime/action.go` (4): `SetTimer` (line 35), `CancelTimer` (43), `StopForRestart` (48), `ShutdownDone` (52)
- `controlplane/internal/controlplane/domain/rebalance/io.go` (3): `SetTimer` (118), `CancelTimer` (125), `SendSelf` (129)
- `broker/internal/core/action.go` (5): `SetTimer` (35), `CancelTimer` (42), `StopForRestart` (113), `ShutdownDone` (117), `RecordFact` (122)

LOC command (the same for all three directories, with the directory substituted):

```sh
find <DIR> -name '*.go' ! -name '*_test.go' -not -path '*/slim/*' -print0 | xargs -0 cat \
  | awk '/^[ \t]*$/{next} /^[ \t]*\/\//{next} {n++} END{print n}'
```

Blank lines and `//` comment-only lines are excluded. Block comments (`/* */`)
are not excluded, so the figure slightly overcounts.

## 4. Current golden pins (verbatim)

### 4.1 `dst/examples/kv/kv_test.go` (`goldenHashes`, lines 48-54)

```go
// goldenHashes pin the trace hash of seeds 1 to 3. A change here means the
// kernel or the example changed behavior.
var goldenHashes = map[string]uint64{
	"kv/1": 0x54fe7c2c349cf68f,
	"kv/2": 0x89317de789cbb955,
	"kv/3": 0x1ecfe9e5fbf7d1b9,
}
```

### 4.2 `controlplane/internal/verify/simulator/golden_test.go` (`goldenHashes` and `goldenWithoutGroups`, lines 10-24)

```go
// goldenHashes pins named scenario/seed traces. An accidental determinism
// change fails this test in review; bump a pin only after intentional review.
var goldenHashes = map[string]uint64{
	"basic/17": 0x337ae38013d6ceff,
	"chaos/42": 0x7d8e4e6f2cc72b2f,
}

// goldenWithoutGroups are the pins of the same seeds with the rebalance machines
// and the consumers left out (the run of before the consumer groups were added,
// T6.5). They moved once, with the others, when the host code moved to the dst
// kernel (T1.11c).
var goldenWithoutGroups = map[string]uint64{
	"basic/17": 0x798ef51af00c8c5f,
	"chaos/42": 0xef9a9a341d46a6ca,
}
```

### 4.3 `broker/internal/verify/simulator/kafka/testdata/golden_hashes.txt` (whole file)

```text
kafka seed=1 steps=12656 rand=57344 hash=96f1d45c08678e07
kafka seed=2 steps=10479 rand=43697 hash=d732377f643c6f58
kafka seed=3 steps=19579 rand=76745 hash=04d04a05d71ab2a0
kafka seed=4 steps=12936 rand=54837 hash=83ab46da2f6a7c92
kafka seed=5 steps=16357 rand=65576 hash=28172f1f48d94275
```

### 4.4 `grep -rnE '0x[0-9a-f]{12,16}' --include='*_test.go' dst shared controlplane broker`

Run with `/usr/bin/grep` and filtered for `slim/`, from the repo root.

```text
dst/trace/hash_test.go:10:	if got, want := trace.New().Sum(), uint64(0xcbf29ce484222325); got != want {
dst/trace/hash_test.go:21:		{"Uint64(0)", func(h *trace.Hash) { h.Uint64(0) }, 0xa8c7f832281a39c5},
dst/trace/hash_test.go:22:		{"Uint64(1)", func(h *trace.Hash) { h.Uint64(1) }, 0x89cd31291d2aefa4},
dst/trace/hash_test.go:23:		{"Uint64(255)", func(h *trace.Hash) { h.Uint64(255) }, 0x9016b196e349a31a},
dst/trace/hash_test.go:24:		{"Uint64(256)", func(h *trace.Hash) { h.Uint64(256) }, 0xe3757ca7d64666ea},
dst/trace/hash_test.go:25:		{"Byte(a)", func(h *trace.Hash) { h.Byte('a') }, 0xaf63dc4c8601ec8c},
dst/trace/hash_test.go:26:		{"String(ab)", func(h *trace.Hash) { h.String("ab") }, 0x9c6001d328a94690},
dst/trace/hash_test.go:73:		v := uint64(i) * 0x9e3779b97f4a7c15 >> (uint(i) % 64)
dst/machine/fact_test.go:20:// The broker kernel folded this fact to 0x433e150fa2bbbd6c (tags.go, fact and
dst/machine/fact_test.go:26:	if got := h.Sum(); got != 0x433e150fa2bbbd6c {
dst/machine/fact_test.go:27:		t.Fatalf("fact hash %#x, want 0x433e150fa2bbbd6c", got)
dst/examples/kv/kv_test.go:51:	"kv/1": 0x54fe7c2c349cf68f,
dst/examples/kv/kv_test.go:52:	"kv/2": 0x89317de789cbb955,
dst/examples/kv/kv_test.go:53:	"kv/3": 0x1ecfe9e5fbf7d1b9,
dst/campaign/run_test.go:415:		Hash:     0x1234abcd5678, Counts: Counts{Seeds: 4, Failed: 1}, Elapsed: 1500 * time.Millisecond,
dst/campaign/run_test.go:429:		Hash:     0x1234abcd5678, Counts: Counts{Seeds: 4}, Elapsed: 1500 * time.Millisecond,
dst/rng/rng_test.go:11:		0xbdd732262feb6e95,
dst/rng/rng_test.go:12:		0x28efe333b266f103,
dst/rng/rng_test.go:13:		0x47526757130f9f52,
dst/rng/rng_test.go:14:		0x581ce1ff0e4ae394,
dst/rng/rng_test.go:15:		0x09bc585a244823f2,
dst/rng/rng_test.go:16:		0xde4431fa3c80db06,
dst/rng/rng_test.go:17:		0x37e9671c45376d5d,
dst/rng/rng_test.go:18:		0xccf635ee9e9e2fa4,
dst/rng/rng_test.go:58:		{99, "swarm", 0xf59e9e20f6e82e0b},
dst/rng/rng_test.go:59:		{5, "x", 0x8b0268362b66c315},
dst/rng/rng_test.go:60:		{0, "", 0x7261e9031e12a9d7},
dst/rng/rng_test.go:95:	for i, want := range []uint64{0xe220a8397b1dcdaf, 0x6e789e6aa1b965f4, 0x06c45d188009454f} {
controlplane/internal/verify/simulator/golden_test.go:13:	"basic/17": 0x337ae38013d6ceff,
controlplane/internal/verify/simulator/golden_test.go:14:	"chaos/42": 0x7d8e4e6f2cc72b2f,
controlplane/internal/verify/simulator/golden_test.go:22:	"basic/17": 0x798ef51af00c8c5f,
controlplane/internal/verify/simulator/golden_test.go:23:	"chaos/42": 0xef9a9a341d46a6ca,
broker/internal/broker/harness_test.go:31:	e.state += 0x9e3779b97f4a7c15
broker/internal/broker/harness_test.go:33:	z = (z ^ z>>30) * 0xbf58476d1ce4e5b9
broker/internal/broker/harness_test.go:34:	z = (z ^ z>>27) * 0x94d049bb133111eb
```

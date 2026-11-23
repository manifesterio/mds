# Verification slimming plan (evidence-based)

Branch `slim/verification`. Analysis only: no existing code or test was changed. Everything
here was measured on a clean tree (`make generate` run, all packages build, all tests pass)
on 2026-11-22. Raw outputs, the scripts and the mutant lists are in `slim/evidence/`
(see "How to reproduce" at the end).

Legend: **[M]** measured, **[E]** estimated (reasoning, not a run).
Line counts are `wc -l` of tracked files (generated `.pb.go` excluded).

## 1. Summary (sorted by lines saved / risk)

| # | Action | Lines saved | Risk | Effort | Evidence in one line |
|---|--------|------------:|------|--------|----------------------|
| A1 | Delete the fake-service "parity" tests, generator code and golden transcripts (`fakeservices/internal/servicetest/parity*.go`, `fakeservices/testdata/parity_*.txt`) | **2,062 [M]** | low | S | They compare the new fakes with old fakes that no longer exist. In 24 mutants of the fake code, parity killed 16, and every one of them was also killed by another layer (0 unique). |
| A2 | Delete the control-plane half of the broker lockstep suite (`speccheck/conformance/{controlplane,gencontrolplane,controlplane_model}.go`, `injectedbug/controlplane.go`, and their test code) | **~1,230 [M]** (1,284 by cascade, minus ~50 object-store test lines that need rewriting) | low | S-M | `ControlPlaneLockstep` has no real subject: only spec-vs-spec and bug-wrapped spec. The real control plane is checked by `controlplane/test/conformance`. On 24 mutants of the CP spec it killed 5, 0 unique. Tests still pass after deletion in a scratch copy. |
| A3 | Delete the `echo` toy scenario (`simulator/echo`, plus its registration in `cmd/mdssimulator`) | **~1,330 [M]** (1,315 + 21) and -9.4 s test time [M] | low | S | Second, hand-written toy broker. 0 unique statements of core code, 0 unique kills in 91 mutants (core + infra + dst). The `make sim-campaign` gate already runs only the `kafka` scenario. |
| A4 | Delete `kafka/capdiff_test.go` (`TestCapChangeIsExplainedBySealReasons`) | **84 [M]**, and **-75 s [M]** of the 119 s `simulator/kafka` run | low | XS | One-off investigation ("decision 52") that compares two campaigns. The partition cap is already pinned by `InjectedBugNoPartitionCap` (caught by unit, sim and the commit-limit tests). |
| A5 | Cut `dst/examples/kvchaos` and `dst/examples/kvsim`, keep `kv` as the documented example | **630 [M]** (all of `examples`: 1,049) | low | S | Examples kill 6 of 22 dst mutants, 0 unique. Framework statements covered only by examples: 0 (the 212 "unique" statements are the example code itself). Docs `dst/README.md` and `docs/contribute/determinism.md` must be edited. |
| A6 | Mechanical clean-ups: dead exports, `campaign.Run` alias, `Loop.Self`, `runinfo.LogAndExit`, cmd tests that re-run campaigns the gate already runs (`cmd/cpsim/main_test.go`) | ~130 [E] | low | XS | `deadcode -test` (below). `cpsim` test: 24 kills, 0 unique. |
| A7 | Collapse the fake clock stack (`dst/sched` + `dst/clock` + `dst/syncclock`) into one small fake in `shared/clock` | ~450-550 [E] | low | M | Used only by `shared/clock.Fake` (1 external test file) and `dst/live` tests. 0 coverage from broker or control-plane tests. |
| **Total, low risk (A1-A6)** | | **~5,500 [M+E]** (A1-A5 are 5,336 measured) | | | |
| **With A7** | | **~6,000** | | | |
| B1 | Trim `domain/rebalance` unit tests to the ones with unique value | ~1,100 [E] | medium | M | Marginal coverage vs everything else: 5 statements. Rebalance mutants are killed by `TestRebalanceMachine` and the simulator. No unique kill in the sample. Needs a bigger mutant run first. |
| B2 | Broker unit: drop `conformance_test.go`, `conformance_rules_test.go`, `fetch_merge_test.go`, `timing_test.go` | ~900 [E] | medium | S | 0 marginal statements, 0 sole kills (they kill 18, 14 and 6 mutants but each is also killed elsewhere). They are the fast spec-vs-broker check, so keep until a bigger run agrees. |
| B3 | Control-plane unit tests with 0 marginal coverage and no sole kill (listed in 4.8) | ~700 [E] | medium | M | See 4.8. |
| **Total with B** | | **~8,700 [E]** | | | |

What this is relative to the 55k lines of verification: low-risk 10-11%, with medium-risk
tier about 16%.

Keep (section 5): broker unit tests for groups/offsets/create/configs, `simulator/kafka` and
`simulator` (cp), `controlplane/test/conformance`, the specs and their own tests, dst kernel,
campaign and spectest tests, the mutate tool.

## 2. Inventory

Lines are `prod / test` [M]. "Prod" for a verification package means non-`_test.go` code
(full table in `evidence/inventory_lines.txt`, per-directory in `evidence/lines_by_dir.txt`).

| Component | Prod | Test | What it verifies | How it runs | Runtime [M] |
|---|---:|---:|---|---|---|
| dst/kernel | 2,232 | 1,863 | the deterministic simulation kernel | `make -C dst test` (`go test -race ./...`), `determinism` script | <1.2 s |
| dst/campaign | 1,368 | 1,211 | seed runner, reporting, env parsing | same | <0.5 s |
| dst/check (spectest, faulty) | 1,430 | 1,524 | lockstep/spec harness, fault wrapper | same | <0.8 s |
| dst/live | 458 | 578 | production loop for a machine | same | <0.5 s |
| dst/examples (kv, kvchaos, kvsim) | 776 | 273 | documentation examples | same | <0.4 s |
| dst other (assert, clock, sched, syncclock, ...) | 1,663 | 1,039 | small kernel parts | same | <1 s |
| shared (clock, objectstore, spec, s3, runinfo, ...) | 2,366 | 1,357 | object store spec, S3 adapter, fakes3 | `make -C shared test`; `it` needs Podman | <2.3 s |
| broker/internal/broker | 3,151 | 3,942 | the broker state machine | `make -C broker test` (commit hook) | 4.1 s |
| broker/internal/verify/simulator/kafka | 1,732 | 1,891 | broker + fakes in simulation, oracle C4 | `make sim`, `mdssimulator -scenario kafka -seeds 300` | **119 s** (75 s one test) |
| simulator/echo | 1,060 | 255 | toy broker scenario | `make sim` (go test only) | 9.4 s |
| simulator/cluster, history, truth, preset, session, span | ~1,030 | ~900 | simulation plumbing | `make sim` | <1 s |
| simulator/fakeservices (+fakenetwork, servicetest) | 1,771 (+1,433 testdata) | 1,430 | spec+faults as fake services, parity | `make sim` | ~2 s |
| speccheck/conformance | 1,821 | 956 | lockstep suites for object store / CP / broker specs | `make test` | 9.2 s |
| speccheck/injectedbug | 702 | 133 | proves the lockstep suites can fail | `make test` | 15.9 s |
| speccheck/s3conform, specbridge, specsystem | 354 | 278 | S3 adapter vs spec; spec as production seams | `make test` | 13.6 s (s3conform) |
| broker/spec | 66 | 182 | broker spec vs naive model | `make test` | 0.5 s |
| broker/test/integrationtest | 12 | 987 | whole stack, Podman | `make it` | not run |
| controlplane domain+replica+metadb+executor+server (+wire, metrics) | 9,855 | 7,620 | the control plane core | `make -C controlplane test` | 0.4-1.3 s per package |
| controlplane/internal/verify/simulator | 3,270 | 253 | CP in simulation with oracle | `make sim` | 20 s |
| controlplane/cmd/cpsim | 85 | 107 | campaign CLI | `make sim-campaign` (300 seeds) + test | 10 s (test) |
| controlplane/spec | 1,370 | 951 | the CP spec | `make test` | 0.3 s |
| controlplane/test/conformance | 0 | 2,809 | real stores (memory, pebble, gRPC, replica) vs spec; rebalance machine | `make test`, `make spec-long` | 10 s |
| controlplane/test/integration, s3store | 5 | 1,104 | Podman | `make it` | not run |
| ops: e2e-kafka-cli.sh, postgres-test.sh, determinism scripts | 1,531 | - | e2e with Kafka CLI, determinism gates | `make build` | not run (Podman) |
| ops/tools/riskscore | 672 | 242 | coverage-weighted risk gate | `make risk` | not evaluated |
| controlplane/ops/tools/mutate + `mutants.txt` | 322 + 264 | - | mutation check of 16 bespoke bugs | `make mutants` (NOT in `make build`) | not run |

How verification is triggered: there is no `.github/` directory, so the only gate is the
pre-commit hook (`ops/githooks/pre-commit`), which runs the root `make build` (all four
modules: fmt, vet, lint, tests, determinism, sims, 300-seed campaigns, and with Podman the
`it` targets, `e2e-kafka-cli`, `risk`). Nothing runs mutation.

Not analysed (skipped as asked): everything that needs Podman; `e2e-kafka-cli.sh` (798
lines); `riskscore`.

## 3. Evidence

### 3.1 How mutants were made

No mutation tool was installed, so I wrote one (`evidence/tools`): single-line mutations
(flip `==`, `<`, `&&`, delete call statement, negate `if`, `+1`/`-1`, `return true/false`)
chosen at random over the core code, applied in throwaway copies of the tree, and each
mutated tree is run through every layer. Plus:

- the 9 `InjectedBug*` of the broker (forced through an env var in the copy),
- the 16 bespoke mutants of `ops/controlplane/mutants.txt`,
- mutants in the verification code itself (fakes, faulty, cluster, dst kernel and the specs).

Sample: 64 broker (55 line + 9 bugs), 52 control plane (36 line + 16 bespoke), 24 fake/infra,
28 dst, 24 CP spec, 10 object-store spec. Non-compiling mutants were dropped (8). "Killed" =
any package of the layer fails. Mutants that survive every layer may be equivalent. The
sample is small and random, so read the matrices as an indication, not a proof. A heavy
machine load (load average above 50 for part of the run) could in principle turn a test
timeout into a "kill"; no build failures or timeouts were recorded in the kill sets.

### 3.2 Catch matrix, core code [M]

Broker (`evidence/catch_matrix_core.txt`): 53 line mutants evaluated, 48 killed, 5 survived.

| Layer | Kills | Unique kills |
|---|---:|---:|
| unit (broker, core, wire, system, cmd) | 35 | **11** |
| simulator/kafka | 33 | **13** |
| echo + cluster + `cmd/mdssimulator` tests | 11 | **0** |

The 9 planted `InjectedBug` mutants: unit 9/9, simulator/kafka 9/9, mdssimulator/echo layer
8/9 (misses `TimerFromLastBatch`). Each bug has a dedicated unit test. So for these 9 each
layer is individually sufficient, which is the strongest overlap in the repo.

Control plane (49 evaluated, 41 killed, 8 survived; includes the 16 bespoke mutants):

| Layer | Kills | Unique kills |
|---|---:|---:|
| unit (domain, replica, metadb, server, ...) | 25 | 3 (`cL15`, `cL22`, `per-replica-window`) |
| internal/verify/simulator | 34 | **9** |
| test/conformance | 26 | **0** |
| cmd/cpsim | 24 | **0** |

All 16 bespoke mutants are killed: conformance 13, simulator 12, unit 11, cpsim 11. Only one is killed by a single layer (`per-replica-window`, unit).

### 3.3 Catch matrix, the verification layers themselves [M]

(`evidence/catch_matrix_verification_layers.txt`)

Fakes and infra (mutants in `dst/check/faulty`, `fakeservices`, `fakenetwork`, `cluster`;
22 evaluated, 19 killed):

| Layer | Kills | Unique |
|---|---:|---:|
| parity (`servicetest`) | 16 | **0** |
| fakeservices unit tests | 13 | 0 |
| faulty unit tests | 4 | 0 |
| simulator/kafka | 17 | 0 |
| echo + cluster | 12 | 0 |

dst framework (27 evaluated, 22 killed):

| Layer | Kills | Unique |
|---|---:|---:|
| dst own tests (without examples) | 21 | **11** |
| dst examples | 6 | 0 |
| broker kafka sim | 8 | 0 |
| echo + cluster | 5 | 0 |
| cp simulator | 7 | 0 |

CP spec (24 evaluated, 22 killed): spec own tests 22 (3 unique), `test/conformance` 19 (0
unique), simulator 7, rebalance unit 3. The broker's CP lockstep suite plus its injectedbug
bugs: 5 kills, 0 unique (`evidence/` file `catch_matrix_verification_layers.txt` plus the
extra run described in A2).

Object-store spec (10 evaluated, 9 killed): spec own tests 9 (3 unique), `speccheck`
packages together 5 (0 unique), fakeservices 6, simulator/kafka 6.

### 3.4 Statement coverage overlap [M]

Coverage of the core logic packages by every test package separately (`-coverpkg`), then
unique statements per layer (`evidence/coverage_overlap_*.txt`).

Broker core (1,812 statements; 94.2% covered by anything):

| Layer | Covers | Unique |
|---|---:|---:|
| unit `internal/broker` | 81.0% | **429** (groups_offsets 118, groups 92, create 58, groups_admin 36, ops 28, configs 28, coordinator 23) |
| `mdssimulator` cmd test (kafka campaign) | 63.4% | 0 |
| simulator/kafka | 62.2% | 4 |
| production + cmd/mdsbroker | 14.8% / 5.3% | 0 |
| echo | 11.3% | **0** |
| cluster | 6.1% | 0 |
| wire/core units | 26.3% | 70 |

So the broker simulator does not exercise groups, offsets, create-topics or configs. Those
are verified by unit tests only. That is the reason to keep the broker unit tests.

Control plane core (3,935 statements; 87.5% covered):

| Layer | Covers | Unique |
|---|---:|---:|
| cmd/cpsim | 64.1% | 2 |
| internal/verify/simulator | 63.9% | 0 |
| test/conformance | 59.3% | 22 |
| domain unit tests (all 4 packages) | 38.6% | 30 |
| replica unit | 24.5% | 19 |
| server unit | 31.5% | 53 |
| other unit (metrics, logtest, wire, runtime, metadb, executor) | 37.0% | 295 (controlplanemetrics 102, logtest 85) |

Per test file, marginal statements against all other tests (`evidence/coverage_marginal_*`):
domain/rebalance 5 in total, domain/group 2, domain/topic 10, replica 16, metadb 14, executor
9, server 38. Most control-plane unit files add 0 statements.

dst (2,598 statements): own tests cover 84.9%; the broker and cp tests cover 67.0%
(1,740). Statements covered by dst's own tests only: 488 (determinismcheck 85, kernel 59,
spectest 64, campaign 60, sched 57, golden 50, syncclock 41, clock 22). Examples add 0
framework statements.

### 3.5 Duplication [M]

`dupl -t 60` over the repo: 78 pairs, about 1,100 lines counting both sides
(`evidence/dupl_pairs.txt`). Nothing big: the largest are `broker/internal/core/fold.go` vs
`controlplane/internal/runtime/fold.go` (68 and 58 lines, production code), `kafka_test.go` vs
`echo_test.go` (48 lines, disappears with A3), and `fold_test.go` against itself.

The three planned "two copies of a fake" cases were checked and are already clean:
the control-plane and object-store fakes are `dst/check/faulty` around the real specs
(`fakeservices/doc.go`), there is one CP spec, and `specbridge` adds no rule. The remaining
copy is the parity transcripts (A1) and the broker-side CP lockstep (A2).

### 3.6 Dead code [M]

`deadcode -test` per module: only `campaign.Run` (alias of `RunSeeds`, "deleted at T1.11"),
`live.Loop.Self`, `runinfo.LogAndExit`, two test-only `Fold` methods in `kernel/harness_test.go`
(client/spec/contract entries are cross-module false positives). The `unused` lint rule is already
on, so unexported dead code cannot exist. Non-test code reachable only from tests
(`deadcode` without `-test`): `speccheck/conformance` 112 functions and `injectedbug` 48
(both test-support), `servicetest` 46 (parity helpers, A1).

## 4. Actions in detail

### A1. Parity tests (2,062 lines [M])
Files: `fakeservices/internal/servicetest/parity.go` (438), `parity_test.go` (191),
`fakeservices/testdata/parity_{controlplane,network,objectstore}.txt` (590 + 459 + 384).
They run a scripted case against the new fake and compare the transcript with one saved from
the *old hand-written fakes*, which were deleted in T1.15. Its own comment says a deliberate
kernel change "must re-pin them by hand". They are a change detector, not a correctness
check. `servicetest/harness.go` stays (used by `fakeservices` and `fakenetwork` tests).
Deleted in a scratch copy: `go vet` clean, `fakeservices/...` tests pass.
Lost: a pin on the exact number of random draws and the digest of every reply. That is also
pinned by `kafka/golden_test.go` and `dst/golden`. Correctness lost: none (0 unique kills in
24 infra mutants; every parity kill was also a simulator/kafka or fakeservices kill).
Order: first. Also drop the sentence in `fakeservices/doc.go` and the `ci-determinism` docs if they mention parity.

### A2. Broker control-plane lockstep (~1,230 lines [M])
`ControlPlaneLockstep` is called only by its own tests and by `injectedbug` (spec wrapped
with bugs). It is never called with a real control plane, and `speccheck/README` says CP
store conformance lives in `controlplane/test/conformance`. Deleting
`controlplane.go` (262), `gencontrolplane.go` (221), `controlplane_model.go` (169),
`injectedbug/controlplane.go` (283) and cascading the compile errors removed 1,284 lines in
the scratch copy (`conformance_test.go` -18, `coverage_test.go` -145, `describe_test.go` -54,
`format.go` -10, `subject.go` -27, `injectedbug_test.go` -95). `go test ./internal/verify/speccheck/...` passes. Because the cascade
dropped a few object-store test functions that mixed both halves, budget about 50 lines to
rewrite; net saving about 1,230.
What is lost: the "naive CP model vs CP spec" comparison and 8+ planted CP bugs that show the
CP lockstep can fail. Evidence that it is redundant: of 24 CP spec mutants, this suite killed 5
and none uniquely; the spec's own tests kill 22 and `controlplane/test/conformance` 19.
`result.go` still has the CP result types (~120 lines), which can go in a second pass.
Risk low. Dependencies: none. Leaves the object-store half and the broker half of the suite.

### A3. Echo scenario (~1,330 lines [M])
`simulator/echo` is a toy broker with its own invariants and workload, registered as a second
scenario of `mdssimulator`. Removing it and its registration (`ownsEcho`, import, the
two-scenario test) leaves `cmd/mdssimulator` compiling, minus 21 lines; one test
(`TestTheTwoScenariosAreRegistered`) needs a two-line change. Echo covers 0 unique
core statements and killed 0 mutants alone in every batch (core, infra, dst). `make sim`
saves 9.4 s. What is lost: a smoke test of the simulator with a trivial broker, and the
"fencing" tests. Risk low. Do before A5/A4; no dependency.

### A4. `capdiff_test.go` (84 lines, -75 s [M])
It runs two large campaigns (128 producers, 128 partitions) and asserts the cap changes the
seal reasons. It is a record of one decision. `InjectedBugNoPartitionCap` is killed by the unit layer
and the simulator, and `TestCommitLimitRefusesUncappedSegment` keeps the commit-limit check. Lost:
nothing known. After deletion `go vet` is clean.

### A5. dst examples (630 lines [M], up to 1,049)
`kvchaos` (329+90+55), `kvsim` (62+94) can go; keep `kv` (419) as the minimal example the
README points to. `kvsim` is only a runner for the two. dst-own tests catch 21/27 mutants,
examples 6/27 and every example kill is also a dst-own kill. Needs doc edits
(`dst/README.md` lines 56, 73, 87-89; `docs/contribute/determinism.md` lines 9, 41-43, 67).

### A6. Mechanical wins (~130 lines [E])
- `dst/campaign/campaign.go` `Run` (7 lines with comment), `dst/live/live.go` `Self` (3),
  `shared/runinfo` `LogAndExit` (6): measured by `deadcode -test`, unused anywhere.
- `controlplane/cmd/cpsim/main_test.go` (107 lines): re-runs the campaign that
  `make sim-campaign` already runs with 300 seeds; 0 unique kills. Keep a flag-parsing test only.
- `broker/cmd/mdssimulator/main_test.go` (113): same, shrink after A3.
- `broker/internal/core/fold.go` vs `controlplane/internal/runtime/fold.go`: 126 duplicate
  lines of production code; a generic helper could save ~60. Not verification, listed only.

### A7. Clock stack (~500 lines [E])
`dst/sched` (155 + 113 test), `dst/clock` (80 + 98), `dst/syncclock` (114 + 67) exist to
build `shared/clock.Fake` (34 lines). `dst/clock` is imported only by `syncclock`; `sched` only
by `syncclock`, `realclock` and `clock`. The kernel has its own queue. Replace with a heap in
`shared/clock` (about 60 lines + 50 test). The `determinism` script and `live_test.go`
need small edits. Risk low, but it touches a gated package, so run `make determinism`.

### B1-B3. Medium-risk tier (needs a larger mutation run first)
- B1 `domain/rebalance` unit tests, 1,524 lines (harness 442, rules 299, term 188, fold 181,
  kafka 147, coordinator 119, sites 92, waitjoin 56): coverage marginal 5 stmts (kafka_test),
  and every rebalance mutant that a unit test killed was also killed by `TestRebalanceMachine`
  or the simulator. Keep `kafka_test.go`, `fold_test.go` (golden hash pin) and the harness
  pieces that `TestRebalanceMachine` needs.
- B2 listed in the table. They are the fast, local version of the C1 check (broker vs
  `broker/spec`); the same oracle runs inside the kafka simulator (C4), which is why the
  coverage is 0.
- B3 candidates with 0 marginal statements and no sole kill: `group/codec_test` (68),
  `replica/{checkpoint,role,reads,trimmed}_test` (339), `metadb/{groups,determinism,schema}_test`
  (344), `executor/exec_test` partly. Fuzz tests are kept (they find different bugs than the
  mutants above can show).
These are statements about coverage plus a 49-mutant sample; a first step is to extend the
mutant set (section 7) and check whether any file still has 0 sole kills.

## 5. Keep, and why

- **Broker unit tests for groups, offsets, create, configs, coordinator** (`groups*_test`,
  `create_test`, `configs_test`, `coordinator_test`, `groups_offsets_test`): 429 core
  statements are covered by nothing else, and `groups_test`, `groups_park`, `coordinator`,
  `groups_offsets`, `fetch_test` and wire tests are the sole killer of mutants (`bL07`,
  `bL23`, `bL26`, `bL39`, `bL49`, `bL06`, `bL24`, `bL30`, `bL47`).
- **`simulator/kafka`** (except A4): 13 unique kills; only layer that checks the broker with faults.
- **`controlplane/internal/verify/simulator`**: 9 unique kills, and all fault-only rebalance bugs.
- **`controlplane/test/conformance`**: 0 unique kills in the core sample, but my mutants did
  not touch the memory/pebble/gRPC/replica adapters, which are its subject. It kills 19/24 CP
  spec mutants. Keep. (`grpc_rebalance_test` 463 vs `rebalance_test` 817 is a possible later merge; not evaluated.)
- **Specs and their tests** (`controlplane/spec`, `broker/spec`, `shared/objectstore` spec): the
  oracle. Each spec's own tests are the sole killer of 3 or more spec mutants.
- **dst kernel, campaign, spectest, golden tests**: 11 of 22 dst mutants are killed only by
  dst's own tests.
- **Object-store half of `speccheck/conformance` + `injectedbug` + `s3conform`**: needed to check S3
  and `fakes3` against the spec; the lockstep is also the only check that can be pointed at real S3.
- **`controlplane/ops/tools/mutate` and `mutants.txt`**: it is the best proof of coverage in
  the repo. Recommend adding it (or my wider harness) to the checklist for any test deletion.
- **Determinism and module-dependency gates (`ops/scripts`, `ci-determinism.sh`)**: static
  checks, not overlapping with the tests above.
- **Fuzz tests** (`FuzzDecode...`): a different bug class.
- **`riskscore`, `e2e-kafka-cli.sh`, integration tests**: not evaluated (Podman); keep.

## 6. Order and dependencies

1. A1, A3, A4, A2 (independent; each is a deletion plus `go vet`/test; do them as separate commits).
2. A5, A6, A7.
3. Re-run the mutation batches (below) on the slimmed tree; every kill set must be unchanged except for the deleted layers.
4. Only then consider B1-B3; extend the mutant sample to 200+ per module, and require that
   each deleted test file has 0 sole kills.
5. Gate for the future: run `make mutants` plus the wider harness whenever a test is deleted.

After the low-risk tier, `make test` runs about 85 s faster in the broker module (A3 + A4).

## 7. How to reproduce

All in `slim/evidence/tools` (scripts use the scratchpad paths; change `S=` and `R=` at the top):

- `gen_mutants.py N out.json`, `gen_mutants2.py`, `gen_mutants3.py`: make mutants.
- `mut_run.py`, `mut_run4.py`: run each mutant in a copy of the tree through the layers
  (`LAYERS` and `L` tables at the top). Broker bugs need a copy with `MDS_FORCE_BUG` read in
  `broker.New` (see `mut_run.py`).
- `matrix.py <results glob> <id prefix>`: kill matrix; `attrib.py`, `mut_names.py`: which unit
  test files killed which mutants.
- `covone.sh`, `cov.py`, `covdst.py`, `perfile.py`: coverage per test package and per test file.
- `cascade.py` + `rmdecl/`: delete a symbol and everything that stops compiling, to measure what
  a deletion really removes.

Known limits: random single-line mutants, small sample; equivalent mutants count as
survivors; 8 of 69 unit-killed control-plane mutants were not attributed to a test file
(a timeout in the name run); statement coverage says nothing about assertion strength, which
is why every candidate was also checked by mutation.

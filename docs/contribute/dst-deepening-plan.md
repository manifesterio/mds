# DST deepening plan: absorb undifferentiated host work

**Status:** in progress on `slim/verification`  
**Thesis:** `dst` packages look shallow because broker and controlplane each own a private host layer on top of dst verbs. Depth belongs in dst. Product oracles/protocols stay product-side.  
**Success (primary):** host patterns that both products reinvent become first-class dst APIs, and products stop reimplementing them.  
**Success (secondary):** deslop `shallow-package` / unused-export pressure on `dst` eases because packages gain owned implementation behind those APIs — not because we “declare findings noise.”

---

## Locked decisions

1. **Compatibility:** break in-tree freely; no aliases.
2. **Goldens / hashes:** prefer hash-neutral; if hashes must move, update goldens in the same commit with an explicit why.
3. **`sched` merge (Phase D):** spike-gated; expect deferral unless zero drift.
4. **Shipping:** commits on this branch only (not stacked PRs). Commit frugally — each commit may run full `make build`.
5. **Deslop:** trailing check after ownership, not a hard gate per phase.
6. **CP counter Env:** first-class `live.NewCounterEnv` (preserve replica semantics).

---

## Non-goals (guardrails for every agent)

Do **not** move into dst:

- Kafka/oracle/log reconstruction, connection-table network semantics
- RoleChanged / companion-actor *protocol* (shared product helper OK; not a dst API)
- Product IO sumtype Answer switches (product codegen OK; not dst)
- Assert/fault **names** and catalogs (names stay in product; only ceremony/helpers deepen)
- Planted bugs, consistency models, swarm menus, segment knobs

Do **not** “fix” tangles by merging files just to silence deslop. Tangles stay unless a real ownership split appears.

Do **not** force controlplane onto `AddService` unless a later design says the shared MemoryLog model allows it. CP’s Executor-based fakes are differentiated.

---

## Program map (phases)

```text
Phase 0  Inventory + freeze baselines
Phase A  live Env ownership          (point 2)
Phase B  campaign / golden / trace   (point 3, easy half)
Phase C  sim host kits               (point 1 — core opportunity)
Phase D  sched vs sim queue          (point 4 — optional / spiked)
Phase E  trim dead sim exports       (point 5)
Phase F  faulty deepen + leftover glue (point 3 remainder)
Phase G  product cleanup + deslop check (point 6 + verification)
```

Ship A→B→C first. D is gated. E can parallelize after C starts if it stays hash-neutral. F after C (faulty helpers lean on sim facts). G last.

---

## Phase 0 — Inventory and freeze

### Goal
Give every later agent a fixed baseline so “deeper” is measurable without LOC theater.

### Work
- Capture `go run ./ops/tools/deslop` summary (dst counts).
- List every `actor.Env` implementation under `broker/` and `controlplane/` (prod + test).
- List every `sim.NewSystem` / `live.NewHost` / `campaign.Main` call site.
- Note which sim exports have zero external namers (seed for Phase E).
- Record golden commands:
  - `go test ./controlplane/internal/verify/simulator -run Golden`
  - `go test ./broker/internal/verify/simulator/kafka -run TestGoldenTraceHashes`
  - `make -C dst build`

### Definition of done
- Short inventory checked into the PR description or appended to this doc under “Baseline”.
- No code behavior change.

### Agent brief
```text
You are doing Phase 0 only of docs/contribute/dst-deepening-plan.md.
Do not change product behavior. Collect baselines listed in Phase 0.
Write findings as a "Baseline" section at the bottom of that plan doc (or PR body).
Stop when inventory is complete.
```

---

## Phase A — Claim Env constructors in `dst/runtime/live`

### Goal
Products stop copying the 12-method `actor.Env` stub. `live` owns production/test Env variants.

### Why this deepens
Today `live.NewEnv` exists (`dst/runtime/live/env.go`) but broker reimplements ChaCha8+slog Env (`broker/internal/system/production/env.go`) and CP keeps a counter Env for replica (`controlplane/.../server/env.go`) while using `live.NewEnv` for other actors. Depth lives in products.

### Design (figure details from code; keep semantics)

| Constructor | Semantics | Replaces |
|---|---|---|
| `live.NewEnv(self, onFail)` | already exists — crypto ChaCha8, inert faults, onFail on assert break | keep; extend if needed |
| `live.NewLoggingEnv(self, log)` or `onFail` adapter | slog/structured violation log, continue | broker `production.Env` |
| `live.NewCounterEnv(self)` | sequential counter RNG, inert faults/asserts | CP `productionEnvironment` |
| `live.NewTestEnv(...)` (optional) | test harness Env with optional real `fault.Injector` | repeated `testEnv` / `fakeEnv` |

Broker’s `core.Environment` may embed/alias `actor.Env` — preserve that product interface by wrapping dst Env, not by reimplementing methods.

### Files (start here)
- `dst/runtime/live/env.go` (+ tests)
- `broker/internal/system/production/env.go` → thin wrapper or delete
- `controlplane/internal/controlplane/server/env.go` → `NewCounterEnv`
- `controlplane/internal/controlplane/server/node.go` (Env selection)
- Test Envs: `broker/internal/broker/harness_test.go`, `broker/.../kafka/kafka_test.go`, `controlplane/.../replica/replica_test.go`, `controlplane/.../rebalance/harness_test.go`

### Definition of done
- No full `actor.Env` method sets remain in broker/CP production packages (test Envs may remain only if they inject real faults; prefer dst helper).
- `live` gains at least Counter + Logging (or equivalent) constructors with tests.
- Broker and CP production paths compile and unit tests for server/broker harness pass.
- Hash-neutral expected (Env unused for faults in prod).

### Agent brief
```text
Implement Phase A of docs/contribute/dst-deepening-plan.md only.
Read live.NewEnv, broker production.Env, CP productionEnvironment, and node.go Env selection first.
Add dst/runtime/live constructors that preserve existing semantics (especially CP counter RNG).
Migrate broker + controlplane production (and obvious test stubs) onto those constructors.
Do not touch sim host kits, campaign, goldens, or sched.
Run: go test ./dst/runtime/live/... and the touched broker/controlplane packages.
Stop when Phase A DoD is met. Open one PR titled "dst/live: own production Env constructors".
```

---

## Phase B — Deepen `campaign`, `golden`, `trace` (easy half of point 3)

### Goal
Kill twin mains’ glue and broker’s bespoke golden; give `trace` the hasher products wrap.

### B1 — campaign registration helpers
**Evidence:** `broker/cmd/mdssimulator/main.go` (`add`/`fromSim`) twin of `controlplane/cmd/cpsim/main.go` (`runner`/`FromSim` + counters).

Add something in the spirit of (names up to agent, match local style):
- `campaign.RegisterSim(reg, name, detail, reproduceFmt, run func(...) sim.Result, counters func(sim.Result) map[string]uint64)`
- or extend `FromSim` to accept optional counter extractors / default crash/exit counters

CP keeps ExtraFlags and product `Text`/`Report` — only the FromSim+counter ritual moves.

**DoD B1:** both mains call the helper; local `add`/`fromSim`/`runner` duplication gone; campaign tests still pass.

### B2 — golden file helper for broker
**Evidence:** CP already uses `dst/verify/golden`; broker `kafka/golden_test.go` reinvents `-update-golden` + file format (richer lines: steps/rand/hash).

Extend `dst/verify/golden` to support broker’s line format **or** a structured `AssertFile` API that can store seed→(steps,rand,hash). Migrate `kafka/golden_test.go` onto it. Do not weaken the pin.

**DoD B2:** broker golden test imports `dst/verify/golden`; custom rewrite logic lives in dst; `-update-golden` still works.

### B3 — trace StateHasher
**Evidence:** `broker/internal/broker/hash.go` wraps `trace.Hash` because std `hash/fnv` is off the determinism allow-list.

Export a small `trace`-owned hasher / `StateHasher` used by broker (and available to CP). Behavior bytes must stay identical.

**DoD B3:** broker hash helper becomes a thin call into `trace`; determinism allow-list unchanged in spirit; broker tests pass.

### Agent brief
```text
Implement Phase B of docs/contribute/dst-deepening-plan.md (B1 then B2 then B3), one commit each if possible.
Do not change sim APIs or live Env (Phase A may already have landed — rebase).
Preserve campaign CLI flags and golden pin strength.
Prefer hash-neutral changes; if golden file format changes, keep values equivalent.
Run campaign package tests, golden tests in CP + broker kafka, and touched broker hash tests.
PR title: "dst: deepen campaign, golden, and trace helpers".
```

---

## Phase C — Claim sim host kits (core opportunity)

### Goal
`sim` owns the host patterns both products reinvent. This is the main deepening of the fat/hollow sim surface.

### C1 — Kernel fact accessors + host RecordFault
**Evidence:**
- `ServiceCtx.RecordFault` already writes `sim_fault` (`dst/sim/service.go`); network code hand-builds the same fact.
- `preset.PhaseStart` / `FaultCoverageCheck` scan `sim_phase` / `sim_crashed` / `sim_exited`.

Add on `System` / `View` / `Timeline` (match existing style):
- `System.RecordFault(kind string, fields ...actor.Field)` (host-side; service name optional/empty)
- `View.PhaseStart(Phase) (Time, bool)` or `Timeline.PhaseStart`
- Helpers for “has crash cause attrition” / “has graceful exit” used by coverage rules — or a stock `FaultCoverageRule(assert handles)` in sim

Migrate `broker/.../preset` scanners and network hand-facts onto them.

### C2 — Bootstrap / run-kit
**Evidence:** CP `sim.go` wraps `NewSystem`+`Start`+`Env`/`Rand`+thin `after`/`tracef`; broker cluster does the full Spawn stack.

Add a small documented kit, e.g.:
- `sim.Bootstrap(Config) (*System, actor.Env, *rng.Rand)` after `Start`, or
- methods already exist — then document + use them and delete product one-liners

Do **not** invent a giant Cluster framework. Only collapse repeated bootstrap.

### C3 — Quiesce + phase tail helpers
**Evidence:** broker `Cluster.quiesce` periodic `sim_quiesce` fact; CP hand-rolls quiet/final phases and largely ignores `SchedulePhases`.

Add optional helpers:
- `System.QuiesceEvery(d, pred func() bool)` or fact-emitting host task pattern
- Document when to use `SchedulePhases` vs custom; if CP quiet-tail can call a shared helper without forcing AddService, do that

CP adopting full `SchedulePhases` is **optional** and only if behavior-equivalent. Prefer extracting the quiet/convergence host loop CP already has next to `check.Converged`.

### C4 — Ask + RTT helper
**Evidence:** CP `client.go` / `groups.go` wrap `Ask` with delay-before, delay-after, parallel timeout.

Add `AskDelayed` / `OutsideCall` style helper on sim (name from local vocabulary). Migrate CP call sites that share the shape. Leave product timeout constants in CP.

### C5 — Shared latency policy next to `FastWithRareSlow`
**Evidence:** broker preset `slowTail`; CP `netDelay`/`slowAnswer` site-gated delays.

If a common “draw delay / maybe slow path with fault site” fits without forcing Services on CP, add it beside `FastWithRareSlow` / `fault`. Otherwise document the two supported patterns (Service latency vs host After) and only share the draw helper.

### Definition of done (Phase C)
- Broker preset no longer owns generic Timeline scanners for kernel facts.
- Host-side fault facts go through `System.RecordFault` (or equivalent).
- At least one CP Ask+RTT call cluster uses the new helper.
- `dst/sim` tests cover new helpers.
- Product oracles/network tables untouched in behavior intent.
- deslop: `sim` may still be “wide,” but new APIs have implementation depth (hidden helpers OK).

### Agent brief
```text
Implement Phase C of docs/contribute/dst-deepening-plan.md. Prefer C1 → C2 → C4 → C3 → C5.
Read: dst/sim/{service,timeline,phase,system,cost}.go, broker preset.go, broker cluster network.go,
controlplane verify/simulator/{sim,run,client,groups}.go.
Absorb undifferentiated host patterns into dst/sim; migrate broker/CP call sites.
Do NOT move Kafka oracle, connection-table semantics, or RoleChanged protocol into dst.
Do NOT force controlplane onto AddService.
Prefer hash-neutral. If hashes move, explain and update goldens.
Run: make -C dst build; go test for touched broker/CP verify packages.
PR series or one PR: "dst/sim: own host kits (facts, bootstrap, ask-delay, quiesce)".
```

---

## Phase D — Reconcile `sched` vs sim queue (point 4)

### Goal
End the structural thinness where `core/sched` is a second event heap unused by `sim` (`dst/sim/queue.go` is the real kernel queue; `sched` feeds clock adapters only).

### D0 — Spike (mandatory gate)
Prove whether sim’s queue can sit on `sched` without changing:
- tie draw counts
- lane FIFO behavior
- trace fold of entry kinds
- any golden hash

If not, stop and write a short ADR-style note in this doc: “sched remains live/clock-only; sim queue stays.” That outcome is a valid DoD for D0.

### D1 — Merge (only if D0 green)
- Implement queue on `sched` or move shared heap into an internal package both use.
- Keep determinism allow-list coherent (`sched` already allowed; sim queue code location must remain gated).
- No public behavior change.

### Definition of done
- Either: one heap implementation shared, tests+goldens green; **or** explicit deferral note with reasons.
- Do not leave a half-migration.

### Agent brief
```text
Phase D0 only unless D0 proves zero drift. Compare dst/core/sched and dst/sim/queue.go.
Prototype behind a branch; run dst tests + broker/CP goldens.
If drift: document deferral in the plan and stop. If clean: proceed to D1 carefully.
No product API work in this phase.
```

---

## Phase E — Trim dead / narrow sim exports (point 5)

### Goal
Fat API cleanup without confusing it with deepening. Unexport or stop exporting symbols with zero external namers once Phase C’s new APIs are in (avoid churn).

### Candidates (verify with grep at execution time; Phase 0 list is seed)
Likely: `Cancel`, `Logging`, `Down`, named `CrashObserver`/`RecoveryObserver` (if only duck-typed), `HostLane`, discarded `RunUntil` status consts if never read, `Executor` type name if only literals, `Counter` type if only via `Result.Stats`.

### Rules
- Unexport only after grep across repo (`broker/`, `controlplane/`, `dst/`, `shared/`).
- Prefer unexport over deletion of behavior.
- Hash-neutral by definition if nothing external called them.

### Definition of done
- Dead exports gone or unexported.
- deslop shallow count for `sim` may drop slightly; not the primary metric.
- Docs/README export tables updated if they name removed symbols.

### Agent brief
```text
Phase E of the plan. Grep every candidate export. Unexport only if zero external references.
Do not remove behavior still used via duck typing without checking sim internals.
No product refactors except compile fixes. Run make -C dst build.
```

---## Phase F — Deepen `faulty` (remainder of point 3)

### Goal
Broker’s twin `faulty.Shape` + Asserts wiring for objectstore/controlplane fakes becomes less ceremonial.

### Work
- Read `broker/.../fakeservices/{objectstore,controlplane}.go` and `dst/verify/check/faulty`.
- Add helpers for default Asserts naming slots / Shape assembly that both fakes share.
- Keep Op/Reply/Fail/Corrupt/Limit product-specific.

### Definition of done
- Twin Shape blocks share a dst helper for the undifferentiated part.
- Faulty self-tests + fakeservices tests pass.

### Agent brief
```text
Phase F only. Deepen dst/verify/check/faulty with helpers proven by broker fakeservices twins.
Do not move Corrupt/MaxCommitItems product policy into dst.
```

---

## Phase G — Product cleanup, ownership check, deslop trailing gate

### Goal
Confirm the project problem shrank: products keep differentiated truth; host layer lives in dst.

### Work
- Grep again for Env stubs, hand `sim_fault` facts, custom goldens, FromSim counter glue, Timeline scanners for `sim_phase`/`sim_crashed`.
- Run full: `make -C dst build`, broker and CP verify test suites, deslop.
- Update `dst/README.md` package table if new host kits are part of the public story.
- Append “Outcome” section to this plan: what moved, what stayed product, deslop before/after for dst.

### Definition of done
- No remaining **undifferentiated** copies of Phase A–F patterns (product-specific Envs with real injectors OK if documented).
- Oracles/network/RoleChanged still product-side.
- Deslop dst summary recorded; remaining shallow/tangle findings classified as accepted (with reason) or follow-ups — not silently ignored.

### Agent brief
```text
Phase G. Do not invent new APIs. Hunt leftover duplicates of absorbed patterns; delete or migrate.
Run full verification + deslop. Write Outcome section into docs/contribute/dst-deepening-plan.md.
```

---

## Cross-cutting rules for every implementing agent

1. Read this plan + `dst/README.md` before coding.
2. Prefer small PRs: one phase (or one lettered sub-phase) each.
3. Match local Go style; use Modern Go Guidelines if the repo skill applies; run package tests you touch.
4. Determinism: anything under `dst/core` or `dst/sim` stays on the allow-list story (`docs/contribute/determinism.md`, `ops/dst/determinism-allow.txt`). Do not pull disallowed imports into gated packages.
5. If stuck choosing between two APIs, pick the one that **deletes a product copy that exists in both broker and CP**, not the one that only cleans one product.
6. When in doubt about product vs dst ownership, leave it product and note it under Outcome.

---

## Suggested PR titles

| Phase | Title |
|---|---|
| A | `dst/live: own production Env constructors` |
| B | `dst: deepen campaign, golden, and trace helpers` |
| C | `dst/sim: own host kits (facts, bootstrap, ask-delay)` |
| D | `dst: unify sched and sim queue` or `docs: defer sched/sim queue merge` |
| E | `dst/sim: unexport unused host API` |
| F | `dst/faulty: share Shape/Asserts wiring helpers` |
| G | `dst: finish host-layer absorption and record deslop outcome` |

---

## Baseline

_(Phase 0 agent fills this in.)_

## Phase D0 — Spike outcome: **DEFER**

**Decision:** do not merge. `sched` remains live/clock-only; `sim` keeps `queue.go`. No D1. No code change.

**Compared:** `dst/core/sched/sched.go` vs `dst/sim/queue.go` (plus `entry.go`, `foldEntry`, `core/clock` cancel pattern, `guarantees_test` lane FIFO).

### Shared shape (not enough)

Both are binary min-heaps ordered by `(at, tie, seq)`. That is the only real overlap. Consumers diverge: `sched` feeds `core/clock`, `core/syncclock`, and product `shared/clock`; `sim` never imports `sched` — its queue is the kernel run loop.

### Why zero-drift sit-on-sched fails

| Gate | `sched` | `sim` queue | Drift if forced together |
|---|---|---|---|
| Tie draw counts | `After` draws one `Uint64` per event; `AfterFIFO` draws **zero** and sets `tie=0` | **Always** draws one `Uint64` per `add`, then may overwrite with a lane group's existing tie (draw still burned) | Mapping lane FIFO onto `AfterFIFO` skips draws → RNG stream shifts → later faults/delays/orders and golden hashes move. Mapping onto `After` alone keeps draws but loses lane FIFO. |
| Lane FIFO | No lanes. `AfterFIFO` = global same-time FIFO. `After` = independent ties (no FIFO group) | Per-`(lane, at)` shared tie → FIFO by `seq` within lane; different lanes at one time stay randomly ordered by their ties (`TestSameTimeOrderIsSeeded`) | Neither `After` nor `AfterFIFO` reproduces “FIFO within lane, seeded across lanes.” Need `laneGroup` maps — i.e. keep sim’s queue logic. |
| Trace fold / entry kinds | Payload is `label` + `func()` only | Frozen `entryKind` values (0/1/2/3/5/7) folded with lane, node, inc, service, body in `foldEntry` | Sitting on `sched` either drops kinds (hash change) or pulls sim metadata into `core/sched` (wrong layer for clock adapters). |
| Cancel / peek | No kill. Clock `Stop` leaves the heap event; callback no-ops. `Peek`/`Len` still see it | Soft-delete (`dead`), `byID`, `live`, lane refcounts; `peek`/`top` skip dead; `dropIncarnation` | Different emptiness and time-peek under cancel. Unifying without changing one side’s peek/cancel story is not free. |
| Clock extras | `Jump`, `Take` unlocked, `max(now, at)` on take | No jump; `add` clamps past→now; `pop` sets `now = e.at` | Adapters need Jump; sim does not. One type serving both grows API without deleting sim’s bookkeeping. |

### What a “clean” merge would actually require

To keep draws, lane FIFO, kinds, and goldens identical, `sched` would have to absorb lane groups, always-draw-then-reuse, soft cancel + incarnation drop, and entry metadata — becoming a second copy of `sim/queue` under `core/`. Clock callers would then sit on a sim-shaped heap they do not need. Extracting only the ~heap push/pop into a shared internal package saves little and still leaves two behavioral façades; it does not end “sched unused by sim.”

### DoD for D0

Explicit deferral recorded. Working tree unmigrated. D1 not started.

## Outcome

_(Phase G agent fills this in.)_

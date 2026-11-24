# dst redesign plan: actors, built-in effects, typed refs, dstgen

Status: DRAFT for review. Nothing in this plan has been executed. The repo is at clean HEAD
`60b91d3` on branch `slim/verification`.

Companion sketches (not compiled, `//go:build ignore`): `docs/design/dst-next/`

- `api.go`: the proposed API (packages `actor`, `sim`, `live`)
- `kv.go`, `server.go`, `store.go`, `client.go`, `kv_gen.go`: the kv example on the new API
- `products.go`: controlplane rebalance, controlplane server, replica soft crash, broker, broker sim producer

## Contents

1. How to run this plan
2. Goal and principles
3. Decisions log
4. Target API in one page
5. Runtime guarantees (the contract the runtimes must keep)
6. Target layout and dependency rules
7. Rules for every agent
8. Standard gates (used by every definition of done)
9. Golden hash procedure
10. Phases P0 to P8, with steps, agent briefs and definitions of done
11. Risks
12. Deferred work
13. Success metrics

---

## 1. How to run this plan

- One orchestrating session (Opus) runs the phases in order. It launches each step's agent
  with the brief below, verifies the result itself with the gates in section 8, and makes the
  phase commit. Agents never commit.
- Model per step: **Opus** for design, new runtime code and semantic migrations. **Haiku** for
  mechanical edits (moves, renames, call-site rewrites, doc sweeps). Opus reviews every Haiku
  step before the phase commit.
- Steps inside a phase run one after another unless the brief says they are independent. Two
  agents must never edit the same files at once.
- A phase ends only when its definition of done (DoD) holds. Then one commit on
  `slim/verification`. A golden-hash regeneration is its own commit inside the phase (section 9).
- Approval gates: P1 ends with the user's approval of the frozen API. Do not start P2 before it.
- If an agent is denied a permission, the orchestrator stops and asks the user. It never runs
  the denied command itself.

## 2. Goal and principles

dst is the base for controlplane, rebalance and broker today, and for a KV store, an SQS-like
queue, an SNS-like pub/sub and an SWF-like workflow service later. The goal is code that a new
reader understands at the call site without reading the runtime.

Principles:

1. **Fix shapes, not names.** A change is worth doing when it deletes bookkeeping from product
   code (request-ID tables, timer generations, route switches, hand-written Fold/Tag/markers).
2. **Actors plus effects.** Each machine is an actor with one input protocol. Sends, timers,
   stop, facts and IO are effects recorded on an outbox. The runtime carries them out.
3. **100% types for wiring.** No string addresses. Strings are only identity labels (node
   names, timer keys, fact names).
4. **Same actor code in sim and production.** Only the runtime and the IO executor differ.
5. **Products keep their models.** dst grows to fit them (for example the replica's injected
   soft crash stays; dst adds `ForgetPending`). gRPC stays as it is.
6. **Determinism is a checked property**: one seed gives one trace hash, and a gate checks it.

## 3. Decisions log

| # | Decision | Choice |
|---|---|---|
| D1 | Model | Actor system (Akka Typed naming) with built-in effects |
| D2 | Outputs | `Receive(now, msg, outbox *Outbox[In, IO])`; effects recorded on the outbox, not returned. Go cannot infer type parameters from a return type, and methods cannot add type parameters |
| D3 | Name of the effects recorder | `Outbox` (type `actor.Outbox`, parameter `outbox`) |
| D4 | Output typing | `Outbox[In, IO]`: `In` = the actor's own protocol (timers and adapters produce it); `IO` = the product's sealed sum of real IO only. `actor.NoIO` when there is none |
| D5 | Refs | `EntityRef[P]` stable (survives restarts); `ReplyTo[R]` dies with the asking incarnation; `ServiceRef[Req, Rep]` for simulated services |
| D6 | Sends | `Tell` (to an actor), `TellService` (to a service), `Ask` (actor, request carries ReplyTo), `Call` (service), `Reply`, `Perform` (product IO with a typed answer), `Watch` (asker gone), `Sub` (component with its own protocol) |
| D7 | Timers | `TimerKey` is a string built by the product; `After` (one-shot), `StartTimer`/`CancelTimer` (keyed, replaceable) |
| D8 | In-machine reset | `outbox.ForgetPending()` drops all pending timers and answers without ending the incarnation |
| D9 | Naming rule | Callable actor or service: `<name>Req` / `<name>Reply`. Actor called by nobody: `<name>Msg`. Generic pairs keep `In, Out`/`In, IO`; one payload parameter is `P`; service reply parameter is `Rep` |
| D10 | Constraint name | `Payload` (not Message, not Event: broker's `core.Event` means input only) |
| D11 | Boilerplate | Custom `dstgen` on go/ast + go/types (not protobuf). Annotations `//dst:sum`, `//dst:cases <sum>`, `//dst:payload`, `//dst:state`. Tags pinned in an append-only `dst.tags` |
| D12 | Scope | dst + kv example, then controlplane, then broker. One API at the end |
| D13 | Old API | Hard cut: no aliases or adapters. Old and new packages coexist only until the last caller moves (end of P7), then the old ones are deleted |
| D14 | Golden hashes | Regenerated deliberately, at most once per phase, in their own commit (section 9) |
| D15 | Layout | `core/` (production-safe), `runtime/live`, `sim`, `verify`, `explore`, `cmd`, `examples` with depguard rules |
| D16 | Extras in scope | Layer split (P2); flow helper only if evidence after migration (P8) |
| D17 | Deferred | Typed state collections (state.Map/Table), fault decorators, promoting broker history, clock-package merge |
| D18 | controlplane process | One `live.Host` with the replica and rebalance actors behind the unchanged gRPC handlers |
| D19 | Branching | One long branch `slim/verification`, one commit per phase (plus golden commits) |
| D20 | Models | Opus for design/new code/semantic migration and review; Haiku for mechanical work |
| D21 | Start | Clean HEAD. The earlier pure-rename pass is discarded (backup patch kept in the session scratchpad) |
| D22 | Network model | No packet/TCP simulation. There is no node-to-node traffic: replicas coordinate only through the log and S3, rebalance and the replica share a process, and broker to controlplane is gRPC, modelled as a `sim.Service` like S3. Broker's `fakenetwork` (about 1255 lines) is deleted in P7; Kafka clients deliver whole frames to the broker with a per-pair delay and a connection-reset fault (P7.4) |

## 4. Target API in one page

Full sketch: `docs/design/dst-next/api.go`. P1 freezes it. Summary:

```go
// package actor (dst/core/actor): what product code imports
type Payload interface{ Fold(h *trace.Hash) }
type Actor[In, IO Payload] interface {
	Receive(now Now, msg In, outbox *Outbox[In, IO])
	StateHash() uint64
}
type Props[In, IO Payload] struct{ Name string; New func(Env) Actor[In, IO]; Boot In; StopMsg func(Mono) In; Priority func(In) Priority; Restart RestartPolicy }

func (o *Outbox[In, IO]) TellSelf(msg In)
func (o *Outbox[In, IO]) After(d Duration, msg In)
func (o *Outbox[In, IO]) StartTimer(key TimerKey, d Duration, msg In)
func (o *Outbox[In, IO]) CancelTimer(key TimerKey)
func (o *Outbox[In, IO]) Do(io IO)
func (o *Outbox[In, IO]) Stop(how End, cause string)
func (o *Outbox[In, IO]) Fact(f Fact)
func (o *Outbox[In, IO]) ForgetPending()

func Tell[In, IO, P Payload](o *Outbox[In, IO], to EntityRef[P], msg P)
func TellService[In, IO, Req, Rep Payload](o *Outbox[In, IO], to ServiceRef[Req, Rep], req Req)
func Ask[In, IO, Req, Rep Payload](o *Outbox[In, IO], to EntityRef[Req], makeReq func(ReplyTo[Rep]) Req, adapt func(Rep) In)
func Call[In, IO, Req, Rep Payload](o *Outbox[In, IO], to ServiceRef[Req, Rep], req Req, adapt func(Rep) In)
func Reply[In, IO, R Payload](o *Outbox[In, IO], to ReplyTo[R], answer R)
func Perform[In, IO, Req, Rep Payload](o *Outbox[In, IO], req Req, adapt func(Rep) In) // typing of Req vs IO: open item O1
func Watch[In, IO, R Payload](o *Outbox[In, IO], who ReplyTo[R], gone In)
func Sub[In, IO, C Payload](o *Outbox[In, IO], wrap func(C) In) *Outbox[C, IO]

// package sim (dst/sim): test only
func NewSystem(cfg Config) *System
func Spawn[In, IO Payload](s *System, p actor.Props[In, IO], opts ...SpawnOption[In, IO]) actor.EntityRef[In]
func AddService[Req, Rep Payload](s *System, name string, svc Service[Req, Rep]) actor.ServiceRef[Req, Rep]
type Service[Req, Rep Payload] interface { Handle(ctx *ServiceCtx, req Req, reply Responder[Rep]); StateHash() uint64 }

// package live (dst/runtime/live): production
func NewHost(cfg HostConfig) *Host
func Spawn[In, IO Payload](h *Host, p actor.Props[In, IO], exec func(*ExecCtx, IO) error) actor.EntityRef[In]
func Ask[Req, Rep Payload](ctx context.Context, h *Host, to actor.EntityRef[Req], makeReq func(actor.ReplyTo[Rep]) Req) (Rep, error)
```

Old to new (for migration briefs):

| Today | New |
|---|---|
| `machine.Machine[In, Out]`, `Step(now, in) []Out` | `actor.Actor[In, IO]`, `Receive(now, msg, outbox)` |
| `machine.Message` | `actor.Payload` |
| `machine.Factory`, `kernel.NodeSpec` | `actor.Props` (+ `sim.SpawnOption` for Cost, Attrition, Workload, OnPhase, AfterReceive) |
| `kernel.World` | `sim.System` |
| `kernel.Addr[In]` | `actor.EntityRef[P]` |
| `kernel.Ref[In]`, `machine.NodeInstance` | internal to the runtime; products see `ReplyTo[R]` |
| `kernel.Router[In]` + product `Route` switch | gone for sends/timers; product IO goes to an executor (`sim.Executor[IO]`, `live` exec func) |
| product `SetTimer`/`CancelTimer`/`SendSelf`/`StopForRestart`/`ShutdownDone`/`RecordFact` output cases | `outbox.StartTimer`/`CancelTimer`/`TellSelf`/`Stop`/`Fact` |
| `TimerFired{ID, Gen}` + generation checks | the timer's own message; runtime drops stale timers |
| request-ID tables (`nextReq`, `reqs map[RequestID]any`, `claimReq`) | `Perform`/`Call`/`Ask` adapters carry the context |
| `kernel.Service` + `Handle(cx, from, req)` + `cx.Deliver(from, ...)` | `sim.Service[Req, Rep]` + `Handle(ctx, req, reply)` + `reply.After(d, answer)` |
| `kernel.Inject` (from a test) | `sim.Ask` / `sim.Tell` from the scenario |
| `live.Run` per machine + hand-made inbox forwarding | `live.Host` with several actors |
| hand-written `isX()`, `Tag()`, `Fold()` | `dstgen` |

Open items P1 must close (with a recorded choice in the ADR):

- **O1** `Perform` typing: how to tie a request case of the IO sum to its answer type at compile
  time (options: dstgen-generated per-request helpers from a `//dst:answer <Type>` annotation;
  runtime type check with a protocol failure; a generic `actor.Request[Rep]` marker interface).
- **O2** `Sub` semantics: which effects a component may record (timers, Perform, Ask) and how
  `ForgetPending` applies to them.
- **O3** fold format of effects and refs in the trace hash (the order and tags, so goldens are
  stable from P4 on).
- **O4** `Watch` for outside callers in `live`: when a gRPC context ends, the runtime delivers
  the `gone` message. Confirm controlplane's `CallAbandoned` behaviour maps 1:1.
- **O5** closed by D22: with `fakenetwork` gone, no sim service sends unsolicited messages.
  Kafka client actors `Tell` the broker its `BytesReceived` frames, and the broker's sim
  executor turns `Do(core.Send)` into a `Tell` to the client actor that owns the connection.
  P1 only confirms that a sim executor may `Tell` an entity (it already holds the ref).

## 5. Runtime guarantees

Both runtimes (`sim`, `live`) must keep these. Each one gets a named test in P4 (sim) and P5
(live). The ADR in P1 states them precisely.

| # | Guarantee | Test name |
|---|---|---|
| G1 | Effects of one Receive are carried out after it returns, in the order recorded | `TestEffectsAppliedInOrder` |
| G2 | Events due at the same virtual time run in an order fixed by (time, seeded tie, insertion seq); the same seed gives the same order | `TestSameTimeOrderIsSeeded` |
| G3 | `CancelTimer(k)` and `StartTimer(k, ...)` also drop a timer message for `k` that fired but is not yet received | `TestCancelDropsFiredTimer` |
| G4 | A `ReplyTo` dies with the asking incarnation: replies to it are dropped | `TestReplyToCrashedAskerDropped` |
| G5 | `ForgetPending` drops every pending timer, Ask/Call/Perform answer of the actor; later answers are dropped | `TestForgetPendingDropsAnswers` |
| G6 | `Watch(who, gone)` delivers `gone` once when the asker's incarnation ends or (live) its context ends; never after a Reply was sent | `TestWatchFiresOnce` |
| G7 | `Stop` ends the incarnation; everything it owned (timers, pending answers, watches) is dropped; `Restart` policy decides the next boot | `TestStopDropsEverything` |
| G8 | Effects recorded through `Sub` behave exactly like the parent's, with messages wrapped | `TestSubWrapsMessages` |
| G9 | Every scheduled entry and every state hash is folded into the trace hash (sim) | `TestTraceFoldsPendingTimers` |
| G10 | A Perform answer reaches only the incarnation and pending set that issued it | `TestPerformAnswerScoped` |

## 6. Target layout and dependency rules

```
dst/
  core/            production-safe, deterministic, no goroutines
    actor/         (new) Actor, Props, Outbox, refs, effects, Env, Now, Fact
    rng/ trace/ assert/ fault/
    clock/         (moved; clock, syncclock, realclock stay separate packages under core/ for now)
    sched/         (moved; used only by the clocks and shared/clock, so it is core, not sim; see issues.txt DST-1)
  runtime/
    live/          production host
  sim/             simulator (System, services, executors)
  verify/
    check/ spectest/ faulty/ golden/
  explore/
    campaign/
  cmd/
    determinismcheck/ dstgen/
  examples/
    kv/
```

Rules (depguard in `dst/.golangci.yml`, plus a product-side rule):

- `core/**` imports only `core/**` and the standard library.
- `runtime/**` imports `core/**`.
- `sim/**` imports `core/**`.
- `explore/**` imports `core/**`, `sim/**`.
- `verify/**` imports `core/**`, `sim/**`, `explore/**` (spectest runs campaigns). Order: core <- sim <- explore <- verify. (Changed in P2.)
- Product non-test code may import `core/**` and `runtime/**` only; `sim/**`, `verify/**`,
  `explore/**` only from test code and from `internal/verify/**` and `cmd/*sim*`.
- Determinism gate is opt-out: every package under `core/` and `sim/` is checked, except an
  explicit exclude list in `ops/dst/determinism-exclude.txt`.

## 7. Rules for every agent (paste into every brief)

```
RULES
- Repo: /Users/arun.lakshman/workspace/manifester/mds-verify-slim (git worktree), branch
  slim/verification. Work on this branch in this directory. Never create a worktree or branch.
- Never commit, never git stash, never git checkout/reset/revert/restore. The orchestrator commits.
- Do not touch slim/ (historical evidence) or CHANGELOG.md history.
- macOS: sed has no \b and -i needs an argument; use perl -pi -e for regex edits, or gofmt -r.
  Never leave backup files (*-E, *.bak). zsh does not split $var into words: use arrays or
  find ... -exec ... {} +.
- Golden trace hashes: if a golden test fails and this step is not a golden step, STOP and
  report. Never edit expected hash values unless the brief says so.
- Do not run integration targets (make it, e2e) unless the brief says so; they need Podman.
- If a command is denied by permissions, stop and report it. Do not work around it.
- Report at the end: what changed (git diff --stat | tail -1), anything deliberately left,
  the exact gate output tails, and any failure.
```

## 8. Standard gates

`GATES-FAST` (every step):

```
gofmt -l dst shared controlplane broker            # prints nothing
go build ./dst/... ./shared/... ./controlplane/... ./broker/...
go vet   ./dst/... ./shared/... ./controlplane/... ./broker/...
```

`GATES-FULL` (every phase DoD; run by the orchestrator):

```
make -C dst build                                   # fmt, ascii, vet, lint, modernize, test -race, determinism
make -C shared fmt-check ascii-check vet lint modernize-check test
make -C controlplane fmt-check ascii-check vet lint modernize-check compile test determinism sim
make -C broker fmt-check ascii-check vet lint modernize-check compile test sim
(cd broker && bash ../ops/broker/scripts/ci-determinism.sh)
make -C controlplane sim-campaign SIM_SEEDS=200
make -C broker sim-campaign SIM_SEEDS=200
```

`GATES-IT` (P6 and P7 DoD only, needs Podman; if Podman is not available the orchestrator asks
the user to run it): `make -C controlplane it`, `make -C broker it`.

## 9. Golden hash procedure

Golden pins live in:

- `dst/examples/kv/kv_test.go` (`goldenHashes`)
- `controlplane/internal/verify/simulator/golden_test.go` (`goldenHashes`, `goldenWithoutGroups`)
- `broker/internal/verify/simulator/kafka/testdata/golden_hashes.txt`
  (`go test ./internal/verify/simulator/kafka -run TestGoldenTraceHashes -update-golden`)
- about 35 hex constants in other `_test.go` files (find with `grep -rnE '0x[0-9a-f]{12,16}' --include='*_test.go'`)

A phase that changes what is folded (P4, P6, P7, maybe P3):

1. Before regenerating, the phase's code must pass every non-golden check: campaigns, oracles,
   planted-bug tests (`spectest.RequireCaught`/`RequireClean`), determinism re-runs.
2. Regenerate all pins in one commit titled `test(golden): regenerate trace hashes for <phase>`,
   whose message says why the fold changed.
3. Run every golden test twice to prove the new pins are stable.

Pure moves and renames (P2) must leave every golden unchanged: that is their proof.

---

## 10. Phases

### P0. Baseline (Haiku) [no code changes]

Goal: record the starting point so every later DoD can compare.

Steps:

- P0.1 (Haiku) record the baseline.

```
BRIEF P0.1 (model: haiku)
<RULES from plan section 7>
Task: create docs/design/dst-redesign-baseline.md with:
1. git rev-parse HEAD.
2. Results of GATES-FULL from docs/design/dst-redesign-plan.md section 8 (pass/fail per line,
   with the last 5 lines of output of each).
3. Counts (non-test .go files, repo dirs dst shared controlplane broker):
   - methods `) Fold(h *trace.Hash)`, `) Tag() uint64`, marker methods `func (X) isY() {}`
   - `StateHash() uint64` implementations
   - timer generation fields/checks: grep -E '[gG]en(\+\+|\b.*TimerVersion)'
   - request-ID bookkeeping: grep -E 'nextReq|map\[machine\.RequestID\]'
   - Route functions: grep -E 'Route *func|Route: '
   - product output cases that duplicate runtime effects: SetTimer, CancelTimer, SendSelf,
     StopForRestart, ShutdownDone, RecordFact type declarations
   - lines of code (non-test, non-comment) of dst/examples/kv, broker/internal/system,
     controlplane/internal/controlplane/server
4. The current golden pins: copy the maps/files listed in plan section 9 verbatim.
Do not change any other file.
```

DoD: the baseline file exists, every gate line has a recorded result, counts filled in.
Commit: `docs(dst): record redesign baseline`.

### P1. Design freeze (Opus) [docs only] [USER APPROVAL GATE]

Goal: one frozen API and an ADR that agents can execute without further design questions.

Steps:

- P1.1 (Opus) update the sketches and write the ADR.

```
BRIEF P1.1 (model: opus)
<RULES>
Read: docs/design/dst-redesign-plan.md (all), docs/design/dst-next/*.go.
Tasks:
1. Update docs/design/dst-next/*.go to the decisions log: rename Out -> Outbox and the
   parameter out -> outbox everywhere; TellService for fire-and-forget to services; keep
   gRPC handlers unchanged in products.go (live.Ask is used inside the handler only if the
   handler's behaviour, errors and timeouts stay identical).
2. Write docs/design/dst-next/ADR.md:
   - the final API (copy of api.go with doc comments), every exported identifier documented
   - runtime guarantees G1..G10 (plan section 5) stated precisely, including edge cases
     (crash during a Receive, Stop and ForgetPending in the same Receive, StartTimer with a
     zero duration, Reply to a Watched asker)
   - closed choices for open items O1..O5 (plan section 4), each with the options considered
     and why the choice was made
   - the trace fold format: tags for each effect kind and for refs; what a ReplyTo folds as
   - dstgen spec: annotations, generated code, dst.tags format (append-only), errors, how
     existing Tag() numbers are imported so P3 keeps hashes, the freshness check
   - migration tables per product: every current output case and input case of
     controlplane runtime, rebalance, broker core, and where it goes (effect, IO case, message)
3. Validate the API against products.go: every snippet must type-check in your head against
   the frozen API; list any change you had to make.
Do not touch code outside docs/design/.
```

- P1.2 (orchestrator) present the ADR to the user and apply their edits.

DoD: ADR complete, O1..O5 closed, sketches consistent with the ADR, **user approved**.
Commit: `docs(dst): freeze actor API and runtime guarantees (ADR)`.

### P2. Layout and dependency rules (Haiku, Opus review) [no behaviour change]

Goal: the new folder layout with depguard and an opt-out determinism gate. Old APIs unchanged.

Steps:

- P2.1 (Haiku) move packages.

```
BRIEF P2.1 (model: haiku)
<RULES>
Move dst packages to the layout in plan section 6, with git mv, keeping package names:
  dst/machine -> dst/core/machine      dst/rng -> dst/core/rng       dst/trace -> dst/core/trace
  dst/assert  -> dst/core/assert       dst/fault -> dst/core/fault
  dst/clock -> dst/core/clock  dst/syncclock -> dst/core/syncclock  dst/realclock -> dst/core/realclock
  dst/live -> dst/runtime/live         dst/kernel -> dst/sim/kernel  dst/sched -> dst/sim/sched
  dst/check -> dst/verify/check (and its subpackages spectest, faulty)  dst/golden -> dst/verify/golden
  dst/campaign -> dst/explore/campaign
Update every import path in dst, shared, controlplane, broker (perl on import lines only),
the package tables in dst/README.md and docs/contribute/determinism.md, the package lists in
ops/dst/scripts/ci-determinism.sh, ops/broker/*, ops/controlplane/*, and any Makefile.
Then GATES-FAST and go test ./dst/... ./controlplane/... ./broker/... ./shared/...
Golden tests must pass unchanged.
```

- P2.2 (Haiku) depguard rules and opt-out gate.

```
BRIEF P2.2 (model: haiku)
<RULES>
1. Add depguard rules to dst/.golangci.yml implementing plan section 6 "Rules" for dst.
   Add the product rule (non-test product code must not import dst/sim, dst/verify,
   dst/explore) to the golangci config used by controlplane and broker (find it via their
   Makefiles / ops/make/common.mk).
2. Change ops/dst/scripts/ci-determinism.sh so it checks every package under dst/core and
   dst/sim found with `go list`, minus the lines of a new ops/dst/determinism-exclude.txt
   (start it empty with a comment header).
3. make -C dst build, and lint for controlplane and broker. If depguard reports existing
   violations, do not fix code: list them in your report.
```

- P2.3 (Opus) review P2.1/P2.2, fix depguard violations if any (they are real layering bugs).

DoD: GATES-FULL green; all goldens unchanged; depguard on in dst, controlplane, broker;
determinism gate checks every core/sim package (show the list).
Commit: `refactor(dst): layered layout with depguard and opt-out determinism gate`.

### P3. dstgen (Opus builds, Haiku applies)

Goal: generated markers, `Tag` and `Fold` from annotations; no hash change.

Steps:

- P3.1 (Opus) build `dst/cmd/dstgen`.

```
BRIEF P3.1 (model: opus)
<RULES>
Read docs/design/dst-next/ADR.md (dstgen spec) and kv_gen.go.
Build dst/cmd/dstgen (go/ast + go/types via golang.org/x/tools/go/packages, already a dst
dependency):
- //dst:sum on an interface; //dst:cases <sum> on a type block; //dst:payload on a struct;
  //dst:state on a struct (StateHash over its fields, maps folded in sorted key order).
- generates <file>_dst.go per package: markers, Tag, Fold; fields folded in declaration order;
  supported field kinds per ADR; unsupported field = error with file:line.
- dst.tags per package: append-only; `-import-tags` mode reads existing Tag() methods so
  current numbers are kept.
- `dstgen -check` exits non-zero when generated files are stale.
Tests: golden-file tests in dst/cmd/dstgen/testdata covering every field kind and error.
Wire `go generate` into dst/Makefile (target generate) and a freshness check into `build`.
GATES-FAST; make -C dst build.
```

- P3.2 (Haiku) apply dstgen to `dst/examples/kv` and dst tests only (products adopt it in P6/P7).

```
BRIEF P3.2 (model: haiku)
<RULES>
In dst/examples/kv and the dst test helpers that declare sum types: run dstgen
-import-tags to create dst.tags, add the annotations, delete the hand-written isX/Tag/Fold
methods that dstgen now generates, go generate, make -C dst build.
Golden tests must pass UNCHANGED (tags imported). If a hash changes because a hand-written
Fold used a field order different from declaration order, reorder the struct fields to match
the old Fold (do not edit goldens) and report it.
```

DoD: dstgen tests green; kv and dst tests use generated code; `dstgen -check` in `make -C dst
build`; goldens unchanged.
Commit: `feat(dst): dstgen generates sum-type markers, tags and folds`.

### P4. Actor core and sim System (Opus) + kv example

Goal: `dst/core/actor` and the new `sim.System` with guarantees G1..G10; kv example on the new
API. Old `machine`/`kernel` stay for controlplane and broker until P7.

Steps:

- P4.1 (Opus) `dst/core/actor`.

```
BRIEF P4.1 (model: opus)
<RULES>
Read the ADR. Implement dst/core/actor exactly as frozen: Payload, Actor, Props, Outbox and
its methods, EntityRef, ReplyTo, ServiceRef, TimerKey, Tell, TellService, Ask, Call, Reply,
Perform, Watch, Sub, ForgetPending, Env, Now, Fact, NoIO, End, Priority, RestartPolicy.
Outbox only records effects (a typed list); it never performs anything. Erasure of types
lives in one file (like today's kernel/erase.go), no `any` elsewhere. Unit tests for
recording, Sub wrapping and fold of refs. make -C dst build.
```

- P4.2 (Opus) `dst/sim` System on `dst/sim/sched`.

```
BRIEF P4.2 (model: opus)
<RULES>
Read the ADR and dst/sim/kernel (the old engine) for reusable parts: event queue, lanes,
inboxes (Control/Data), cost model, phases, attrition, incarnations, facts, trace hash,
rules. Implement package sim at dst/sim: NewSystem, Spawn with SpawnOption (WithExecutor,
Workload, OnPhase, Cost, Attrition, AfterReceive), AddService, Service, ServiceCtx,
Responder, Executor/ExecCtx, sim.Tell/sim.Ask from a scenario, Run, Result, Crash/Restart/
Stop/Pause/Resume, phases. Copy (do not import) what you reuse from sim/kernel; the old
package is deleted in P7.
Implement guarantees G1..G10 with the named tests in plan section 5, plus: same seed same
hash; different seeds explore different same-time orders; a planted runtime bug (e.g. not
dropping a fired timer on cancel) is caught by G3's test.
make -C dst build.
```

- P4.3 (Opus) rewrite `dst/examples/kv` from the sketches (server.go, store.go, client.go,
  kv.go), with dstgen, its campaign test and golden pins (new hashes, regenerated per
  section 9), and a planted-bug test proving `readSeesLastWrite` catches a broken store.
- P4.4 (Haiku) update `dst/README.md` "How to write a machine" to the actor API (text only,
  from the ADR and the kv example).

DoD: GATES-FULL green; G1..G10 tests exist and pass; kv example matches the sketches
(reviewed by Opus); kv golden regenerated in its own commit; old controlplane/broker untouched
and green.
Commits: `feat(dst): actor API and sim System` and `test(golden): regenerate kv trace hashes for P4`.

### P5. live Host (Opus)

Goal: production runtime for the actor API: several actors per process, executors, outside Ask.

Steps:

- P5.1 (Opus) implement `live.Host`.

```
BRIEF P5.1 (model: opus)
<RULES>
Read the ADR and dst/runtime/live (old loop: inbox, timers, Inject, StopMsg/StopGrace,
Shutdown, AfterStep). Implement in dst/runtime/live, next to the old API (deleted in P7):
NewHost(HostConfig{Clock, Env, Shutdown, StopGrace}), Spawn(h, props, exec), Host.Run(ctx),
live.Ask(ctx, h, ref, makeReq) with Watch firing on ctx end, AfterReceive hooks.
One goroutine per actor; inbox Control before Data with optional bound on Data; real timers
with keys (G3); ForgetPending (G5); Stop + RestartPolicy (G7); executors run on the actor's
goroutine and must not block (document it); executor results go back as Perform answers or
plain messages.
Tests: G1, G3..G8, G10 for live (timing-tolerant, -race), shutdown and stop grace, outside
Ask with cancelled context fires Watch. make -C dst build.
```

DoD: GATES-FULL green; live tests pass with `-race` 20 times in a row
(`go test -race -count=20 ./dst/runtime/live/...`).
Commit: `feat(dst): live Host runs actors in production`.

### P6. controlplane migration (Opus semantic, Haiku mechanical)

Goal: controlplane on the actor API, models unchanged, gRPC unchanged.

Steps (in order):

- P6.1 (Haiku) controlplane runtime sums: split `runtime.Action` into effects (gone) and
  `runtime.IO` (real IO only), annotate with dstgen, import tags.

```
BRIEF P6.1 (model: haiku)
<RULES>
Read the ADR migration table for controlplane/internal/runtime. In that package:
- annotate EventData and the IO cases with dstgen annotations; run dstgen -import-tags;
  delete generated-equivalent hand-written methods.
- create the sealed sum IO with the IO cases (LogAppend, LogRead, LogGetEnd, LogClaimEpoch,
  LogDeleteOld, ObjectPut/Get/Delete/List, ControlPlaneCall/Reply as the ADR says).
- do NOT delete SetTimer/CancelTimer/StopForRestart/ShutdownDone/RecordFact yet: P6.2 removes
  their users first.
GATES-FAST.
```

- P6.2 (Opus) replica on the actor API.

```
BRIEF P6.2 (model: opus)
<RULES>
Read the ADR, docs/design/dst-next/products.go (replica section) and all of
controlplane/internal/controlplane/replica. Convert Replica to actor.Actor[runtime.EventData,
runtime.IO] keeping its model: injected soft crash, stall, roles, checkpoint, catch-up, reads.
- Step -> Receive with outbox; []runtime.Action returns -> outbox effects.
- Timers: TimerKeys and the timer's own message; delete timerGen/roleGen and their checks.
- Log and object IO: Perform with adapters carrying the context; delete claimReq,
  inFlightAppends-by-request, nextReq where Perform makes them unnecessary.
- softCrash and hitTrimmed: outbox.ForgetPending() + the existing state reset.
- client answers (m.answers, ReplyToClient): actor.Reply on the stored ReplyTo.
- Facts and assertions unchanged in meaning.
Keep every existing replica test passing (adapt the harness, not the assertions). Report each
behaviour you were unsure about instead of guessing.
```

- P6.3 (Opus) rebalance on the actor API (state pattern stays; `state any` becomes a sealed
  sum or three typed fields per ADR; parked calls keep `ReplyTo` + `Watch`; per-member timers
  with `TimerKey`; term loss uses `ForgetPending`; `SendSelf` -> `TellSelf`).
- P6.4 (Opus) server wiring: one `live.Host` with both actors; replica Tells rebalance
  `RoleChanged`; rebalance asks the replica directly; gRPC handler signatures, errors and
  timeouts unchanged (existing tests and `GATES-IT` prove it); delete the forwarding in
  `groups_handler.go` and the old `call()`/`answer()` only if behaviour is identical.
- P6.5 (Haiku, Opus review) controlplane simulator: `sim.System`, services as `sim.Service`,
  IO executor per `runtime.IO`, workloads as actors, scenarios; planted bugs still caught.
- P6.6 (Haiku) delete the now-unused effect cases and route code in controlplane; dstgen for
  the remaining sums; `make -C controlplane generate` if proto is touched (it should not be).
- P6.7 (orchestrator) golden regeneration per section 9; GATES-FULL; GATES-IT.

DoD: GATES-FULL and GATES-IT green; controlplane imports no `dst/core/machine` or
`dst/sim/kernel`; counts vs baseline: zero `TimerFired`-generation checks, zero request-ID
maps in replica/rebalance, zero effect duplicates in `runtime`; planted bugs caught;
`cpsim` campaign of 200 seeds clean.
Commits: `refactor(controlplane): replica, rebalance and server on the actor API`,
`test(golden): regenerate controlplane trace hashes for P6`.

### P7. broker migration and old-API removal (Opus semantic, Haiku mechanical)

Goal: broker on the actor API; then delete the old dst APIs.

Steps (in order):

- P7.1 (Haiku) broker `core`: `Action` split into effects (gone) and `core.IO`; `Event`
  envelope removed (`Receive` gets `now`); dstgen annotations; import tags.
- P7.2 (Opus) broker machine (`broker/internal/broker`): Receive/outbox; flush timer by key
  (delete `timerGen`/`open.gen`); S3 puts/gets with `Perform`; sockets with `Do` plus
  unsolicited messages; segments/ops timers by key; facts/assertions unchanged.
- P7.3 (Opus) production: `broker/internal/system` and `system/production` on `live.Host`
  with an executor; delete the route switch cases that duplicate effects.
- P7.4 (Opus) sim: cluster, fakeservices (objectstore, controlplane gRPC) as `sim.Service`;
  history unchanged. Delete `fakeservices/fakenetwork` (D22) and the `network` route in
  `cluster/route.go`. Replace it with a small connection table in the broker's sim executor:
  - Dial gives a ConnID; client writes arrive at the broker as one whole `BytesReceived`
    frame each, after a per-pair delay from `Env.Duration`, in FIFO order per connection.
  - Broker `Do(core.Send)` is delivered to the owning client actor the same way.
  - One fault: connection reset (both ends get a close; in-flight frames dropped), with the
    existing `random_close` probability. Partition, clog and black hole become resets or
    client timeouts; partial IO is dropped.
  - Coverage moved: partial reads and frame splitting are tested by a codec unit/fuzz test
    in `broker/internal/kafka` (add it if missing, before deleting fakenetwork).
  - Remove `fakenetwork.Faults` from `internal/preset` and the cluster `Settings`; keep the
    reset probability as `ConnResetProb`.
- P7.5 (Opus) sim clients: `session` as a `Sub` component; kafka producer/consumer workloads
  as actors.
- P7.6 (Haiku) delete `dst/core/machine`, `dst/sim/kernel`, the old `live.Run`/`Config`/
  `Effects`, `dst/verify/check/faulty`'s old adapter if replaced; update depguard so nothing
  can import them again; dstgen everywhere.
- P7.7 (orchestrator) golden regeneration per section 9; GATES-FULL; GATES-IT; broker `risk`
  target if Podman is available.

DoD: GATES-FULL and GATES-IT green; no file imports the deleted packages; counts vs baseline
(target: zero hand-written Fold/Tag/markers outside generated files, zero Route switches for
effects, zero timer-generation checks); `mdssimulator` campaign of 200 seeds clean.
Commits: `refactor(broker): broker and simulator on the actor API`,
`refactor(dst): remove machine, kernel and old live API`,
`test(golden): regenerate broker trace hashes for P7`.

### P8. Flow helper decision, docs, final review (Opus)

Steps:

- P8.1 (Opus) evidence for a flow helper: search controlplane and broker for multi-step
  sequences still kept by hand (stage enums with counters, e.g. replica checkpoint stages).
  Build `dst/core/actor/flow` only if at least two products repeat one pattern; otherwise write
  "not needed, because ..." in the ADR.
- P8.2 (Haiku) docs: `dst/README.md`, `docs/contribute/determinism.md`, glossary in
  `dst/core/actor/doc.go` and `dst/sim/doc.go`, architecture docs that name old types.
- P8.3 (Opus) final review: read the kv example, replica and broker as a new reader; list
  anything still confusing; compare metrics with the baseline (section 13).

DoD: GATES-FULL green; metrics table filled; review findings fixed or filed.
Commit: `docs(dst): actor API docs and redesign results`.

## 11. Risks

| Risk | Mitigation |
|---|---|
| The API freeze misses something a product needs | Sketches of every product machine in P1; P6/P7 Opus steps report gaps; ADR amendments need user approval |
| Golden changes hide behaviour changes | Section 9: all non-golden checks first; planted bugs must still be caught; campaigns of 200 seeds |
| `Perform` typing cannot be checked at compile time | O1 closed in P1 with dstgen help; else runtime protocol failure with a clear message |
| live host behaviour differs from sim | Same guarantee tests G1..G10 in both runtimes |
| Haiku makes unintended edits | Narrow briefs, GATES-FAST per step, Opus review before each commit |
| A long branch drifts from mainline | Rebase on mainline at the start of P4 and P6 |
| gRPC behaviour changes by accident | GATES-IT and existing server tests in P6 DoD |
| Deleting fakenetwork loses a bug class (partial IO, half-open connections) | Codec fuzz test lands before the delete (P7.4); the planted-bug check of section 9 must still catch every broker bug that fakenetwork-only faults caught at P0, or the gap is written in the ADR |

## 12. Deferred work (separate plan later)

- Typed state collections (`state.Map`/`Table`: canonical Merkle treap, snapshots, `Diff`)
  for reference models and linearizability checking.
- Composable fault decorators; promoting broker `history` into dst.
- Merging the clock packages.
- Non-deterministic specs (`Model.Step` returning several states) for SQS-style semantics.

## 13. Success metrics (filled in P8 against the P0 baseline)

| Metric | Baseline | Target |
|---|---|---|
| Hand-written Fold / Tag / marker methods | 120 / 112 / 147 | 0 outside generated files |
| Timer generation fields and checks | (P0) | 0 |
| Request-ID tables in product machines | (P0) | 0 where Perform/Call/Ask apply |
| Route switch cases that only forward effects | (P0) | 0 |
| kv example code lines (non-comment) | 202 | about 120 |
| Product output cases duplicating effects | 9 (broker 5, controlplane 4) | 0 |
| Determinism gate coverage | 10 listed packages | every core/sim package |

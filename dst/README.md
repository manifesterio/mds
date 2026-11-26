# dst

Deterministic simulation tools for Go tests. A product writes **actors** (`actor`): state machines that
take one message at a time and record effects on an outbox, and do no IO. The **simulator** (`sim`) runs many
actors in one goroutine, with one seeded `rng`, virtual time, a buggify injector, an assertion log and a `trace`
fingerprint per run. The same actors run in production on `live`.

Module: `github.com/manifesterio/mds/dst`

This module is a folder of the MDS monorepo. It imports no other module of the repo.

Docs: [docs/contribute/determinism.md](../docs/contribute/determinism.md).

## Build

From the repo root: `make -C dst build`. It runs format, ASCII, vet, lint, modernize, the tests and the
determinism check. The determinism tool is in `ops/tools/determinismcheck`.

## Packages

| Package | Role |
|---|---|
| `actor` | The actor API: `Actor`, `Props`, `Outbox` with its effects, the handles (`EntityRef`, `ServiceRef`, `ReplyTo`), `Perform` and `Sub`. Also the typed assertion handles (Always / AlwaysOrUnreachable / Sometimes / Reachable / Unreachable, `Pair`, `AssertKind`) and the declared buggify sites (`Site`, `NewSite`, `NewFaultSite`). Product actor code imports this package. Its types are aliases of `internal/core`, which holds the runtime seam for `sim` and `live`. |
| `actor/actortest` | A test `Env` and step helpers for unit tests of actors |
| `trace` | Running FNV-1a `Hash` |
| `sim` | The simulation kernel, mechanism only: `System` (queue, virtual time, trace hash, rules), `Spawn` with executors, workloads and `Cost`, `Tell` / `Ask` from host code, `Hold` / `Reply` and `Track` for outside systems, `Crash` / `Stop` / `Pause`, `OnStep`, `SetFaults`, `SetEnd`, `Run`. Its `Result` carries `Coverage` and `SiteCount`. |
| `sim/kit` | Simulation policy over the exported API of `sim`: `Phase` / `Phases` / `SchedulePhases` / `SetPhase` / `PhaseStart`, `Attrition`, `FaultCoverage` and its rule, `CostParams`, `FastWithRareSlow`, `DrawBetween`, `MaybeAdd`, `DeriveRand`, `RecordFault`. It cannot touch anything unexported in `sim`, and it must not import `live`, `campaign`, `spectest` or `examples`. |
| `live` | The production runtime for the same actors: a `Host` with one goroutine per actor, Control before Data inboxes, real keyed timers, an exec function per actor for product IO, and `Ask` / `Tell` / `OnLoop` for callers outside the actors. Real time and goroutines: it is not in the determinism gate. |
| `campaign` | The one campaign tool: scenario `Registry`, parallel seeds folded in seed order, one `Report` (text and JSON), until-stable mode, the determinism re-run, the standard flags and `<PREFIX>_SEEDS` / `_SEED` / `_CAMPAIGN` variables, and `Main` for a product command |
| `spectest` | The spec toolkit for tests, on rapid: `Spec`, `Lockstep` (ops, reboots, crashes, checkpoints), `SimplestFirst`, `SelfCheck`, planted bugs (`Bug`, `RequireCaught`, `RequireClean`), `Rerun`; also fixed seed-to-hash asserts (`AssertHash`, `AssertCampaign`) and golden file `LoadHashes` / `SaveHashes` |
| `ops/tools/dstgen` (repo root) | Generates the sum-type boilerplate (markers, `Tag`, `Fold`, state hash) into `zz_dst_gen.go`; tag numbers are kept in `dst.tags`. |

`actor.Env` is a struct (`*Rand`, `Faults`, `Assertions`, `Self()`) built by the runtime with `actor.NewEnv`; pass it by value. `actor.Rand` is the one implementation of every draw (`Intn`, `Between`, `Float64`, `Chance`, `NewID`) over any `actor.Source`. The simulator's source is the internal SplitMix64 stream (`internal/rng`); host setup before a `System` exists uses `kit.DeriveRand(seed, label)`.
The catalog of declared properties and sites (`internal/catalog`), the environment parser (`internal/seedenv`), the assertion log and the buggify injector (per-site latch and Quiet, inside `sim`) are internal too: code records and asks through the Env, and a harness reads coverage with `System.Coverage`.

## How to write an actor

An actor is a state machine. It takes one message at a time, does no IO, and records what it wants on
an `Outbox`. The runtime carries the effects out after `Receive` returns. `sim` runs the same actor code
in tests; `live` (`dst/live`) runs it in production with a `Host`.

```go
func (s *server) Receive(_ actor.Now, msg serverReq, outbox *actor.Outbox[serverReq, actor.NoIO]) {
	switch m := msg.(type) {
	case putValue:
		s.store.Tell(outbox, storePut(m))
	case getValue:
		actor.Call(outbox, s.store.Request(storeGet{Key: m.Key}), thenAnswer(m.ReplyTo))
	case storeAnswered:
		actor.Reply(outbox, m.Asker, serverReply(m.Answer))
	}
}

func (*server) StateHash() uint64 { return 0 }
```

`actor.Props` says how to run it, and is the same value in sim and live. `Name` is a label for logs.
`New` builds the actor on every boot and is required. `Boot` makes the first message of a boot,
`StopMsg` the graceful-stop request, `Priority` picks the Control or Data inbox, and `Restart` says
when the next boot starts (nil: the actor stays down).

Messages are a sum interface that embeds `actor.Payload`. Each case is listed in a `//dst:cases` comment:

```go
//sumtype:decl
//dst:sum
type serverReq interface {
	actor.Payload
	isServerReq()
}

//dst:cases serverReq
type (
	putValue struct{ Key, Value string }
	getValue struct {
		Key     string
		ReplyTo actor.ReplyTo[serverReply]
	}
)
```

Run `go generate` with `//go:generate go run github.com/manifesterio/mds/ops/tools/dstgen`. It writes `zz_dst_gen.go`
(markers, `Tag`, `Fold`) and numbers each case in `dst.tags`, which is append-only: never change or reuse a number.

Effects on the outbox:

- `outbox.TellSelf(msg)`: put msg in my inbox now, behind what is already there.
- `outbox.After(d, msg)`: deliver msg to me after d. One shot, nothing to cancel.
- `outbox.StartTimer(key, d, msg)`, `outbox.CancelTimer(key)`: named timers. A new timer with the same key replaces the old one.
- `outbox.Do(io)`: hand product IO to the executor. Use `actor.Perform` when the IO has one typed answer.
- `outbox.Stop(how, cause)`: end this incarnation with `actor.Trip` or `actor.Exit`. It must be the last effect.
- `outbox.Fact(f)`: add an observation to the timeline. It never changes behaviour.
- `outbox.ForgetPending()`: drop my timers and pending answers, keep running.
- `outbox.Watch(who, gone)`: deliver gone once if the asker behind `who` ends before I reply.

Sends go through handles and the outbox:

- `ref.Tell(outbox, msg)` sends to an actor, and `svc.Tell(outbox, req)` to a service. No answer.
- `actor.Ask(outbox, ref, makeReq, adapt)` asks an actor. `makeReq` puts a fresh `ReplyTo` in the request,
  and the answer comes back as my own message, built by `adapt`.
- `actor.Call(outbox, svc.Request(req), adapt)` asks a service. It gets the reply handle itself.
- `actor.Reply(outbox, replyTo, answer)` answers an Ask I received.
- `actor.Perform(outbox, req, adapt)` hands a product IO request (a case with `//dst:answer`) to the executor.
- `actor.Sub(outbox, name, wrap)` lends a component its own outbox. Its messages come back wrapped into mine.

Sends are methods on the handles because Go cannot infer `P` in `actor.Tell(o, server, putValue{...})`.
A method checks the argument against the handle's own type instead (ADR 2.1, item 1).

To run a system, use `sim`:

```go
system := sim.NewSystem(sim.Config{Seed: seed})
serverRef := sim.Spawn(system, actor.Props[serverReq, actor.NoIO]{Name: "server",
	New: func(actor.Env) actor.Actor[serverReq, actor.NoIO] { return &server{} }})
sim.Spawn(system, actor.Props[clientMsg, actor.NoIO]{Name: "client",
	Boot: func(actor.Incarnation) clientMsg { return start{} },
	New:  func(env actor.Env) actor.Actor[clientMsg, actor.NoIO] { return &client{env: env, server: serverRef, script: script} },
}, sim.Workload())
return system.Run()
```

`sim.Workload()` marks the driver of the script (its panic halts the run). The same seed gives the same
trace hash. `examples/kv` is the full example.

## Docs

- [docs/contribute/determinism.md](../docs/contribute/determinism.md) - dst overview, allow-list, campaigns

## How to run campaigns

A product simulator command is a few lines. It registers its scenarios and calls `campaign.Main`.
`broker/cmd/mdssimulator` is a full example (one scenario, `kafka`).

```go
reg := &campaign.Registry{}
reg.Add(campaign.Scenario{Name: "kv", Detail: "a store with no faults",
    Run: func(seed uint64, o campaign.RunOptions) campaign.Outcome {
        sys := sim.NewSystem(sim.Config{Seed: seed, Log: o.Log, HaltOnFirst: o.HaltOnFirst, Tail: 20})
        // ... Spawn: see dst/examples/kv/kv.go ...
        return campaign.Outcome{Result: sys.Run()} // no Reproduce: the replay command is made from Config.Tool
    }})
cfg := campaign.Config{Tool: "mysim", EnvPrefix: "MDS", Defaults: campaign.Options{CheckEvery: 10}}
os.Exit(campaign.Main(cfg, reg, os.Args[1:], os.Stdout, os.Stderr))
```

```bash
go run ./cmd/mysim -seeds 200 -report         # 200 seeds, with the report
go run ./cmd/mysim -scenario kv -seed 7       # one seed, with its step log
go run ./cmd/mysim -until-stable              # more seeds until no new assertion appears
go run ./cmd/mysim -json                      # the report as JSON, nothing else
MDS_CAMPAIGN=30s go run ./cmd/mysim           # seeds until 30 seconds have passed
```

Flags: `-seeds`, `-first` (or `-start`), `-seed` (replay one seed), `-scenario` (a name, a list with
commas, or `all`), `-j`, `-report`, `-json`, `-v`, `-until-stable`, `-stable-after`, `-max-seeds`,
`-campaign` (a time limit), `-check-every`. The variables are `<EnvPrefix>_SEEDS`, `<EnvPrefix>_SEED`
and `<EnvPrefix>_CAMPAIGN`. A flag wins over its variable. `ParseOptions` is the one place that reads both.

The report lists: counts, failed runs with the command that replays them, every assertion with its
checks and passes, the paths that were never reached, the Always properties that were never checked,
the buggify sites (asked, on, calls, fired), the sites that never fired, and the coverage curve.
"Never reached", "never checked" and "never fired" come from the catalog (`catalog.Asserts`,
`catalog.Sites`), so a path that never ran is still named. Every `CheckEvery`-th seed runs twice with the
step log switched, and a different hash, step count or number of random calls fails the seed
("nondeterminism"). A campaign of `MinSeedsForCoverage` seeds or more also fails on a coverage gap.

`campaign.RunSeeds` is the parallel fold under it. It stamps `Result.Seed`, folds hashes in seed
order, and merges coverage by summing `Checks` and `Passes`. A test can call it directly.
Same seed must still replay to the same hash. Cross-seed hash collisions are normal and not signal.

For planted bugs, use `spectest.RequireCaught` so each bug fails at least once in seeds `1..budget`, and `spectest.RequireClean` so the same budget raises no false alarms. With a `spectest.Lockstep`, `RunSeed(seed)` runs one seed and returns the shrunk failure.

A failed `spectest.Lockstep` prints the seed, the shrunk steps as Go code, and the command to run it again. The product sets `Rerun.EnvPrefix` (for example `P`); then `P_SEED=<n> go test -run '^TestX$' -count=1 -v .` runs only seed n. `Check` reads the same three variables as the campaign tool, with the same parser (`internal/seedenv`): `P_SEED` (one seed), `P_CAMPAIGN` (seeds until a time has passed) and `P_SEEDS` (the number of checks). `Lockstep.Checks` and `Lockstep.StepsPerCheck` set the number of checks and the average number of steps in a check.

```bash
go test ./examples/kv -run TestCampaign -count=1
go test ./campaign ./spectest ./...
```

## Host layer ownership

`dst` is private to this repo. A helper lives in `dst` only if both products use it or it is generic
simulation policy. The split:

- Kernel, `sim`: mechanism only. The queue and virtual time, `Spawn` and its options (`WithExecutor`,
  `Workload`, `Cost`, `AfterReceive`, `OnEnd`), executors (`Answer`, `Hold` / `Reply`, `Tell`), `Ask`,
  `Track`, the lifecycle (`Boot`, `Crash`, `Stop`, `Stopping`, `Pause`, `Resume`, `Up`), the buggify switch
  (`SetFaults`, `FaultsOn`, the `sim_faults` fact), `OnStep` (a hook after every queue pop), `SetEnd`, rules
  and the timeline. The kernel facts are `sim_crashed`, `sim_exited`, `sim_tripped`, `sim_restarted`,
  `sim_stop_requested` and `sim_faults`.
- Policy, `sim/kit`: phases (`SchedulePhases`, `SetPhase`, `PhaseStart`), `Attrition` (built on `OnStep`,
  `FaultsOn`, `Crash` and `Stop`), `FaultCoverageRule`, the CPU cost model for `sim.Cost`, `DrawBetween` /
  `MaybeAdd`, `DeriveRand`, `RecordFault`.
- Product helpers stay in the product. The control plane simulator keeps `everyWhile`, `askDelayed` and
  `quietElapsed` (its own run loop drives the quiet and final tails). The broker simulator keeps its outside
  systems (object store, control plane) as services in `broker/internal/verify/simulator/service`, which is
  `sim.Hold` plus a handler and `sim.Track`, and its quiesce points in `cluster`.
- Elsewhere: `live.NewEnv(self, opts...)` (one production Env; `OnViolation` or `LogViolations` choose what a
  broken assertion does), `live.RealClock` / `live.FakeClock`; `campaign.Outcome` / `SetupFailure`;
  `trace.Hash`; `actor/actortest`.

Domain schedules, fault rates, and message vocabularies stay in the product.

Controlplane replica IO (shared MemoryLog/MemoryObjects, per-node storage cut,
host-owned timeout/lost coins) stays on a product `WithExecutor`, not on
the broker `service` package + `faulty.Service` (the broker faulty kit). Broker fakeservices Forward cleanly because
outside RPCs are caller-blind and partitions sit on the socket table; CP
leader-fencing needs Issuer-scoped cut and timeout-never-applied semantics that
`faulty` does not provide. Do not treat the broker service package as the required home for
shared storage fakes.

## Stay out

These stay out of this module on purpose:

- Closed domain message vocabularies (Kafka, S3, product RPCs)
- Product specs, and product scenario registries
- Product I/O fakes beyond the tiny KV examples
- Full network-partition simulators

Generic spec machinery is in: `spectest` works on any spec with `Apply` and
`StateHash`. It has no product words. A product brings its own spec, its own fault numbers and its
own message types.

Product wall clocks, product fault policy, and domain IDs belong in the importing project.

## License

Apache-2.0. See [LICENSE](LICENSE).

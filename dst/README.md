# dst

Deterministic simulation tools for Go tests. A product writes **actors** (`core/actor`): state machines that
take one message at a time and record effects on an outbox, and do no IO. The **simulator** (`sim`) runs many
actors in one goroutine, with one seeded `rng`, virtual time, a `fault` injector, an `assert` log and a `trace`
fingerprint per run. The same actors run in production on `runtime/live`.

Module: `github.com/manifesterio/mds/dst`

This module is a folder of the MDS monorepo. It imports no other module of the repo.

Docs: [docs/contribute/determinism.md](../docs/contribute/determinism.md).

## Build

From the repo root: `make -C dst build`. It runs format, ASCII, vet, lint, modernize, the tests and the
determinism check. The determinism tool is in `ops/tools/determinismcheck`.

## Packages

| Package | Role |
|---|---|
| `core/actor` | The actor API: `Actor`, `Props`, `Outbox` with its effects, the handles (`EntityRef`, `ServiceRef`, `ReplyTo`), `Perform` and `Sub`. Product actor code imports this package. |
| `sim` | The simulator for actors: `System`, `AddService` services, `Spawn` with executors and workloads, `Run`. |
| `runtime/live` | The production runtime for the same actors: a `Host` with one goroutine per actor, Control before Data inboxes, real keyed timers, an exec function per actor for product IO, and `Ask` / `Tell` / `OnLoop` for callers outside the actors. Real time and goroutines: it is not in the determinism gate. |
| `core/sched` | Virtual mono clock and `(at, tie, seq)` event heap (live/clock adapters; sim keeps its own queue) |
| `core/clock` | Wall offset over `core/sched`, `AfterFunc` / `Advance` on the same heap |
| `core/syncclock`, `core/realclock` | Clock adapters for code that needs a real or synchronised clock outside a run |
| `core/trace` | Running FNV-1a `Hash` / `StateHasher` |
| `core/assert` | Typed handles (Always / AlwaysOrUnreachable / Sometimes / Reachable / Unreachable), `Coverage` and the catalog |
| `core/fault` | Declared buggify sites (`NewSite`, `NewFaultSite`), `SiteCount` and the catalog |
| `explore/campaign` | The one campaign tool: scenario `Registry`, parallel seeds folded in seed order, one `Report` (text and JSON), until-stable mode, the determinism re-run, the standard flags and `<PREFIX>_SEEDS` / `_SEED` / `_CAMPAIGN` variables, and `Main` for a product command |
| `verify/check/spectest` | The spec toolkit for tests, on rapid: `Spec`, `Lockstep` (ops, reboots, crashes, checkpoints), `SimplestFirst`, `SelfCheck`, planted bugs (`Bug`, `RequireCaught`, `RequireClean`), `Rerun` |
| `verify/check/faulty` | Puts timing and faults (slow replies, errors, timeouts, lost answers, outages) around a fault-free spec, so the code under test meets an outside system that fails |
| `verify/golden` | Fixed seed-to-hash asserts plus optional golden file Load/Save |
| `verify/history` | Append-only client-operation history (`Invoke`/`Return`) for workload/oracle separation |
| `ops/tools/dstgen` (repo root) | Generates the sum-type boilerplate (markers, `Tag`, `Fold`, state hash) into `zz_dst_gen.go`; tag numbers are kept in `dst.tags`. |

The run stream (SplitMix64) is internal (`internal/rng`): code draws through `actor.Rand` (the Env), and host setup before a `System` exists uses `sim.DeriveRand(seed, label)`.
The assertion log (`internal/assertlog`) and the buggify injector (`internal/inject`, per-site latch and Quiet) are internal too: code records and asks through the Env, and a harness reads coverage with `System.Coverage`.

## How to write an actor

An actor is a state machine. It takes one message at a time, does no IO, and records what it wants on
an `Outbox`. The runtime carries the effects out after `Receive` returns. `sim` runs the same actor code
in tests; `live` (`dst/runtime/live`) runs it in production with a `Host`.

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
storeRef := sim.AddService[storeReq, storeReply](system, "store", theStore)
serverRef := sim.Spawn(system, actor.Props[serverReq, actor.NoIO]{Name: "server",
	New: func(actor.Env) actor.Actor[serverReq, actor.NoIO] { return &server{store: storeRef} }})
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
    Run: func(seed uint64, o campaign.RunOptions) campaign.Result {
        sys := sim.NewSystem(sim.Config{Seed: seed, Log: o.Log, HaltOnFirst: o.HaltOnFirst, Tail: 20})
        // ... AddService, Spawn: see dst/examples/kv/kv.go ...
        return campaign.FromSim(sys.Run(), "") // "": the replay command is made from Config.Tool
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
"Never reached", "never checked" and "never fired" come from the catalog (`assert.Catalog`,
`fault.Sites`), so a path that never ran is still named. Every `CheckEvery`-th seed runs twice with the
step log switched, and a different hash, step count or number of random calls fails the seed
("nondeterminism"). A campaign of `MinSeedsForCoverage` seeds or more also fails on a coverage gap.

`campaign.RunSeeds` is the parallel fold under it. It stamps `Result.Seed`, folds hashes in seed
order, and merges coverage by summing `Checks` and `Passes`. A test can call it directly.
Same seed must still replay to the same hash. Cross-seed hash collisions are normal and not signal.

For planted bugs, use `spectest.RequireCaught` so each bug fails at least once in seeds `1..budget`, and `spectest.RequireClean` so the same budget raises no false alarms. With a `spectest.Lockstep`, `RunSeed(seed)` runs one seed and returns the shrunk failure.

A failed `spectest.Lockstep` prints the seed, the shrunk steps as Go code, and the command to run it again. The product sets `Rerun.EnvPrefix` (for example `P`); then `P_SEED=<n> go test -run '^TestX$' -count=1 -v .` runs only seed n. `Check` reads the same three variables as the campaign tool, with the same parser (`campaign.ReadEnv`): `P_SEED` (one seed), `P_CAMPAIGN` (seeds until a time has passed) and `P_SEEDS` (the number of checks). `Lockstep.Checks` and `Lockstep.StepsPerCheck` set the number of checks and the average number of steps in a check.

```bash
go test ./examples/kv -run TestCampaign -count=1
go test ./explore/campaign ./verify/check/... ./...
```

## Host layer ownership

Product simulators and production Envs should call dst for shared host glue rather
than re-owning it: `live.NewEnv` / `NewLoggingEnv` / `NewCounterEnv`;
`sim.Bootstrap`, `RecordFault`, `AskDelayed`, `QuiesceEvery`, `EveryWhile`,
`Timeline.PhaseStart`, `FaultCoverageRule`, `DrawBetween` / `MaybeAdd`;
`campaign.RegisterSim` / `FromSim`; `faulty.NamedAsserts` / `ShapeOf`;
`trace.StateHasher`; `core/actor/actortest`; `verify/check.Verdict`;
`verify/history`. Domain schedules, fault rates, and message vocabularies stay
in the product. Sched merge into sim remains deferred (sim keeps its own queue).

Controlplane replica IO (shared MemoryLog/MemoryObjects, per-node storage cut,
host-owned timeout/lost coins) stays on a product `WithExecutor`, not on
`AddService`+`faulty.Service`. Broker fakeservices Forward cleanly because
outside RPCs are caller-blind and partitions sit on the socket table; CP
leader-fencing needs Issuer-scoped cut and timeout-never-applied semantics that
`faulty` does not provide. Do not treat AddService as the required home for
shared storage fakes.

## Stay out

These stay out of this module on purpose:

- Closed domain message vocabularies (Kafka, S3, product RPCs)
- Product specs, and product scenario registries
- Product I/O fakes beyond the tiny KV examples
- Full network-partition simulators

Generic spec machinery is in: `verify/check/spectest` and `verify/check/faulty` work on any spec with `Apply` and
`StateHash`. They have no product words. A product brings its own spec, its own fault numbers and its
own message types.

Product wall clocks, product fault policy, and domain IDs belong in the importing project. Use `core/clock` only for virtual time over `core/sched`.

## License

Apache-2.0. See [LICENSE](LICENSE).

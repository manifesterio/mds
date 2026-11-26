# Starter prompt: dst public-surface internalization

Copy everything below the line into a new Cursor session on branch `slim/verification`.

---

## Mission

Redraw the **dst public contract** so extracted shared guts stop looking like product API. Broker and controlplane were built independently; dst was cut out of them later — that is why so much implementation sits on public import paths.

**Thesis (user):** if only Env / fault / sim / campaign need RNG draws, **`core/rng` must not be a public package**. That pattern generalizes to sched/clocks/Ring/Injector/Log/sim.Rand leaks.

**Baseline:** `make all` is green on `slim/verification` as of the prior session. Prefer hash-neutral work; if goldens must move, say why in the commit message.

## Hard rules

1. **Same branch only:** `slim/verification`. No stacked PRs, no new branches unless the user asks.
2. **Plan first, then execute.** Before coding, write a meticulous step plan (below). Each step must be handable to a **subagent** with its own brief, DoD, and out-of-scope list.
3. **Commit frugally:** the pre-commit hook runs `make build` (expensive). Commit **once at the end of each completed step** (or fewer), never mid-step. Do not invent a giant plan doc that bloats the tree; keep the plan in the chat (or a short scratch note you delete before commit).
4. **Anti-bloat:** delete product/other-dst imports of a gut package **before** unexporting it. dst must not grow a parallel public wrapper surface. Product-negative or near-flat line deltas preferred.
5. **Cross-module constraint:** broker/CP cannot import `dst/internal/...`. “Internalize” means stop exporting / stop leaking types through sim/Env doors, then relocate or unexport once no external importer remains.
6. **Use subagents** for implementation steps. Parent: plan, verify DoD, commit, kick next step.
7. **ASCII only** in dst Go comments. Match local style. Modern Go guidelines when editing Go.
8. Do **not** force CP MemoryLog onto `AddService`+`faulty` (already deferred: Issuer-scoped cut / timeout-never-applied).

## Public contract to KEEP (product-facing)

- `core/actor` (actors, Outbox, Env, refs)
- `assert` / `fault` **catalogs only** (`NewAlways`, `NewSite`, …) — calls go through Env
- `trace.Hash` (+ dstgen Fold) for StateHash
- `sim` / `live` **entrypoints** (System/Host/Spawn/Run)
- `campaign` Main / RegisterSim

## Guts to INTERNALIZE / NARROW (priority order)

| Priority | Target | Intent |
|---|---|---|
| 1 | Stop **rng leaks** through sim | Remove/replace `System.Rand()`, `ServiceCtx.Rng()`, Bootstrap’s `*rng.Rand` return, APIs taking `*rng.Rand` (e.g. CostParams.Sample). Host draws via Env or unexported helpers. |
| 2 | **Internalize `core/rng`** | Migrate broker/CP verify off direct `*rng.Rand` (swarm/preset/services) onto Env/System APIs; then unexport package or move so products cannot import it. |
| 3 | **`sched` + `clock`** | sim has its own queue; sched only feeds clock adapters. Fold/hide; do not pretend shared kernel. |
| 4 | **`syncclock` / `realclock`** | Only `shared/clock` uses them; park under live/shared, not public “core”. |
| 5 | **`trace.Ring`** | Hide inside sim; keep Hash public. |
| 6 | **Narrow assert/fault** | Keep New*/Site public; hide Log/Injector construction behind Env/System. |
| 7 | **Narrow sim fat surface** | Unexport dead/near-dead; don’t market broker-only and CP-only helpers as one universal API. |
| 8 | **Admit verify asymmetry** | check / faulty / history are harness kits (CP- or broker-shaped), not actor core — document or relocate later; don’t force a mega-unify in this pass. |

## How you must work

### Phase 0 — Plan (chat only, no commit)

Produce a step list where **each step** has:

- **Goal** (one sentence)
- **Subagent brief** (copy-paste ready: files to read, edits allowed, forbidden edits)
- **DoD** (checkable: importers gone, tests named, `make -C dst build` and named product packages)
- **Commit policy:** one commit at step end with message focusing on *why*; hook will run `make build`
- **Rollback signal:** what failure means stop/defer

Do not start Step 1 until the plan covers at least priorities 1–2 fully and sketches 3–7.

### Phase 1+ — Execute via subagents

For each step:

1. Spawn subagent with the brief; tell it **DO NOT commit**.
2. Parent verifies DoD (importers, tests, line delta sanity).
3. Parent commits **once** for that step.
4. Only after several steps (or at the end of the session): run **`make all`** if the user asks, or after the last step of the session.

### Suggested first steps (refine in Phase 0)

1. Inventory current importers of `rng` / `System.Rand` / `ServiceCtx.Rng` / Bootstrap returns (evidence table).
2. Replace sim RNG leaks with Env-only (or unexported) draws; migrate call sites in dst + products.
3. Migrate broker/CP verify off `*rng.Rand` signatures.
4. Unexport or quarantine `core/rng` once zero external importers.
5. Then sched/clocks/Ring/narrowing — separate steps, each with DoD.

## Out of scope

- Stacked PRs / new feature branches
- Absorbing product oracles, network tables, RoleChanged, MemoryLog, IO sumtypes
- Giant markdown plan files committed to the repo
- Deslop-count theater (ownership first; deslop is trailing)

## Success for this session

- At least **rng leak stop + rng internalization path started or finished** with green `make -C dst build` and touched product tests.
- Each finished step has **one** commit on `slim/verification`.
- A short status in chat: what internalized, what deferred, next step for a later session.

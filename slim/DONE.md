# Executed: actions 1-5 (measured low-risk tier)

Lines are `git show --numstat` of each commit (deleted, added). Actions 6+ were not done.

| Action (PLAN id) | Commit | Lines removed (net) | Test-time change | Notes |
|---|---|---|---|---|
| 1. fake-service parity suite (A1) | 87a65ad | 2,063 (-2,063/+0) | fakeservices tests ~2 s, small | Also dropped the unused `Harness.samples` field. |
| 2. echo scenario (A3) | e6f8929 | 1,399 (-1,417/+18) | about -9 s (`make sim` go test) | Dropped the `Owns` ownership rules in mdssimulator (single scenario owns every name) and 3 two-scenario tests. `preset_test` switched to `KafkaFaultCoverage`. Kafka campaign hash unchanged. |
| 3. CP half of speccheck conformance/injectedbug (A2) | 969ec07 | 1,355 (-1,372/+17) | speccheck+injectedbug ~25 s of `make test` removed | `result.go` CP result types, their tests and the CP weights in `profile.go` kept (second pass, see PLAN). |
| 4. dst examples kvchaos, kvsim (A5) | 0ac2162 | 633 (-647/+14) | <1 s | dst/README.md and docs/contribute/determinism.md updated. |
| 5. kafka capdiff_test.go (A4) | 5fd4d0e | 84 (-84/+0) | `simulator/kafka` package 105 s -> 36 s (measured) | |
| Total | | 5,534 net (-5,583/+49) | about -75 s kafka sim, -9 s echo | |

Pre-commit hook (full `make build`, with Podman integration tests) passed for every commit; no `--no-verify`.

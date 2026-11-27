# Behavioral Smoketest Suite

A suite of tests to check that notifications get sent properly.

## Setup

1. Nothing to set up: each test keeps its data in a Delta table in memory.

## Running Tests

Run `make smoketest` from the root of the repo to run all tests.
The script will automatically rebuild goalert.

To run a single test you can use `go test` from the smoketest directory.

### Experimental Flags

Tests that need an experimental flag pass it explicitly via `harness.NewHarnessWithFlags`.

To run the whole suite (or a subset) with additional experimental flags enabled, without editing any tests, set `SMOKE_EXP_FLAGS` to a comma-separated list of flag names (the same names accepted by `goalert --experimental`):

```sh
cd test/smoke && SMOKE_EXP_FLAGS=example go test -run 'Rotation|Schedule' .
```

The flags are merged into the set used by every harness, including those created with `NewHarness` and `NewHarnessWithFlags`. This is how a change gated behind an experimental flag is proven to preserve behavior: the unchanged smoke tests must pass with the flag off (the default) and with it on.

## Creating Tests

- Try to keep the test under 1 minute, where possible.
- Fixtures load at the latest schema; the migration name a harness takes only records the schema a fixture was first written for.
- Make sure to call `t.Parallel()` and `defer h.Close()` in your test.

## Engine Cycles

The harness drives the backend engine explicitly rather than waiting for its timer: `h.Trigger()` runs one engine cycle on every running pod and waits for it to finish, so the pod that holds the engine's lease does the work. Most assertion helpers (`ExpectSMS`, webhook expectation helpers, `WaitAndAssertOnCallUsers`, ...) call `Trigger` for you inside their retry loops. There is no job queue: the engine finds its work in the data.

Note that `h.FastForward` moves the storage's clock between engine cycles.

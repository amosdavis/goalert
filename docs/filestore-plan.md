# Filestore Backend: Plan

This plan replaces PostgreSQL with an in-memory store and an append-only log on
disk. Every phase must follow [filestore-failure-modes.md](filestore-failure-modes.md).
A phase is done when its exit checks pass, not when its code is written.

## Design

All data lives in memory as Go structs indexed by maps. One writer applies
every change. It checks the change, stamps IDs and timestamps into a log
record, appends the record to the log, fsyncs, applies the record to memory and
only then reports success. Readers get an immutable snapshot, so they never see
a half-applied change. Startup loads the newest snapshot and replays the log
after it. Applying a record is deterministic, so the same log always rebuilds
the same state, whether on restart or (later) on another pod.

A running GoAlert uses one backend for everything (`--backend=postgres|file`),
never a mix. Until Phase 5, both backends sit behind the same interfaces, and
progress is measured by how many smoke tests pass on the file backend.

## Phase 0: Make the test harness backend-neutral

The 141 smoke tests are the acceptance check (F22), but the harness assumes
Postgres:

- 128 tests seed their data with raw SQL `INSERT`s.
- 133 tests load their fixtures at an old schema version (44 of them at
  `ids-to-uuids`) and rely on the migrations to upgrade the data. The file
  backend has no migrations.
- Time is moved by changing Postgres's clock (`pgmocktime`).
- River jobs are counted with SQL.
- The database is dumped when a test fails.

Work:

1. Record a baseline of which tests pass on this machine before any change.
2. Move every fixture to the latest schema. Load fixtures after all
   migrations, and change fixture SQL only where a test then fails. No
   assertion changes. Tests of what a migration does to existing data keep
   their pinned version through `harness.NewMigrationHarness` until the
   migrations are removed in Phase 5. Those are `TestMigrations`,
   `TestInProgress`, `TestStatusInProgress` and `TestNCDedup`.

   Each translated fixture reproduces the state the old fixture had after
   migrating. This was checked by loading both versions and diffing the
   data. What had to be reproduced:
   - renamed columns (`alerts.description`, `rotations.schedule_id`,
     `user_verification_codes.user_id`)
   - values the database no longer fills in: alert dedup keys, through the
     `autoDedupKey` fixture function
   - backfills: `enable_status_updates`, 24-hour schedule rules, and a
     rotation already on a later shift
   - alerts already escalated, with their notification cycles and
     `created` log
   - data that predates a limit, loaded by lifting the limit and then
     restoring it
3. Put fixtures, the clock, job checks and failure dumps behind interfaces.
   The Postgres implementation does exactly what the harness does today.

Exit: the same tests pass as in the baseline, with no assertion changed.

## Phase 1: Storage core (`filestore/`)

- Log records are length-prefixed, checksummed and carry a format version. A
  torn tail is detected and refused, never guessed at (F6, F19).
- There is one writer. The log file is fsynced before success is returned, and
  so is its directory when a file is created (F1). Each write carries the
  version it read, so a stale write fails instead of overwriting newer data
  (F4).
- Reads come from immutable snapshots (F10).
- Snapshots and log compaction, plus a consistent `backup` command (F9, F16).
- An exclusive OS lock on the data directory. Network filesystems are refused
  where they can be detected (F11). Files are `0600`, or have restrictive ACLs
  on Windows (F20). A full disk fails loudly (F21).
- A `Clock` interface shared by the store, the engine and the harness (F13).
  Postgres `now()` appears about 125 times in the SQL, and all of those move to
  it.

Exit:

- Crash tests kill the process at every step of a write, restart it, and compare
  the state.
- A test replays every log and compares the state (F25).
- Both pass on Linux and on Windows (F14).

## Phase 2: Replace the transaction seam (still on Postgres)

122 functions take a `*sql.Tx`, and 37 take a generic query interface, so that
several stores can write in one transaction. They switch to a backend-neutral
transaction type. On Postgres it is a normal transaction. On the file backend it
is a snapshot plus one batch of log records. Direct SQL outside the stores (for
example in `graphql2/graphqlapp`) moves into the stores.

Inventory every trigger (42), function (67), cascade (75), `CHECK` (31) and
unique constraint. Map each one to the Go code that will replace it and the
test that proves it (F2, F7, F8).

Exit:

- Only store implementations import `database/sql` or `gadb`.
- The suite is green on Postgres.
- Every row in the inventory is mapped.

## Phase 3: File backend for the stores, leaf packages first

Port the packages in this order:

1. config, keyring, auth (nonces, sessions)
2. users, contact methods, notification rules
3. services, integration keys, heartbeats, labels
4. escalation policies
5. rotations, schedules, rules, overrides
6. alerts, alert log, dedup
7. notification channels, calendar subscriptions, API keys, universal keys,
   signals, favorites, limits

Two pieces of work go with the port:

- A test-only fixture loader parses the fixture SQL and applies it through the
  file backend's normal write paths, so the fixtures also exercise the ported
  trigger logic. Fixtures use a small subset of SQL: `INSERT ... VALUES`,
  `UPDATE ... SET ... WHERE column = value`, `now()` plus or minus an
  interval, and casts. Values such as dedup keys come from fixture
  template functions, never from SQL functions. The loader supports that
  subset and rejects anything else.
- Search becomes in-memory matching. Postgres full-text search (`textSearch`)
  has no exact equivalent, so the difference is documented (F24).

Exit for each package: its inventory tests pass on the file backend, and so does
every smoke test that needs only the packages ported so far (F5, F23).

## Phase 4: Engine and jobs on the file backend

- Each engine module's SQL becomes Go over the in-memory indexes. A cycle-time
  budget and a large-dataset benchmark guard against slowdowns (F15, F17).
- The processing lock becomes the single writer, and the advisory locks go away.
- Outgoing message states keep exactly the semantics they have today (F3).
- River (14 job kinds, 2 clients) is replaced by a small job queue whose jobs are
  records in the same log, so a job commits together with the change that
  created it.

Exit: all 141 smoke tests pass on the file backend.

## Phase 5: Remove Postgres

- Delete `migrate/`, `gadb/`, `sqlc.yaml`, `swo/`, River and its UI, and the
  Postgres devtools.
- Replace `--db-url` and related flags with `--data-dir`.
- List every removed or changed feature in the release notes (F24).
- State the single-instance limit in the docs and in the startup logs (F12).

## Phase 6 (optional): Several pods with a replicated log

Do this phase only if more than one pod is wanted. `hashicorp/raft` uses the
Phase 1 log as its log store and the in-memory store as its state machine:

- Each pod has its own disk, and pods never share a filesystem.
- Only the leader runs the engine (`Engine.Pause` and `Resume` already exist).
- Followers forward writes to the leader.

This phase must satisfy F26 through F30.

## Status

- 2026-11-20: Plan written. Phase 0 started on branch `filestore-backend`.
- 2026-11-20: Phase 0 baseline: all 149 top-level smoke tests pass on
  Windows against Postgres 17.12.
- 2026-11-20: Phase 1 done. `filestore/` passes its crash, process-kill,
  replay, locking and permission tests on Linux, Windows and macOS
  (Filestore workflow).
- 2026-11-20: Phase 0 step 2: fixtures load at the latest schema. 36 fixtures
  were translated and 3 migration tests stay pinned. The Smoke workflow runs
  the full suite on every push to the branch.
- 2026-11-21: Phase 0 step 3: the harness reaches the database only through
  a `storage` interface (`test/smoke/harness/storage.go`), whose Postgres
  implementation does what the harness did before.
  `overrideconflict_test.go` still opens transactions itself on purpose;
  it moves with the transaction seam in Phase 2. Fixing a CI hang also
  fixed a pre-existing deadlock: River's smoke-test clock shared the app's
  5-connection pool.

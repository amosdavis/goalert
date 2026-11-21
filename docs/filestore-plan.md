# Delta Lake Backend: Plan

This plan replaces PostgreSQL with Delta Lake tables in object storage, on GCS
and on Azure, shared by several GoAlert pods. Every phase must follow
[filestore-failure-modes.md](filestore-failure-modes.md), and
[filestore-inventory.md](filestore-inventory.md) lists the database behavior
that has to move into Go. A phase is done when its exit checks pass, not when
its code is written.

## Design

**Table layout.** All of GoAlert's data is one Delta table at a configured
location (`gs://bucket/prefix`, or a container and prefix in an Azure storage
account). A Delta commit is atomic only within one table, so this is what
makes each GoAlert transaction atomic (F2).

- Partition columns `tbl` (the GoAlert table) and `bucket` (a hash of the
  row's key, modulo a per-table bucket count) keep files small (F39).
- Each row has a `key` and one struct column per GoAlert table, and only that
  table's column is set. Spark reads a table as
  `SELECT alerts.* FROM goalert WHERE tbl = 'alerts'`, and GoAlert ships those
  view definitions.
- Only Delta protocol reader version 1 and writer version 2 features are used,
  so any Delta reader can open the table (F19, F35).

**Commits.** A GoAlert transaction becomes one Delta commit:

- For each bucket it changes, a new Parquet file holds the bucket's rows after
  the change, and the old file is removed.
- Append-only tables (alert logs, message history) only add files with the
  new rows, and are compacted later.
- Data files are uploaded first. Then the commit file `_delta_log/<N>.json` is
  written create-if-absent, so only one writer can create version N (F6). Its
  `commitInfo` names the writer, the store ID, GoAlert's format version and a
  commit ID (F11, F30, F32).
- Transactions are grouped: a pod waiting on a commit queues the next
  transactions and commits them together (F15).

**Pods.** Every pod holds the whole current state in memory, in the tables,
indexes and copy-on-write views from Phase 1. It loads the newest checkpoint
and the commits after it, then follows the log by checking for the next
version.

- A write is optimistic. The transaction runs against the pod's latest
  version. If the next version already exists, the pod catches up and runs
  the transaction again (F4). Transaction functions therefore do no I/O (F3).
- One pod holds the engine lease, a row in the table, and runs the engine.
  The lease only avoids wasted work. A message is sent only after the commit
  claiming it, so a second engine cannot page twice (F3, F28).
- A write response carries its version, and a read can wait for that version
  (F29).
- The lease holder also writes checkpoints, removes old log entries, vacuums
  and compacts (F16).

**Object storage.** One interface (`Get`, `List`, `PutIfAbsent`, `Put` for
`_last_checkpoint` only, and `Delete`) has four implementations: GCS (create
with `ifGenerationMatch=0`), Azure Blob Storage (create with
`If-None-Match: *`), a local directory for development, and an in-memory fake
that injects conflicts, unclear results and throttling for tests.

A running GoAlert uses one backend for everything, never a mix (F23). Until
Postgres is removed, both backends sit behind the same interfaces, and
progress is measured by how many smoke tests pass on the new backend.

**Storage settings (F34).** The bucket or container holds nothing but
GoAlert's table, under its prefix. At startup a self-test checks the
credentials and create-if-absent (F33), and `CheckSettings` refuses the
settings the API shows to break the table. Required:

- Private, with no public access (F20). On GCS, enforce public access
  prevention; on Azure, the container's public access level is off.
- No lifecycle rule that deletes live objects under the prefix, however old:
  a data file stays in use as long as its rows don't change (F6). Rules for
  noncurrent versions only are fine.
- No retention policy, event-based hold, immutability policy or legal hold on
  the table's objects: `_last_checkpoint` is replaced, and cleanup deletes
  old files (F16, F36).
- Recommended: versioning and soft delete off, or a lifecycle rule that
  deletes noncurrent versions, since every commit replaces bucket files
  (F37); a dual-region GCS bucket or geo-redundant Azure storage (F31); pods
  in the bucket's region (F37).

Azure account settings (blob soft delete, versioning, lifecycle management
policies) can't be read with container credentials, so the deployment
checks them.

## Phase 0: Make the test harness backend-neutral (done)

The 141 smoke tests are the acceptance check (F22), but the harness assumed
Postgres. Fixtures were raw SQL loaded at old schema versions (133 tests), time
was moved through Postgres's clock (`pgmocktime`), and River jobs were counted
with SQL.

1. A baseline: all 149 top-level tests pass against Postgres 17.12.
2. Fixtures load at the latest schema. 36 were translated so each produces the
   data its old version had after migrating, checked by diffing both
   databases. Tests of what migrations do to existing data (`TestMigrations`,
   `TestInProgress`, `TestStatusInProgress`, `TestNCDedup`) keep their pinned
   version through `harness.NewMigrationHarness` until Postgres is removed.
3. The harness reaches the database only through a `storage` interface
   (`test/smoke/harness/storage.go`).

Exit, met: the same 149 tests pass on the Smoke workflow, with no assertion
changed.

## Phase 1: In-memory core (done)

`filestore/` holds typed tables, secondary and unique indexes, copy-on-write
views (F10), one writer, transaction timestamps from a `Clock` (F13), and
deterministic apply (F25). Its local log and snapshot files are superseded by
Phase 1b. The in-memory part becomes each pod's copy of the state.

Exit, met: crash, process-kill, replay and locking tests pass on Linux,
Windows and macOS (Filestore workflow).

## Phase 1b: Delta log on object storage

- The object-store interface and its four implementations, with one
  conformance suite that runs against all of them (F14).
- A Delta writer and reader: the `protocol`, `metaData`, `add`, `remove`,
  `txn` and `commitInfo` actions, Parquet data files, checkpoints and
  `_last_checkpoint`.
- Optimistic, grouped commits. Unclear results are resolved by reading the
  version back (F32).
- Following the log, catching up, and refusing commits from unknown writers
  or format versions (F11, F30).
- A startup self-test of permissions (F33), and checks of bucket settings
  (F34).

Exit:

- The conformance suite passes on the in-memory fake, the local directory,
  fake-gcs-server and Azurite, in CI.
- Tests cover crashes at every step of a commit, commit conflicts and unclear
  commit results. Nothing acknowledged is lost and nothing is applied twice
  (F1, F4, F32).
- Several in-process pods writing one table converge on the same state (F25).
- delta-rs reads the table and its rows match GoAlert's state (F35).

## Phase 2: Replace the transaction seam (still on Postgres)

122 functions take a `*sql.Tx`, and 37 take a generic query interface, so that
several stores can write in one transaction. They switch to `txn.Tx`.

- `*sql.Tx` already has `Commit` and `Rollback`, so it satisfies `txn.Tx`.
  Store methods take a `txn.Tx` and unwrap it in the Postgres implementation,
  so callers keep compiling and the change lands one package at a time.
- Methods that take a `gadb.DBTX` take a `txn.Tx` instead, where nil means
  "not in a transaction".
- Callers then start transactions through `txn.DB`. `withContextTx` in
  `graphql2/graphqlapp` keeps its nesting behavior.
- A transaction function can run more than once on the new backend (F3, F4),
  and a long write transaction delays every other writer (F15). Phase 2 lists
  every transaction that does I/O or runs long, and those are fixed before
  Phase 4.

Exit:

- Only store implementations import `database/sql` or `gadb`.
- The suite is green on Postgres.

## Phase 3: The stores on the Delta backend, leaf packages first

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
  new backend's normal write paths, so the fixtures also exercise the ported
  trigger logic. Fixtures use a small subset of SQL: `INSERT ... VALUES`,
  `UPDATE ... SET ... WHERE column = value`, `DELETE`, `now()` plus or minus
  an interval, and casts. Values such as dedup keys come from fixture
  template functions, never from SQL functions. The loader rejects anything
  else.
- Search becomes in-memory matching. Postgres full-text search (`textSearch`)
  has no exact equivalent, so the difference is documented (F24).
- Row normalization gets fast. Every `Put` keeps the row exactly as it will
  decode (F25), today through a one-row Parquet round trip that costs about
  0.5 ms and 250 KB (`BenchmarkNormalize`). A reflection-based normalizer
  replaces it, and randomized tests check that it always gives what the
  Parquet round trip gives (F15, F17). Row types use pointers for times that
  can be unset, since parquet-go misreads times outside 1677-2262 (F25).

Exit for each package: the inventory items for its tables are implemented and
tested, and every smoke test that needs only the packages ported so far passes
on the new backend (F5, F7, F8, F23).

## Phase 4: Engine and jobs on the Delta backend

- Each engine module's SQL becomes Go over the in-memory indexes. A cycle-time
  budget and a large-dataset benchmark guard against slowdowns (F15, F17).
- The engine lease replaces the processing locks and advisory locks (F28).
- Outgoing messages are claimed by a commit before they are sent, and keep
  today's status semantics otherwise (F3).
- River (14 job kinds, 2 clients) is replaced by a small job queue whose jobs are
  rows in the same table, so a job commits together with the change that
  created it.

Exit: all smoke tests pass on the new backend.

## Phase 5: Several pods

- The smoke suite also runs with three GoAlert pods on one table behind a
  load balancer, with session affinity.
- Read-your-writes across pods (F29). Lease handover when the holder dies or
  is cut off, with no duplicate pages (F28). Rolling upgrades with a format
  change (F30). Commit-conflict rates under load (F40).
- Clock skew detection (F13), and the cost and operation-count metrics (F37).

Exit: the suite passes with three pods, including tests that kill the lease
holder while it is sending.

## Phase 6: Remove Postgres

- Delete `migrate/`, `gadb/`, `sqlc.yaml`, `swo/`, River and its UI, and the
  Postgres devtools.
- Replace `--db-url` and related flags with the table location and
  credentials settings, and add the explicit command that creates a table
  (F27).
- List every removed or changed feature in the release notes (F24), and
  document the object-store dependency and required bucket settings (F31,
  F34).

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
- 2026-11-21: Phase 0 done. The harness reaches the database only through
  a `storage` interface (`test/smoke/harness/storage.go`), whose Postgres
  implementation does what the harness did before.
  `overrideconflict_test.go` still opens transactions itself on purpose;
  it moves with the transaction seam in Phase 2. Fixing a CI hang also
  fixed a pre-existing deadlock: River's smoke-test clock shared the app's
  5-connection pool.
- 2026-11-21: Phase 2 started: the store packages take `txn.Tx`.
- 2026-11-21: Direction changed: Delta Lake tables on GCS and Azure, shared
  by several pods, instead of a local log. The failure modes and this plan
  were rewritten for it, adding Phase 1b and Phase 5.
- 2026-11-21: Phase 1b: `objstore/` and its in-memory, local-directory, GCS
  and Azure implementations pass one conformance suite, including the
  create-if-absent race, against fake-gcs-server and Azurite in CI (Delta
  backend workflow).
- 2026-11-21: Phase 1b: `deltalog/` writes and replays the Delta log. It
  settles an unclear commit by retrying the same commit and reading the
  version back, and refuses commits from other writers or tables and newer
  protocols or formats.
- 2026-11-21: Phase 1b: `deltatable/` stores each GoAlert table as a struct
  column in Parquet data files, partitioned by table and bucket, and
  `deltalog/` writes checkpoints. In CI, GoAlert writes and checkpoints a
  table, deletes the commits the checkpoint covers, and delta-rs 1.6.6 reads
  every row as GoAlert wrote it (F35).
- 2026-11-21: Smoke flake: `TestGenericAPIClose` failed once on a duplicate
  SMS, on a commit that changed no app code, and passed when the same commit
  ran again. F3 now covers a retry while the first send may have gone out.
- 2026-11-21: Phase 1b: `filestore.DeltaDB` keeps the in-memory tables in a
  Delta table that several pods share. Its tests cover pods racing on one
  counter without losing an update and ending with the same state (F4, F25),
  a unique key won by exactly one pod (F8), unclear commits (F32), failed
  uploads (F6), foreign commits (F11), schema and bucket changes (F19, F30),
  clock skew between pods (F13), following and waiting for a version (F29),
  and starting from a checkpoint (F16, F38). Testing it found that parquet-go
  misreads times outside 1677-2262; such times are now refused (F25).
- 2026-11-21: Phase 1b: a test stops a pod at every storage write of a run
  of transactions, with the last write landing or not. Another pod always
  finds the transactions in order, every acknowledged one included (F1) and
  none applied twice (F4). It found that a commit whose first attempt was
  unclear, and whose retry was rejected, was reported as failed even though
  it might be in the log; it now stays unknown until read back (F32).
- 2026-11-21: Phase 1b: delta-rs also reads a table written by the store,
  with bucket rewrites, an emptied bucket, tombstones, the sequences table and
  a checkpoint, and sees every row as GoAlert wrote it (F35). For F33, every
  store runs a self-test in a scratch prefix when a table is created or
  opened: create-if-absent must be enforced, and read, list, replace and
  delete must work. GCS and Azure report refused credentials (401, 403) as
  `objstore.ErrDenied`, and the store's health stays red while storage
  refuses them.
- 2026-11-21: Phase 1b: cleanup (F16). `CleanLog` deletes commits and
  checkpoints before the newest usable checkpoint older than the log
  retention, and `Vacuum` deletes data files removed, or never committed,
  longer ago than the tombstone retention; a data file's name carries its
  upload time for that. A pod that has not confirmed it is current within
  half the shorter retention reloads from a checkpoint first. Without that, a
  test shows a paused pod taking a cleaned-up commit for the end of the log
  and committing that version a second time.
- 2026-11-21: Phase 1b: grouped commits (F15). Transactions that wait for
  each other in one pod share a Delta commit; each runs on top of the ones
  before it, and one that fails is left out without affecting the rest. A
  caller that gives up while its transaction is being committed gets an
  unknown outcome, never a failure (F32). `Batches` counts commits and the
  transactions they held.
- 2026-11-21: Phase 1b: storage settings (F34). GCS and Azure stores report
  settings that break the table (a Delete lifecycle rule that can reach live
  objects, retention policies, holds, public containers) or weaken it
  (versioning, soft delete, public access prevention not enforced), and the
  required settings are documented above.

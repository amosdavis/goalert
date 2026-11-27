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

- Partition columns are `tbl` (the GoAlert table) and `bucket` (a hash of the
  row's key, modulo a per-table bucket count).
- Each row has a `key` and one struct column per GoAlert table, and only that
  table's column is set. Spark reads a table as
  `SELECT alerts.* FROM goalert WHERE tbl = 'alerts'`, and GoAlert ships those
  view definitions.
- Only Delta protocol reader version 1 and writer version 2 features are used,
  so any Delta reader can open the table (F19, F35).

**Commits.** A GoAlert transaction becomes one Delta commit:

- For each bucket it changes, a new Parquet file holds the rows it added or
  changed, with what else was in the files that held them, which are removed.
- Files no larger than the new one are merged into it, up to a limit, so a bucket
  has few files and adding a row to a large table costs no more than to a small
  one (F39).
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
`_last_checkpoint` only, and `Delete`) has GCS (create with
`ifGenerationMatch=0`), Azure Blob Storage (create with `If-None-Match: *`),
local-directory, and in-memory implementations. `objstore/chunkstore` provides
the same whole-object guarantees on size-limited document databases, including
Cosmos DB and Firestore, with atomic manifests and immutable chunks.

**Other storage.** Two ports separate the store from where it keeps its data:
`objstore.Store` for objects and `tablelog.Log` for what decides commits. Cosmos
DB and Firestore implement the first, through `objstore/chunkstore`. BigQuery and
an Iceberg catalog decide commits (`bigquerylog`, `iceberglog`, on
`managedlog`), and whatever decides them, the result is the same Delta table. All
of it is optional and picked by URL at deployment (`storagebackend`).
[storage-backends.md](storage-backends.md) describes them, and how to add
another.

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

## Phase 1b: Delta log on object storage (done)

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

## Phase 2: Replace the transaction seam (still on Postgres) (done)

122 functions take a `*sql.Tx`, and 37 take a generic query interface, so that
several stores can write in one transaction. They switch to `txn.Tx`.

- `*sql.Tx` already has `Commit` and `Rollback`, so it satisfies `txn.Tx`.
  Store methods take a `txn.Tx` and unwrap it in the Postgres implementation,
  so callers keep compiling and the change lands one package at a time.
- Methods that take a `gadb.DBTX` take a `txn.Tx` instead, where nil means
  "not in a transaction".
- Callers then start transactions through `txn.DB.Run`, which takes the
  transaction as a function. On the Delta backend, a transaction that loses
  to another pod's commit runs again on the new state (F4), which a caller
  holding a `BeginTx` handle could not do; it could only fail the request.
  `withContextTx` in `graphql2/graphqlapp` keeps its nesting behavior.
  `BeginTx` stays only for Postgres-only code (switchover, the dump tool),
  which goes with Postgres (F24).
- A transaction function can run more than once on the new backend (F3, F4),
  and a long write transaction delays every other writer (F15). Phase 2 lists
  every transaction that does I/O or runs long, and those are fixed before
  Phase 4.

  Transactions that do I/O, found in Phase 2:

  - `notificationchannel.Store.MapDestToID` validates a destination and reads
    its display name through the destination registry, which for Slack
    calls the Slack API. It runs inside the transactions of the schedule
    on-call notification rules mutation, the escalation store's step
    actions, and the integration key store's universal key actions.
  - Creating or updating a contact method validates its destination inside
    the transaction; a Slack DM looks the user up.
  - The fix for both: resolve and validate the destination before the
    transaction starts, and pass the result in, so the transaction only
    reads and writes the store.
  - The engine sends messages outside its transactions, after marking each
    one as sending, but under its Postgres processing lock. Phase 4 replaces
    the lock with the lease and claims (F3, F28).

  Each engine module updates everything due in one transaction per cycle.
  On the Delta backend that is one commit per cycle (F15), and Phase 4
  measures its size against the cycle-time budget.

Exit:

- Only store implementations, the engine (until Phase 4) and the app's
  wiring touch the database: they alone call `gadb.New`, hold a `*sql.DB`
  or `*sql.Tx`, or run SQL. Other packages may still use the types `gadb`
  generates, such as `gadb.DestV1`, and `sql.Null*` values; those move in
  Phase 3.
- The suite is green on Postgres.

## Phase 3: The stores on the Delta backend, leaf packages first

Port the packages in this order:

1. config, keyring, auth nonces (done)
2. users, contact methods, notification rules, then auth sessions and
   subjects, whose queries join users and whose rows cascade from them
3. services, integration keys, heartbeats, labels
4. escalation policies
5. rotations, schedules, rules, overrides
6. alerts, alert log, dedup
7. notification channels, calendar subscriptions, API keys, universal keys,
   signals, favorites, limits

How a store is ported, in every package:

- The package defines its tables for the Delta backend with
  `DefineDeltaTables(s *filestore.Schema)`: row types with parquet tags (a
  time that can be unset is a pointer, F25), the primary key, and an index
  or unique index for each Postgres index and unique constraint the store
  relies on (F8). Every table is in the one schema the app builds, so a
  GoAlert transaction stays one commit (F2).
- The store keeps its type and its methods. `NewDeltaStore` builds it for
  the Delta backend, and each method runs its Delta implementation when the
  store has one, so callers don't change, and a running GoAlert uses one
  backend (F23).
- A Delta method takes the same `txn.Tx` (`txn/deltatxn`), runs in or as a
  `txn.DB.Run`, and does no I/O inside it (F3). Triggers, defaults (`now()`
  becomes `tx.Now()`), cascades and checks become code in the same
  transaction (F2, F7, F8), and the table's inventory entry says where.
- Its tests run the store against an in-memory Delta table, including two
  pods racing wherever the Postgres version relied on a lock or a unique
  index (F4, F8).

Packages are ported side by side, so each port stands on its own:

- A package's `DefineDeltaTables` defines only its own tables. Foreign keys to
  another package's tables are declared by a generic function that takes those
  tables, so ports don't import each other, and one place builds the whole
  schema and lists every foreign key. Reads that join another package's rows
  take a function, like the user store's favorite lookup.
- Methods run in `deltatxn.Read` or `deltatxn.Write`, whose `Scope` has the
  transaction, its reader and its time, and return `deltatxn.ErrNotPorted`
  until they are ported.
- Primary keys, unique indexes, foreign keys and checks carry the names of the
  Postgres constraints they stand for. `sqlutil.MapError` understands
  filestore's constraint errors, so the messages users get are the same on both
  backends (F22). A rule a trigger enforced, such as a limit, is a
  `filestore.CheckError` with the trigger's constraint name and hint.
- A missing row is `sql.ErrNoRows` wherever the Postgres store returned it.

Foreign keys (82 in the schema, 75 of them `ON DELETE CASCADE`) are declared
on the filestore tables rather than written into each store: a child row's
parent must exist when it is written, and deleting a parent cascades, sets
the reference to null, or is refused, all in the same transaction (F2, F7).
This comes before the users store, which most tables reference.

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
- A cycle's modules run in order, each as a transaction of its own (F53), and
  share one commit (`DeltaDB.UpdateEach`).
- The engine lease replaces the processing locks and advisory locks. It is a row,
  and every transaction of the engine checks in the same commit that its pod
  still holds it (F28, F55).
- Outgoing messages are claimed by a commit before they are sent, and keep
  today's status semantics otherwise (F3). A claim that expires is retried, where
  Postgres gave up on the message.
- River (14 job kinds, 2 clients) goes away without a replacement. Each job only
  said that some row needs attention, and the rows say so themselves (F52): a
  rotation whose shift has ended, a subscription whose status differs from its
  alert's, a signal without a message. The periodic cleanups run on the pod with
  the lease.

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
- 2026-11-21: Phase 1b done. Every exit criterion is met in CI: the
  conformance suite on all four object stores, a pod stopped at every
  storage write, conflicts and unclear commits with nothing acknowledged lost
  or applied twice, pods converging on one state, and delta-rs reading tables
  GoAlert wrote. Phase 2 continues with `txn.DB.Run`.
- 2026-11-21: Phase 2: the GraphQL layer runs its transactions through
  `txn.DB.Run`, and the 37 store methods that took a `gadb.DBTX` take a
  `txn.Tx`, with nil meaning "not in a transaction". Callers no longer pass
  a `*sql.DB` to them.
- 2026-11-21: Phase 2: the GraphQL layer no longer touches the database. Its
  six direct queries moved into the alert, integration key, notification and
  user stores, and its `App` holds a `txn.DB`. `keyring.ReEncryptAll` takes
  a `txn.DB`. Left for Phase 2: the auth handler's transaction, the list of
  transactions that do I/O or run long, and `overrideconflict_test.go`.
- 2026-11-21: Phase 3 started. `txn/deltatxn` puts the Delta backend behind
  the `txn` seam, and the porting pattern is above. The first port is
  `auth/nonce`: two pods racing on one nonce consume it exactly once (F4).
- 2026-11-21: Phase 3: `config` ported. Two pods updating the config at once
  lose no update (F4). `DeltaDB.Changes(table)` stands in for NOTIFY
  triggers: another pod reloads a new config version as soon as it follows
  the commit. A read outside a transaction sees the pod's current view, which
  can lag the log until the pod follows it (F29).
- 2026-11-21: Phase 2 done: its last changes passed the Smoke suite on
  Postgres, with only the stores, the engine and the app's wiring touching the
  database.
- 2026-11-21: Phase 3: `keyring` ported. Two pods setting up a keyring at once
  share its keys, a due rotation happens once, and forced rotations each
  happen (F4).
- 2026-11-21: Phase 3: foreign keys in filestore. A child row's parent must
  exist when it is written, and deleting a parent cascades, sets the
  reference to null, or is refused, in the same transaction (F2, F7). A pod
  that adds a child for a parent another pod just deleted fails its check
  once it catches up (F4).
- 2026-11-21: Phase 3: users partly ported (insert, update, role, find) and
  favorites ported. Methods not ported yet return an error on the Delta
  backend. Found an upstream bug: unsetting a favorite schedule, rotation,
  escalation policy or user deletes it for every user (AND/OR precedence in
  `UserFavUnset`).
- 2026-11-24: Phase 3: the shared layer for the ports (`deltatxn.Read` and
  `Write`, constraint errors that map to the same user messages as Postgres's,
  `DeltaDB.Now` for reads). GitHub started no workflow runs for `fc53c8e37`,
  `b3f4ef9ef` and `c1f2e631c` on 2026-11-22, so CI has not checked them on
  their own; the next run covers them.
- 2026-11-25: Rows are normalized by reflection instead of through a Parquet
  file: 1.8 µs and 448 bytes per write instead of about 500 µs and 247 KB. Shapes
  the fast way doesn't cover still go through a file. GitHub has started no
  workflow runs since 2026-11-21, so nothing after `f95b178be` has been checked
  by CI.
- 2026-11-25: Phase 3: the rest of `user` (auth subjects, search, existence
  checks, deletion, the on-call overview). Porting deletion found that the
  Postgres code could skip the participant on call when a deleted user was in a
  rotation twice; fixed (F5).
- 2026-11-25: Phase 3: limits, contact methods, notification rules and
  notification channels. Wiring still to do: the create command calls
  `limit.DeltaTables.InsertDefaults`, and the GraphQL mutations that map
  destinations or create contact methods in a transaction
  (`SetScheduleOnCallNotificationRules`, escalation policy actions, the
  integration key config, `createUserContactMethod`) call `ResolveDests` or
  `Normalize` before it. Until then those return an error on the Delta backend.
- 2026-11-25: Phase 3: labels, heartbeat monitors, services and integration
  keys. A transaction in which a constraint failed can't commit any more, and a
  panic in a transaction ends only that transaction (F8, F41). Open: a heartbeat
  is a commit of its own unless it waits behind another (F37, F40), so many
  monitors need a short group delay; and a new key or token can be refused by a
  pod that has not caught up (F29), so a miss should be retried at the latest
  version.
- 2026-11-25: Phase 3: schedules, schedule rules, the on-call tables, rotations
  and overrides. Deleting a user keeps the right participant on call (F5). Open
  for phase 4: a commit's time can equal the previous one's when another pod's
  clock was ahead (F13), so ending a shift at its exact start would break
  `schedule_on_call_users_check` until the clock passes it.
- 2026-11-25: Phase 3: escalation policies, their state, and notices. An alert
  whose service moves from a policy without steps to one with steps starts to
  escalate, which it never did on Postgres (F2). Wiring still to do: the GraphQL
  mutations that add escalation policy actions call `ResolveDests` before their
  transaction.
- 2026-11-25: Phase 3: auth links, basic auth, sessions, API keys and calendar
  subscriptions. GraphQL transactions no longer feed a result from one attempt
  into the next, and `SET NULL` handles repeated references to a deleted row.
- 2026-11-25: Phase 3: outgoing messages and their status history, verification
  codes, Slack's table and Twilio SMS callbacks. The engine's notification
  cycles and jobs remain in Phase 4.
- 2026-11-25: Phase 3: alerts, alert logs, feedback, metadata, metrics and
  status subscriptions. Every acknowledgment on a multi-ack step is logged.
- 2026-11-25: Other storage, all optional and chosen at deployment
  (storage-backends.md): Cosmos DB and Firestore through `objstore/chunkstore`,
  and an Iceberg catalog through `iceberglog`, whose tables are Delta tables as
  well. `storagebackend` opens them by URL. A commit uploads its files in
  parallel and a lost race keeps them (F15, F40), and `backendtest` runs several
  pods at once on every backend. Found and fixed: the Azure SDK's own retries
  could turn a commit that happened into a failure (F32), as could a failed look
  at a version that exists; a directory store could not replace a file in use on
  Windows. Open: none has been run against its service, the emulators in CI have
  not run yet, and no server flag selects a backend before Phase 5.
- 2026-11-25: BigQuery as a backend: it decides commits by the ID of a job, with
  the Delta table in a bucket and registered in BigQuery (F50). What it shares
  with Iceberg is now `managedlog`: staging a commit, having an arbiter decide,
  and copying it into the Delta log, with one suite for arbiters. Breaking
  managedlog and bigquerylog on purpose in 27 places made a test fail in all but
  one, where the arbiter refuses anyway. An Iceberg table refuses a data file that
  does not say how many rows it has, which Iceberg readers would skip (F47).
- 2026-11-25: `deltastores` assembles every store and foreign key, and
  `engine/enginetables` adds notification cycles, module state and a lease that
  only one pod can hold. Tests compare the assembled schema with the inventory,
  build every store, and race four pods for the lease. The full backend suites
  pass; the only remaining local test failures need Postgres or Unix-only
  devtools, and live-service verification is still open.
- 2026-11-25: Several pods on one Azure table. Against Azurite 3.37.0, the
  Azure conformance suite and `backendtest` pass, each pod opened by its
  `az://` URL with a client of its own. `backendtest.RunUnreliable` runs three
  pods with three writers each while the Azure SDK's transport refuses
  requests, breaks connections, and loses the replies to requests that were
  carried out (F32): with 10% and with 30% of requests failing, every
  acknowledged transaction is in the table, no failed one is, none is applied
  twice, and every pod ends with the same rows. Found and fixed: listings of an
  ADLS Gen2 account include its directories (F14); pods did not compare their
  clocks with Azure's (F13), and now go red, and leave the engine to another
  pod, when theirs is off. Open: a real Azure account, and ADLS Gen2 with a
  hierarchical namespace, which Azurite does not have.
- 2026-11-26: Slack and Postgres removed. Every store, the engine and the app
  are Delta-only: no `*sql.DB`, pgx, River, migrations or switchover, and the
  old database flags are gone. What went is listed in
  [filestore-failure-modes.md](filestore-failure-modes.md#removed-with-postgres);
  the types the Delta stores still took from sqlc are hand-written in
  `gadb/values.go` (F56). Cypress uses an in-memory Delta server with fixture
  and clock controls; Playwright uses a file-backed Delta table.
- 2026-11-27: Smaller surface (F57-F65). Removed the alert sources other than
  the generic API and GraphQL (Grafana, Prometheus Alertmanager, Site24x7,
  Mailgun, the SMTP server, heartbeat monitors, universal keys and signals),
  GitHub login, account linking, calendar subscriptions, the system API,
  `goalert monitor`, the `/v1/...` routes, Twilio SMS reply codes (STOP and
  START remain), carrier lookup, RCS, the admin SMS toolbox, alert metrics,
  user avatars, Google Analytics and the feedback link; listed in
  [filestore-failure-modes.md](filestore-failure-modes.md#removed-on-2026-11-27).

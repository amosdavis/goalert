# Delta Lake Backend: Ways to Fail

These are the design rules for moving GoAlert off PostgreSQL. A change that would
cause any failure below must not be made. GoAlert pages people when systems break,
so a storage bug here is worse than downtime: the alert fails **silently**.

## How deep the Postgres dependency goes (measured on `master` @ 0ea37b6f5)

| Surface | Count |
| --- | --- |
| Go files (total / touching Postgres directly) | 914 / 175 |
| Tables in `migrate/schema.sql` | 61 |
| Triggers / stored functions (business logic in the DB) | 59 / 67 |
| Foreign keys (75 with `ON DELETE CASCADE`) / `CHECK` constraints | 82 / 31 |
| sqlc named queries (`*/queries.sql`) | 200 |
| Hand-written SQL (`p.P(...)`, `Prepare`, inline) | ~180 |
| River (Postgres job queue) job kinds | 14 |
| Smoke tests (all run against Postgres) | 141 |

Postgres-only features in use: advisory locks plus `FOR UPDATE NOWAIT`/`FOR SHARE`
(coordination between engine instances), `LISTEN/NOTIFY` (config refresh, signals),
`now()` as the one shared clock, `SKIP LOCKED`, `ILIKE` search, enums, ranges,
the switchover system (`swo/`), and River.
`docs/filestore-inventory.md` lists every trigger and constraint, table by table.

## Decisions

- **Storage: real Delta Lake tables in object storage** (chosen 2026-11-21).
  Data files are Parquet, and changes are commits in `_delta_log`, following
  the Delta protocol, so Spark, Databricks and delta-rs can read GoAlert's
  data.
- **Both GCS and Azure** (Blob Storage and ADLS Gen2), behind one object-store
  interface. A local-directory implementation is used for development and
  tests.
- **One Delta table holds every GoAlert table.** Each GoAlert table is one
  struct column, and the Delta table is partitioned by GoAlert table and hash
  bucket. A GoAlert transaction is therefore one Delta commit, which is atomic.
  A Delta table per GoAlert table cannot work: Delta commits are atomic only
  within one table, so a crash between commits would leave, for example, an
  alert without escalation state (F2).
- **Multiple pods share the table.** A commit is the create-if-absent write of
  the next `_delta_log` version, which only one pod can win (optimistic
  concurrency). Every pod keeps the whole current state in memory and follows
  the log.
- **Other storage is optional, chosen at deployment** (2026-11-25). Cosmos DB
  and Firestore can hold the objects, and an Iceberg catalog can decide the
  commits. Two ports separate them from everything else, `objstore.Store` and
  `tablelog.Log`, and a deployment picks one driver for each
  ([storage-backends.md](storage-backends.md)). Every installation still has
  one serializable sequence of commits: no backend trades that for speed (F2,
  F4).
- **Every backend gives a Delta table** (2026-11-25). With GCS, Azure or a
  document database the Delta log itself decides commits. With an Iceberg
  catalog or BigQuery, that decides them, and the Delta log in the bucket gets
  a copy of each (`managedlog`). So with Iceberg, one table is both formats, and
  with BigQuery, the Delta table is in the bucket and registered in BigQuery.
- **BigQuery decides a commit by a job's ID** (2026-11-25). It "rejects any job
  creation request that reuses a previous job ID", which is the create-if-absent
  a log needs, and documented. A conditional `UPDATE` is not documented to be a
  compare-and-swap, streams of the Storage Write API allow one connection, and
  multi-statement transactions are not queued
  ([bigquery.md](bigquery.md)).
- **Fresh installs only.** There is no Postgres import, so F18 is out of scope.
- **Superseded:** the local log on disk (chosen 2026-11-20). The in-memory
  tables, indexes and transactions in `filestore/` stay; the Delta log
  replaces the local log and snapshot files.
- **Still rejected:** pods sharing a filesystem volume (NFS, SMB,
  `ReadWriteMany`). Locks and appends on them are unreliable.

## Failures

### A. Paging correctness (the worst kind)

- **F1 Lost alert.** GoAlert tells a caller (Grafana, Prometheus, email) that an
  alert was created, but the change never became durable. *Rule:* never reply
  success until the commit object exists in the object store: either its
  create-if-absent write succeeded, or a check after an unclear result proved
  it (F32).
- **F2 Alert never escalates.** The alert is saved but its escalation state is not.
  Today the trigger `fn_insert_ep_state_on_alert_insert` does both in one
  transaction. *Rule:* every trigger side effect moves into the same atomic
  commit as the change that caused it, and a GoAlert transaction is always
  exactly one Delta commit.
- **F3 Duplicate pages.** A message is sent twice: a crash between sending and
  recording it, two pods sending the same message, a transaction function
  re-run after a commit conflict, or a retry while the first attempt may
  already have gone out. Postgres GoAlert retries a temporary send failure
  after 15 seconds, and fails a send still running past its 10-second
  `sending_deadline`, so a slow provider can page twice. A smoke run on
  2026-11-21 saw one duplicate SMS (`TestGenericAPIClose`) while the harness
  moved the clock forward; a rerun of the same commit passed. *Rule:* a
  message is claimed by a commit before it is sent, and only the pod whose
  claim committed sends it. Transaction functions do no I/O, because they can
  run more than once. A claim lasts longer than the provider call's own
  timeout, so no second send starts while one is in flight. A send whose
  outcome is unknown is retried, since a duplicate page is better than a lost
  one (F1), and every retry is counted in a metric.
- **F4 Lost ack or close.** A stale read-modify-write overwrites a newer change,
  so an acked alert keeps paging. *Rule:* a commit is always made on top of the
  latest version. If another pod committed first, the transaction re-runs on
  the new state.
- **F5 Wrong person paged.** Rotation and schedule logic in triggers
  (`fn_advance_or_end_rot_on_part_del`, the position no-gap rules,
  `fn_set_rot_state_pos_*`) is ported slightly wrong, so who is on call drifts.
  The Postgres code had such a bug too: deleting a user who was in a rotation
  twice could skip the participant on call. *Rule:* the rotation, schedule and
  escalation smoke tests pass unchanged, and a port checks the existing logic
  instead of copying it.

### B. Data integrity

- **F6 Commits that point at missing data.** A commit references a data file that
  failed to upload, or cleanup deletes a file a pod still needs. *Rule:* data
  files are uploaded, and the upload confirmed, before the commit that adds
  them. Every part of a commit object is uploaded before one atomic metadata
  operation publishes it, whether that operation is an object create, a catalog
  snapshot swap, or a document-store manifest write, so commits are complete or
  absent. VACUUM deletes a file only when it is unreferenced and older than the
  retention window.
- **F7 Broken references.** For example, deleting a user leaves dangling rotation
  participants or escalation steps (75 cascades handle this today).
  *Rule:* every foreign key and cascade has an explicit Go equivalent with a test.
- **F8 Dropped constraints.** The limit triggers (`fn_enforce_*_limit`), the
  `UNIQUE` and `CHECK` constraints and the no-reopen rule (`fn_prevent_reopen`)
  stop being enforced, or code that ignores a constraint's error commits the
  row anyway, which Postgres prevents by aborting the transaction. *Rule:*
  every constraint in the inventory gets a Go check with a test, and a
  transaction in which a constraint failed can't commit.
- **F9 No usable backup.** Object storage keeps data, but a mistake (a bad
  release, a deleted prefix) has no undo. *Rule:* a documented backup and
  restore: copying the table at a version, or restoring from Delta history
  within the retention window, tested end to end.
- **F10 Readers see a half-applied change.** For example, the API shows an alert
  as closed while its escalation state is still active. *Rule:* each pod
  serves reads from an immutable view of one version.
- **F25 Replay builds a different state.** Every pod rebuilds state by reading
  the log. If applying a commit calls `time.Now()`, generates an ID, uses
  randomness or depends on map iteration order, pods diverge from each other
  and from what was acknowledged. A value that does not read back exactly as
  it was written diverges the same way: parquet-go v0.32.0 reads timestamps
  through int64 nanoseconds, so a time outside 1677-09-21 to 2262-04-11, such
  as Go's zero time, comes back wrong (while other Delta readers get it
  right, F35). *Rule:* applying a commit is deterministic. The writer stamps
  IDs and timestamps into the commit, every row is kept exactly as it
  decodes (a shortcut for that is used only where tests show it gives the same
  result, and shapes that lose data, such as lists of pointers, are refused),
  a time outside that range is refused when written (a time that
  can be unset is a pointer), and a test replays the log and compares the
  state.

### C. Concurrency and deployment

- **F11 Writers that don't follow the protocol.** A Spark or Databricks job writes
  to the table, someone runs OPTIMIZE or VACUUM with a short retention, a second
  GoAlert deployment points at the same location, or files are edited by hand.
  *Rule:* GoAlert is the only writer. Every commit names its writer and store
  ID, and a pod that sees a commit from anyone else stops writing and goes
  red. Analytics get read-only access.
- **F12 Superseded.** Several pods now provide high availability. Its new limit is
  F31.
- **F13 Clock drift between pods.** Each pod has its own clock, so commit times can
  go backwards and timers fire early or late. *Rule:* a commit's time is the
  later of the pod's clock and the previous commit's time. Pods compare their
  clock with the object store's `Date` header and go red beyond a threshold
  (`MaxClockSkew`, 15 seconds by default, half a message claim). Such a pod
  gives the engine's lease up, so that one whose clock is right runs the
  engine, and takes it only if no other pod does for two of its terms.
  What may be deleted from a document database is judged by the database's
  clock, never a pod's.
- **F14 Platform and SDK differences.** The GCS, Azure, Cosmos DB, Firestore
  and Iceberg catalog APIs report conflicts, throttling and timeouts
  differently, and the local implementations behave differently again. An Azure account with
  a hierarchical namespace (ADLS Gen2) lists directories like blobs. *Rule:*
  every implementation passes the suite of its port (`objstoretest`,
  `chunkstoretest`, `tablelogtest`), and every backend passes one that runs
  several pods at once (`backendtest`), also while requests are refused,
  connections break, and replies to requests that were carried out are lost. CI runs them on emulators (GCS, Azure,
  Firestore) and on servers that speak the real protocol to the real SDK
  (Cosmos DB, Firestore, an Iceberg REST catalog), and the real services are
  tested before release.

### D. Performance

- **F15 Paging gets slower.** Every commit costs at least two network round trips,
  and the engine runs multi-table work every cycle. *Rule:* commits are grouped,
  the engine batches its changes per cycle, every query it runs has an
  in-memory index, and a cycle-time budget is measured against a large dataset.
- **F16 Unbounded growth.** Commit files, removed data files and small files pile
  up, and startup and listing slow down. Cleaning up has its own way to fail:
  a pod that fell behind (paused, partitioned) reads a commit that cleanup
  deleted as the end of the log, and its next create-if-absent writes that
  version again, forking the log; or it reads a data file VACUUM deleted.
  *Rule:* a checkpoint every N commits, old log entries removed after the
  retention window but never at or after the newest checkpoint older than it,
  VACUUM only for files removed or never committed longer ago than the
  tombstone retention, and compaction of small files. A pod that has not
  confirmed it is current within half of the shorter retention, by wall or
  monotonic clock, reloads from a checkpoint instead of catching up commit by
  commit. The cleanup and retention managers keep working.
- **F17 Running out of memory.** Every pod holds all of the data in RAM. *Rule:*
  set and test a memory ceiling for a stated dataset size.
- **F21 Storage unavailable or refusing writes.** An outage, throttling, an
  authorization failure or a quota. *Rule:* fail loudly: the health check goes
  red, the error is logged, writes back off with jitter, and an alert is never
  dropped quietly.

### E. Migration, upgrades, security

- **F18 Existing data is stranded.** Out of scope: fresh installs only.
- **F19 Format changes break upgrades or rollback.** *Rule:* the table uses only
  Delta protocol reader version 1 and writer version 2 features. GoAlert's own
  format version is in the table's configuration, unknown versions are refused,
  and each format change has an explicit, tested migration.
- **F20 Data or secrets exposed.** Alert data holds phone numbers, emails and
  provider credentials. *Rule:* a private bucket or container, a
  least-privilege identity, no public access, encryption at rest, and GoAlert's
  keyring encryption kept for secrets. Credentials are never logged.

### F. Rewrite process

- **F22 Behavior drift.** About 380 queries, 59 triggers and 67 functions are
  rewritten by hand, and some behavior changes along the way. *Rule:* the 141
  smoke tests are the acceptance check. They run against the new backend, and
  no test is deleted or weakened to make it pass. The four that tested what
  migrations do to existing data went with the migrations (F24).
- **F23 Codebase left half-migrated.** *Rule:* put a storage interface at each
  package boundary. A running GoAlert uses one backend for everything, never a
  mix, because a single change often spans several packages. Progress is
  measured by how many smoke tests pass on the new backend.
- **F24 Features removed without saying so.** Switchover (`swo/`), the River UI
  and the pgdump/resetdb devtools only make sense with Postgres. *Rule:* list
  every feature that is removed or changed ([below](#removed-with-postgres)).

### G. Several pods on one table

- **F26 Superseded.** There is no quorum to lose. Availability now depends on the
  object store (F31).
- **F27 Two tables by mistake.** A pod pointed at an empty location creates a new
  table and runs a separate GoAlert that also pages. *Rule:* creating a table
  needs an explicit command. A pod never creates one on its own.
- **F28 Two engines at once.** A pod that lost its lease, through a pause or a
  partition, keeps running its engine. *Rule:* the lease only avoids wasted
  work. Correctness comes from claims (F3): a message is sent only by the pod
  whose claim committed, and the engine pauses (`Engine.Pause`) as soon as the
  lease is lost.
- **F29 Stale reads.** A pod's view lags the log, so a user acks an alert and the
  next page load, served by another pod, still shows it unacked. *Rule:* a
  write returns its version, and a read that carries a version waits until the
  pod has applied it. Session affinity at the load balancer is recommended.
- **F30 Mixed versions during a rolling upgrade.** A new pod commits something old
  pods can't apply, and they crash or diverge. *Rule:* commits carry a format
  version, a pod that can't apply one stops and goes red, and a new format is
  written only once every pod understands it.

### H. Object storage and Delta Lake

- **F31 The provider or network is down.** Every pod depends on the bucket, so an
  outage or partition stops new alerts and all paging, with no local fallback.
  *Rule:* this dependency is documented, health goes red at once, and
  dual-region GCS buckets or geo-redundant Azure storage are recommended.
- **F32 An unclear commit.** The create-if-absent write of a commit times out or
  returns a server error, and the commit may or may not exist. *Rule:* read
  that version back and compare its commit ID before reporting success or
  trying again. Never apply a commit twice, and never report one that doesn't
  exist. An SDK that retries on its own hides this: once any attempt may have
  been applied, a later one being refused does not make it a failure, so every
  adapter sees each attempt, or turns the retries off.
- **F33 Credentials or permissions break.** Expired keys or SAS tokens, a rotated
  service account, or a missing permission for conditional writes. *Rule:* at
  startup, a self-test reads, creates-if-absent and deletes in a scratch prefix.
  Health goes red on any authorization failure.
- **F34 Bucket settings that break the protocol.** Lifecycle rules that delete log
  files, retention locks that block VACUUM, soft delete or versioning that
  bring files back, or public access. *Rule:* check these settings at startup
  where the API allows it, and document the required ones.
- **F35 Other readers can't open the table.** A wrong protocol version, a bad
  checkpoint or wrong file statistics break Spark, Databricks or delta-rs.
  *Rule:* CI reads the table with delta-rs and compares the result with
  GoAlert's state. GoAlert can ignore an unreadable checkpoint and replay the
  JSON commits.
- **F36 Throttling and request limits.** GCS limits writes to one object name to
  about one per second, and both providers throttle busy accounts. *Rule:*
  every commit and data file gets a new object name. Only `_last_checkpoint`
  is rewritten, and at most once per checkpoint. Retries back off with
  jitter.
- **F37 Cost runaway.** Many small writes, frequent polling, kept history, or
  egress between regions. *Rule:* pods run in the bucket's region, polling
  backs off when the table is idle, retention is set, and operation counts are
  exported as metrics.
- **F38 Slow pod start.** A new pod reads the checkpoint and every active data
  file before it can serve. *Rule:* readiness waits until the pod has caught
  up, the checkpoint interval bounds replay, and files are fetched in parallel.
- **F39 Write amplification.** A commit rewrites rows it does not change, and
  the more rows a table has, the slower every commit gets. *Rule:* a commit writes
  the rows it adds or changes and the rest of the files that held them, and a file
  has a limited number of rows. Files no larger than a commit's are merged into
  it, so a bucket has few, and a row is rewritten only as often as their sizes
  double. Tests and a benchmark check that adding a row costs the same however
  many there are.
- **F40 Commit storms.** Pods that keep conflicting retry each other into a
  standstill. *Rule:* only the lease holder runs the engine. Other writes
  retry with backoff and a cap, and conflicts are counted in metrics. A retry
  uploads only the files that the other pod's commit changed for it.
- **F41 A panic takes the pod down.** Transactions run on a goroutine of their
  own, where the HTTP server's and GraphQL's handling of panics doesn't reach:
  one panic would end every request and the engine. *Rule:* a panic in a
  transaction undoes that transaction only, and goes on in its caller. Tests
  cover that the rest of its batch still commits.

### I. Optional storage backends

- **F42 A collector deletes live chunks.** A document database's records are
  small, so a large object is split into chunks, and chunks that failed uploads
  left behind have to be deleted. A collector that trusts an incomplete
  listing, a pod's clock, or a chunk another upload shares, deletes data in
  use; so does one that runs while a paused pod is about to publish chunks it
  uploaded hours ago. *Rule:* a chunk belongs to one upload of one object and is
  never reused. It is deleted only if it is older than the grace period (an hour
  or more) by the database's clock, and a strongly consistent read of its
  object's manifest shows that it is unused. A read that fails deletes nothing.
  An upload that took more than half the grace period starts over, and one whose
  publication did stores its chunks again. Objects are checked against their
  digest when they are read.
- **F43 Two things decide commits.** A pod is configured to use the Delta log of
  a table whose commits an Iceberg catalog or BigQuery decides, or the reverse,
  or another project's BigQuery, and both win the same version. *Rule:* the
  table's metadata names what decides its commits, and any other log refuses
  it. A commit is in the Delta log only if it was decided, and the next one is
  asked for only once the one before is in the Delta log, so whichever pod comes
  next can finish what a crashed one left. Every commit names the one it is on
  top of, and a Delta version that was not decided stops writes.
- **F44 Stale reads of the latest version.** With weaker than strong
  consistency, a database answers with old data: a pod takes an old version for
  the newest, misses its own commit when it looks for it (F32), or reads a
  deleted commit as the end of the log (F16). *Rule:* a backend needs strongly
  consistent reads. Cosmos DB is asked for Strong with every read, which an
  account set to less refuses, and is checked at startup. "Not found" counts
  only if it is the record that is missing, not the database or the container.
- **F45 A database's limits.** A Cosmos DB logical partition holds 20 GB and an
  item 2 MB; a Firestore document holds 1 MiB, and its index 1500 bytes of a
  name. A table that outgrows one stops taking writes. *Rule:* data is spread
  over many partitions, no record comes near its limit, and what can't be stored
  is refused before it is sent.
- **F46 Catalog metadata grows.** An Iceberg catalog rewrites all of a table's
  metadata for every commit, so anything kept per commit makes each one slower,
  and the total grows with the square of their number. *Rule:* a snapshot holds
  the commit's identity and nothing else, the catalog keeps a fixed number of
  snapshots, manifests are a fixed number of shards, and a commit writes only
  those it changes. A test counts them.
- **F47 Iceberg readers see other rows.** The snapshot's manifests and GoAlert's
  state differ, and a report is wrong without anyone noticing. *Rule:* opening a
  table compares the two file by file, a commit that removes a file Iceberg does
  not have fails, and tests compare the rows an Iceberg reader finds with
  GoAlert's.
- **F48 A backend nobody chose gets in the way.** GoAlert asks for credentials of
  a cloud that is not used, contacts it, fails to start without it, or falls
  back to it. *Rule:* every backend is optional. Only the configured one is
  opened, there is no default location and no fallback (F27), and build tags
  leave out any driver with its SDK. CI builds without each.
- **F49 The catalog and the object store are different places.** Iceberg's
  metadata names files where GoAlert does not write them, so its readers find
  nothing, or another installation's data. *Rule:* a table's location is
  checked against the object store's whenever it is created or opened, and
  storage that Iceberg readers can't reach is refused.
- **F50 BigQuery's decisions are lost or someone else's.** BigQuery keeps a job
  for six months, in one project and location. Someone deletes a job's
  metadata, a pod asks in another location, or another program's job has the
  ID; a quota refuses a job, which looks like a lost permission. *Rule:* the
  table's metadata names the project and the location (F43). A job's labels name
  its table, version and commit, and a job with an ID of ours that has others
  stops the pod. Only the newest decision depends on its job, since every older
  one is in the Delta log, and a table nobody used for months is opened from the
  Delta log. At startup a pod checks that a used ID is refused (F33). A refusal
  by quota is told from one of permission, and tried again. Nobody may be
  allowed to delete job metadata.

### J. Assembly and the engine

- **F51 The assembly leaves something out.** A store that is built without a
  limit, a lookup or a foreign key works, and silently enforces less (F7, F8),
  and a table missing from the schema can't be added later without a migration.
  *Rule:* one package defines the whole schema and builds every store. Tests
  compare its tables and foreign keys with the Postgres schema's, and fail if
  any lookup is missing.
- **F52 Engine work depends on a queue.** A job that is lost, stuck, duplicated
  or piled up leaves a rotation that never advances or a status update that is
  never sent, while the data says otherwise. *Rule:* there is no job queue. What
  has to be done is found in the data itself every cycle, through indexes (F15),
  so a crash, a lost lease or a skipped cycle loses nothing. The plan's queue was
  dropped for this.
- **F53 One bad row stops all paging.** A schedule with an unknown time zone, a
  rotation in an impossible state or a panic in one module ends the cycle, every
  cycle. *Rule:* each module's work is a transaction of its own, so one that
  fails leaves the others' alone. A module skips a row it can't handle and says
  so, and messages are sent whatever the other modules did.
- **F54 A late result overwrites a newer claim.** A pod that paused reports how
  a send went after its claim expired and another pod sent the message again.
  *Rule:* a result counts only for the claim it belongs to. A claim that expired
  is retried like any send whose outcome is unknown (F3), up to the same limit.
- **F55 The engine stops and nobody notices.** Every pod thinks another one holds
  the lease, or the holder's cycles fail. *Rule:* a lease is taken over once it
  has expired, by any pod that runs an engine, and given back on a clean stop. A
  pod that can't hold or see a current lease, or whose cycles keep failing, says
  so in its health and metrics.

### K. Removing Postgres

- **F56 Removing Postgres removes what the Delta backend needs.** Code generated
  from Postgres's schema and queries, and packages named for SQL, carry types
  and behavior that the Delta path uses: the parameters and rows of alert
  statistics, pending messages and signals (`gadb`), and turning a broken
  constraint into a message for the user (`sqlutil.MapError`, which the
  filestore's constraint errors speak to, F8). Deleting their source breaks the
  next regeneration, or quietly gives users raw errors. *Rule:* before generated
  or Postgres-only code is deleted, what the Delta path uses moves into
  hand-written code; only what nothing uses is deleted, and every package is
  built and vetted after. A setting only Postgres had is removed with its flag
  and environment binding, never silently forwarded to another backend (F48).

### L. Removing features (2026-11-27)

GoAlert is made smaller: alert sources, logins, integrations and extras the
owner does not use are removed ([below](#removed-on-2026-11-27)). Removing code
can fail in ways of its own:

- **F57 A removed alert source still answers success.** A monitor keeps posting
  to a removed endpoint (Grafana, Prometheus Alertmanager, Site24x7, email,
  heartbeats, `/v1/api/alerts`), and the alert is lost if anything replies 2xx
  (F1). *Rule:* a removed endpoint leaves the router, so its callers get an
  error their own retries and monitoring can see. Nothing that accepted alerts
  is replaced by a handler that discards them, and an integration key of a
  removed type is never treated as a generic one.
- **F58 A stored setting from a removed feature stops startup.** A saved config
  names `GitHub.Enable` or `Twilio.SMSCarrierLookup`, or a system limit row
  names `heartbeat_monitors_per_service`. *Rule:* a stored config still loads
  when it names removed settings, and they are dropped the next time it is
  saved. A setting that is still read is never removed from the config.
- **F59 The UI or an API client asks for a removed field.** GraphQL refuses the
  whole query when one field in it is unknown, so one leftover field blanks a
  page. *Rule:* the GraphQL schema, its generated Go and the TypeScript types
  are regenerated, the web app type-checks, nothing in `web/src` names a
  removed type or operation, and the Cypress and Playwright suites pass.
- **F60 Half a feature is left behind.** Its table, store, engine module,
  cleanup task, config setting, limit, experimental flag, route or document
  stays, still costing a read at startup, a commit, or a setting that does
  nothing. *Rule:* a removed feature goes entirely. A search for its names
  finds only the record of its removal.
- **F61 Removing a feature removes what a kept one needs.** Email
  notifications share code with email alerts, SMS notifications share their
  inbound handler with reply codes, and the cleanup manager also expires
  sessions. *Rule:* before shared code is deleted, what the kept features use
  of it is found and kept. A smoke test is deleted only when everything it
  checks was removed. Tests of kept behavior are never deleted or weakened
  (F22).
- **F62 A message promises what no longer exists.** An SMS still says "reply
  1a to acknowledge", or a reply to one is silently ignored, so the person
  thinks the alert is handled while it keeps escalating. *Rule:* alert
  messages offer no reply codes. STOP and START are still handled, since
  carriers require them. Any other reply is answered, within the existing
  reply limit, with a short note that replies are not supported.
- **F63 A safety net goes with a feature.** Notices warn that a service or
  escalation policy would page nobody; heartbeat monitors page when a source
  goes quiet. *Rule:* only what the owner chose to remove goes, and anything
  that warns about paging stays (notices stay). Removing heartbeat monitors is
  listed below, so whoever relied on them watches their sources another way.
- **F64 A removed login locks people out.** Someone who only ever signed in
  with GitHub, or through a linked account, can't sign in. *Rule:* username
  and password, OIDC and API keys stay. The removal is listed below, and
  `goalert add-user --user-id` gives an existing user a password.
- **F65 A removed experimental flag stops startup.** A deployment still passes
  `--experimental=univ-keys` with `GOALERT_STRICT_EXPERIMENTAL` set. *Rule:*
  a removed flag is refused like any unknown flag, and the message names it.

## Removed with Postgres

On 2026-11-26 GoAlert stopped supporting Postgres: its data is only ever in a
Delta table (fresh installs, F18). What went with it (F24):

- **Postgres.** `--db-url`, `--db-url-next`, `GOALERT_DB_URL`, `--db-max-open`
  and `--db-max-idle` are gone. `--storage-objects` is required.
- **Migrations.** `goalert migrate`, `goalert export-migrations`,
  `goalert-migrate` and `migrate/` with `schema.sql`. The schema the parity
  test checks against is recorded in [filestore-inventory.md](filestore-inventory.md)
  (F51). `goalert version` no longer prints a migration.
- **Switchover.** `swo/`, the GraphQL `swoStatus` query and `swoAction`
  mutation, the Admin → Switchover page and its guide, `docs/switchover.md`,
  `Procfile.swo` and `make start-swo`.
- **River.** The job queue, the River UI at `/admin/riverui` (and its CSP
  hashes, and the dev-only Job Queues link). The engine finds its work in the
  data (F52).
- **`LISTEN/NOTIFY`.** A config change reached other instances through it;
  pods follow the Delta log instead.
- **`--region-name`** is ignored, with a warning: one pod at a time runs the
  engine and sends messages, whatever its region (F28).
- **Postgres's error codes.** Retrying transaction-rollback, connection and lock
  timeout errors (SQLSTATE classes 40 and 08, `55P03`), treating a canceled
  query (`57014`) as a client disconnect, and the `SQLErrDetails` log field.
  Constraint errors keep their Postgres names and messages (F8).
- **Devtools.** `pgdump-lite`, `psql-lite`, `pgmocktime`, `resetdb` (its random
  demo data has no Delta equivalent yet), `ordermigrations`, `fetchcounts`,
  `db-url-set-db`, `procwrap`, sqlc with `sqlc.yaml`, River's CLI, `speedbump`, the
  `postgres://` scheme of `waitfor`, the Makefile targets `postgres`,
  `postgres-reset`, `regendb`, `resetdb`, `db-schema`, `new-migration`, `sqlc`,
  `config.json.bak` and `test-migrations`, the migrations workflow and the
  schema checks of `codecheck.sh`. `make start` keeps its data in
  `bin/dev-storage` (`make dev-storage`).
- **The demo image** has no Postgres: it starts with an empty table and the
  logins admin and user, without random data.
- **Smoke tests of what migrations do to existing data**: `TestMigrations`,
  `TestInProgress`, `TestStatusInProgress` and `TestNCDedup`, with
  `test/smoke/migratetest`, as the plan said (filestore-plan.md, phase 0). The
  harness has no `SMOKE_STORAGE=postgres`, `NewMigrationHarness`, `DBURL` or job
  helpers (`WaitForJobs`, `CountJobs`, `EnqueueJob`).
- **Go APIs.** `app.NewApp`, `App.DB`, `engine.NewEngine`, the stores' Postgres
  constructors (`NewStore`, `NewDB`), `txn.DB.BeginTx`, `txn/sqltxn`,
  `util/sqldrv`, `util.Prepare`, `engine/processinglock`, the SQL parts of
  `util/sqlutil`, `search`'s query templates, `dataloader.NewStoreLoaderWithDB`,
  `timeutil.ISODuration.PGXInterval` and the SQL `Scan`/`Value` of
  `timeutil.WeekdayFilter`. The types the Delta stores still use are in
  `gadb/values.go` (F56).

**Browser harnesses:** Cypress uses `devtools/cyserver`, an in-memory Delta
installation with fixture and fake-clock controls, and retains coverage through
`bin/cyserver.cover`. Playwright uses the file-backed table in
`bin/integration-storage`; `make reset-integration` creates it with the test
logins. The switchover browser test and its extra server are gone.

## Removed on 2026-11-27

To make GoAlert smaller, these features are removed (F24, F57-F65):

- **Alert sources.** Grafana, Prometheus Alertmanager and Site24x7 keys, email
  alerts (the Mailgun webhook, the built-in SMTP server and its
  `--smtp-listen*`, `--email-integration-domain` and
  `--smtp-additional-domains` flags), heartbeat monitors, and universal keys
  with their rules, expressions, signals and the `univ-keys` experimental
  flag. The generic API (`/api/v2/generic/incoming`) and alerts made in the UI
  or GraphQL remain.
- **Logins.** GitHub, account linking, and calendar subscriptions with their
  tokens and feeds. Username and password, OIDC and GraphQL API keys remain.
- **The system API.** The gRPC server (`--listen-sysapi` and its certificate
  flags) and `goalert gen-cert`.
- **`goalert monitor`** (`remotemonitor`).
- **The `/v1/...` routes** that duplicated `/api/v2`.
- **Twilio extras.** SMS reply codes (acknowledging or closing by text), carrier
  lookup, per-carrier from numbers, RCS, the voice name and language settings,
  and the admin tools that send a test SMS or look up a number. STOP and START
  still work (F62).
- **Alert metrics.** The service metrics and alert count pages and the engine
  module that computed their data.
- **User avatars**, **Google Analytics** and **the Feedback link**.

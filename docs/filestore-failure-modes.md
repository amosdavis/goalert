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
- **With an Iceberg catalog the table is Delta as well.** The catalog's atomic
  swap decides a commit, and the Delta log gets a copy of it afterwards, so
  readers of both formats see one table over the same files.
- **BigQuery only reads.** It allows one mutating statement at a time on a
  managed Iceberg table and has no low-latency conditional writes, so it can't
  decide commits ([bigquery.md](bigquery.md)).
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
  clock with the object store's `Date` header and go red beyond a threshold.
  What may be deleted from a document database is judged by the database's
  clock, never a pod's.
- **F14 Platform and SDK differences.** The GCS, Azure, Cosmos DB, Firestore
  and Iceberg catalog APIs report conflicts, throttling and timeouts
  differently, and the local implementations behave differently again. *Rule:*
  every implementation passes the suite of its port (`objstoretest`,
  `chunkstoretest`, `tablelogtest`), and every backend passes one that runs
  several pods at once (`backendtest`). CI runs them on emulators (GCS, Azure,
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
  no test is deleted or weakened to make it pass.
- **F23 Codebase left half-migrated.** *Rule:* put a storage interface at each
  package boundary. A running GoAlert uses one backend for everything, never a
  mix, because a single change often spans several packages. Progress is
  measured by how many smoke tests pass on the new backend.
- **F24 Features removed without saying so.** Switchover (`swo/`), the River UI
  and the pgdump/resetdb devtools only make sense with Postgres. *Rule:* list
  every feature that is removed or changed.

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
- **F39 Write amplification.** Updating one row rewrites its whole bucket file.
  *Rule:* the bucket count per table keeps files small. Append-only tables
  (alert logs, message history) only append, and are compacted later. Both
  are benchmarked.
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
  a table whose commits an Iceberg catalog decides, or the reverse, and both
  win the same version. *Rule:* the table's metadata names what decides its
  commits, and any other log refuses it. A commit is in the Delta log only if
  the catalog has it, and the next commit is made only once the one before is in
  the Delta log, so whichever pod comes next can finish what a crashed one left.
  A Delta version the catalog does not have stops writes.
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

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
  *Rule:* the rotation, schedule and escalation smoke tests pass unchanged.

### B. Data integrity

- **F6 Commits that point at missing data.** A commit references a data file that
  failed to upload, or cleanup deletes a file a pod still needs. *Rule:* data
  files are uploaded, and the upload confirmed, before the commit that adds
  them. Commit files are written with a single request, so they are complete
  or absent. VACUUM deletes a file only when it is unreferenced and older than
  the retention window.
- **F7 Broken references.** For example, deleting a user leaves dangling rotation
  participants or escalation steps (75 cascades handle this today).
  *Rule:* every foreign key and cascade has an explicit Go equivalent with a test.
- **F8 Dropped constraints.** The limit triggers (`fn_enforce_*_limit`), the
  `UNIQUE` and `CHECK` constraints and the no-reopen rule (`fn_prevent_reopen`)
  stop being enforced. *Rule:* every constraint in the inventory gets a Go
  check with a test.
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
  decodes, a time outside that range is refused when written (a time that
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
- **F14 Platform and SDK differences.** The GCS and Azure SDKs report conflicts,
  throttling and timeouts differently, and the local implementation behaves
  differently again. *Rule:* one conformance test suite runs against every
  object-store implementation: the emulators (fake-gcs-server, Azurite) in CI,
  and the real services before release.

### D. Performance

- **F15 Paging gets slower.** Every commit costs at least two network round trips,
  and the engine runs multi-table work every cycle. *Rule:* commits are grouped,
  the engine batches its changes per cycle, every query it runs has an
  in-memory index, and a cycle-time budget is measured against a large dataset.
- **F16 Unbounded growth.** Commit files, removed data files and small files pile
  up, and startup and listing slow down. *Rule:* a checkpoint every N commits,
  old log entries removed after the retention window, VACUUM for removed data
  files, and compaction of small files. The cleanup and retention managers
  keep working.
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
  exist.
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
  retry with backoff and a cap, and conflicts are counted in metrics.

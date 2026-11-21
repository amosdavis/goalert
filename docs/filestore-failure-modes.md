# Filestore Backend: Ways to Fail

These are the design rules for moving GoAlert off PostgreSQL. A change that would
cause any failure below must not be made. GoAlert pages people when systems break,
so a storage bug here is worse than downtime: the alert fails **silently**.

## How deep the Postgres dependency goes (measured on `master` @ 0ea37b6f5)

| Surface | Count |
| --- | --- |
| Go files (total / touching Postgres directly) | 914 / 175 |
| Tables in `migrate/schema.sql` | 61 |
| Triggers / stored functions (business logic in the DB) | 42 / 67 |
| `ON DELETE CASCADE` / `CHECK` constraints | 75 / 31 |
| sqlc named queries (`*/queries.sql`) | 200 |
| Hand-written SQL (`p.P(...)`, `Prepare`, inline) | ~180 |
| River (Postgres job queue) job kinds | 14 |
| Smoke tests (all run against Postgres) | 141 |

Postgres-only features in use: advisory locks plus `FOR UPDATE NOWAIT`/`FOR SHARE`
(coordination between engine instances), `LISTEN/NOTIFY` (config refresh, signals),
`now()` as the one shared clock, `SKIP LOCKED`, `ILIKE` search, enums, ranges,
the switchover system (`swo/`), and River.

## Decisions so far

- **Storage engine: in-memory store with an append-only log on disk** (chosen
  2026-11-20). All state lives in memory. Every change is a record appended to
  the log and fsynced before it is acknowledged. Startup loads the newest
  snapshot and replays the log after it.
- **Fresh installs only.** There is no Postgres import, so F18 is out of scope.
- **Pods sharing one filesystem store: rejected.** Several pods writing one log
  over a shared volume (NFS, SMB, any `ReadWriteMany` volume) causes F1, F3,
  F4, F6 and F11.
- **Instance count: open.** Either one active pod (F12 accepted), or three or
  more pods, each with its own disk, replicating the log (section G).

## Failures

### A. Paging correctness (the worst kind)

- **F1 Lost alert.** GoAlert tells a caller (Grafana, Prometheus, email) that an
  alert was created, then crashes before the alert is durably on disk.
  *Rule:* never reply success until the write is fsynced, including the parent
  directory when a file is created.
- **F2 Alert never escalates.** The alert is saved but its escalation state is not.
  Today the trigger `fn_insert_ep_state_on_alert_insert` does both in one
  transaction. *Rule:* every trigger side effect moves into the same atomic write
  as the change that caused it. Each of the 42 triggers is listed and mapped.
- **F3 Duplicate pages.** A message is sent, then GoAlert crashes before saving that
  it was sent, or two writers send the same message. *Rule:* keep the
  `outgoing_messages` status transitions exactly as they are today, with a single
  writer.
- **F4 Lost ack or close.** Two concurrent read-modify-write cycles mean the older
  state overwrites the newer one, so an acked alert keeps paging. *Rule:* all
  writes are serialized and version-checked. Never read a file, change it and
  write it back without a lock.
- **F5 Wrong person paged.** Rotation and schedule logic in triggers
  (`fn_advance_or_end_rot_on_part_del`, the position no-gap rules,
  `fn_set_rot_state_pos_*`) is ported slightly wrong, so who is on call drifts.
  *Rule:* the rotation, schedule and escalation smoke tests pass unchanged.

### B. Data integrity

- **F6 Torn or partial files** after a crash or power loss. *Rule:* write a temp
  file, fsync it, then rename it atomically. Check a checksum or framing on load
  and refuse to start on corruption rather than guess.
- **F7 Broken references.** For example, deleting a user leaves dangling rotation
  participants or escalation steps (the 75 cascades handle this today).
  *Rule:* every foreign key and cascade has an explicit Go equivalent with a test.
- **F8 Dropped constraints.** The limit triggers (`fn_enforce_*_limit`), the
  `UNIQUE` and `CHECK` constraints and the no-reopen rule (`fn_prevent_reopen`)
  stop being enforced. *Rule:* every constraint in `schema.sql` is listed and
  each one gets a Go check with a test.
- **F9 Inconsistent backups.** Someone copies the data directory in the middle of a
  write. *Rule:* ship a consistent snapshot/backup command and document it.
- **F10 Readers see a half-applied change.** For example, the API shows an alert
  as closed while its escalation state is still active. *Rule:* reads come from
  a consistent snapshot.
- **F25 Replay builds a different state.** Startup rebuilds memory by replaying
  the log. If applying a record calls `time.Now()`, generates an ID, uses
  randomness or depends on map iteration order, the rebuilt state differs from
  what was acknowledged (and replicas, if any, diverge). *Rule:* applying a
  record is deterministic. The writer stamps IDs and timestamps into the record
  before appending it, and a test replays every log and compares the state.

### C. Concurrency and deployment

- **F11 Two processes on the same data.** This can happen through a second
  instance started by accident, overlap during a rolling deploy, or Kubernetes
  `replicas > 1`. *Rule:* take an exclusive OS lock on the data directory at
  startup and refuse to run without it. Network filesystems (NFS, SMB, EFS) and
  volumes shared between pods (`ReadWriteMany`) are not supported: locks and
  appends on them are unreliable across machines.
- **F12 Silent loss of high availability.** Today GoAlert runs N instances behind a
  load balancer. Without replication (section G), a local filestore means
  exactly one instance. *Rule:* this trade-off is stated in the docs and in
  startup logs, never left implicit.
- **F13 Clock drift.** Postgres `now()` used to be the single clock. *Rule:* one
  process owns time. Use the monotonic clock for intervals and keep timestamps
  in UTC.
- **F14 Platform differences.** Rename and replace, file locking, and fsync
  behave differently on Windows and Linux. *Rule:* run the durability tests on
  both platforms.

### D. Performance

- **F15 Paging gets slower as data grows.** The engine runs multi-table queries
  every cycle. Full scans in place of indexes make the cycle slower, so pages
  arrive later. *Rule:* every query the engine runs each cycle has an in-memory
  index. Set a cycle-time budget and benchmark against a large dataset.
- **F16 Unbounded growth.** Alert logs and message history grow forever, and
  startup or load time grows with them. *Rule:* the cleanup and retention
  managers keep working, and append-only logs are segmented and compacted.
- **F17 Running out of memory** because everything is held in RAM. *Rule:* set and
  test a memory ceiling for a stated dataset size.
- **F21 Full disk.** Writes fail. *Rule:* fail loudly (the health check goes red
  and the error is logged) and never drop the alert quietly.

### E. Migration, upgrades, security

- **F18 Existing data is stranded.** *Rule:* a one-way Postgres-to-filestore
  export, plus a verification step that compares row counts and checksums.
- **F19 Format changes break upgrades or rollback.** Without the migration
  framework, nothing manages changes to the on-disk format. *Rule:* the format
  is versioned and each change has an explicit, tested migration.
- **F20 Secrets readable on disk.** Keyring keys, API key data, sessions and
  provider credentials in config could be read by other users. *Rule:* files are
  `0600` (or have restrictive ACLs on Windows), and keyring encryption at rest
  is kept.

### F. Rewrite process

- **F22 Behavior drift.** About 380 queries, 42 triggers and 67 functions are
  rewritten by hand, and some behavior changes along the way. *Rule:* the 141
  smoke tests are the acceptance check. They run against the filestore, and no
  test is deleted or weakened to make it pass.
- **F23 Codebase left half-migrated.** *Rule:* put a storage interface at each
  package boundary. A running GoAlert uses one backend for everything, never a
  mix, because a single change often spans several packages. Progress is
  measured by how many smoke tests pass on the new backend.
- **F24 Features removed without saying so.** Switchover (`swo/`), the River UI
  and the pgdump/resetdb devtools only make sense with Postgres. *Rule:* list
  every feature that is removed or changed.

### G. Running more than one pod (only if the log is replicated)

These apply only if GoAlert runs as several pods, each with its own disk, with
the leader replicating its log to the others (Raft, for example
`hashicorp/raft`). Pods never share one filesystem store.

- **F26 Quorum loss.** With 3 pods, losing 2 means no writes: new alerts are
  refused and nobody is paged. *Rule:* run 3 or 5 pods on different nodes.
  Health checks and logs go red the moment quorum is lost.
- **F27 Split brain from a second bootstrap.** A pod that starts a new cluster
  instead of joining the existing one creates a second leader, and both page.
  *Rule:* bootstrap only on an empty data directory and only with an explicit
  flag. Never bootstrap automatically.
- **F28 A deposed leader keeps paging.** A leader that is cut off keeps running
  its engine and sends pages the new leader also sends. *Rule:* only the leader
  runs the engine, and it pauses as soon as leadership is lost (`Engine.Pause`
  already exists). A message is claimed through the replicated log before it
  is sent, so a cut-off leader cannot claim it.
- **F29 Stale reads.** A follower's data lags slightly behind the leader's, so a
  user acks an alert and the next page load still shows it unacked. *Rule:*
  followers forward requests to the leader, or wait until they have applied
  the user's own last write.
- **F30 Mixed versions during a rolling upgrade.** A new pod writes a record type
  that old pods can't apply, and they crash or diverge. *Rule:* records are
  versioned, and a new record type is written only once every pod understands
  it.

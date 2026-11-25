# Storage Backends

GoAlert's data is one table that every pod shares. Where it is kept is a
deployment's choice. Every backend below is optional: a deployment names the one
it uses, and nothing of the others is configured, contacted or needed.
[filestore-failure-modes.md](filestore-failure-modes.md) has the rules they
follow (section I).

> **Status.** The backends, and the store on top of them, work and are tested.
> Running the whole of GoAlert on them still needs Phases 4 and 5 of
> [filestore-plan.md](filestore-plan.md), so there is no server flag for them
> yet.

## Choosing one

Two URLs say where the data is and what decides commits
(`storagebackend.Config`):

| `Objects` | Holds the data in | Credentials |
| --- | --- | --- |
| `gs://BUCKET/PREFIX` | Google Cloud Storage | application default credentials |
| `az://ACCOUNT/CONTAINER/PREFIX` | Azure Blob Storage, ADLS Gen2 | default Azure credential, or `GOALERT_AZURE_STORAGE_CONNECTION_STRING` |
| `cosmos://HOST/DATABASE/CONTAINER/PREFIX` | Azure Cosmos DB for NoSQL | default Azure credential, or `GOALERT_COSMOS_KEY` |
| `firestore://PROJECT/DATABASE/PREFIX` | Cloud Firestore, Native mode | application default credentials |
| `file:///PATH` | a local directory, for development | |

| `Log` | Commits are decided by |
| --- | --- |
| empty, or `delta` | the Delta log in the object store |
| `iceberg+https://HOST/PATH?namespace=NS` | an Iceberg REST catalog (package `icebergrest` lists its parameters) |

`Objects` has no default, and nothing falls back to anything else, so a pod that
was told the wrong thing stops instead of starting a second GoAlert (F27). A
table records what decides its commits, and a pod configured otherwise is
refused (F43). URLs can't hold secrets, since they end up in logs (F20).

Build tags leave a driver and its SDK out of a build: `goalert_no_gcs`,
`goalert_no_azure`, `goalert_no_cosmosdb`, `goalert_no_firestore` and
`goalert_no_iceberg`.

| Backend | A commit is | Needs | Other software can read it as |
| --- | --- | --- | --- |
| Delta on GCS | an object created with `ifGenerationMatch=0` | the settings in filestore-plan.md | Delta; BigQuery through BigLake |
| Delta on Azure | a blob created with `If-None-Match: *` | the settings in filestore-plan.md | Delta |
| Delta on Cosmos DB | one document created | Strong consistency as the account's default, so one write region | nothing: export it |
| Delta on Firestore | one document created | a database in Native mode, with no time-to-live policy | nothing: export it |
| Iceberg, with GCS or Azure | a snapshot the catalog swaps in | a catalog that checks `assert-ref-snapshot-id`, and an existing namespace | Iceberg and Delta |

Cosmos DB and Firestore suit an installation that already runs on one of them
and wants no bucket. Their records cost more per byte, and no analytics tool
reads them. Use a bucket otherwise.

## Concurrency

Every backend gives the same guarantee, which is Postgres's strictest:
transactions are serializable, over all pods.

- A transaction runs on the newest version and is published as the next one, in
  one atomic operation. If another pod published that version first, the
  transaction runs again on top of it (F4). Nothing is ever merged, and no two
  pods hold a lock on each other.
- A transaction is one commit whatever it touches, so an alert and its
  escalation state can't come apart (F2). Commits are not split by table or
  bucket to gain speed, for that reason.
- Transactions of a pod that wait for each other share a commit, so throughput
  grows with load instead of falling (F15).
- A commit uploads its data files in parallel, and a pod that lost a race
  uploads again only the files the other commit changed (F37, F40).
- Waits between retries grow, with jitter, up to a limit, and a pod that keeps
  losing reports `ErrConflicts` instead of trying forever (F40).
- A commit whose outcome is unclear is looked up before anything else happens
  (F32). A read can wait for the version a write returned (F29).

`filestore.DeltaOptions` has the controls:

| Option | Default | What it does |
| --- | --- | --- |
| `MaxBatch`, `BatchTimeout` | 256, 1 min | how many transactions share a commit, and how long one may take |
| `ConflictBackoff`, `MaxConflictBackoff`, `MaxConflicts` | 5 ms, 1 s, 20 | the waits after losing to another pod, and when to give up |
| `CommitAttempts`, `CommitBackoff` | 8, 100 ms | the tries to settle an unclear commit |
| `UploadConcurrency`, `LoadConcurrency` | 8, 16 | how many files are written, or read, at once |
| `PollInterval`, `MaxPollInterval` | 1 s, 10 s | how often a pod looks for other pods' commits |
| `TombstoneRetention`, `LogRetention`, `ChunkGracePeriod` | 7 days | how long what nobody should need is kept |

`DeltaDB.Conflicts` and `DeltaDB.Batches` count lost races and grouped commits,
and `BenchmarkDeltaPods` measures pods that change one row and different ones.

## How each works

**Delta in an object store** is described in filestore-plan.md.

**Document databases** limit the size of a record, so `objstore/chunkstore`
splits large objects. An object is published by creating one document, its
manifest, atomically. Up to 128 KiB, which is nearly every commit and most data
files, the object is in the manifest, so it takes one request to write and one
to read. A larger one is in chunks that are stored first. Objects are checked
against a SHA-256 digest when they are read. `CollectGarbage`, which `Vacuum`
and `CleanLog` run, deletes the chunks that failed uploads and deleted objects
leave (F42).

**Cosmos DB** keeps manifests in one logical partition and spreads chunks over
256, since one holds no more than 20 GB (F45). Create the container with
`cosmosstore.ContainerProperties`: partition key `/pk`, only `/name/?` indexed,
no time to live.

**Firestore** keeps two collections, `goalert_manifests` and `goalert_chunks`.
Exempt the fields `inline` and `data` from indexing, which would only cost
writes, and turn on delete protection.

**An Iceberg catalog** decides commits in three steps (`iceberglog`). The commit
is staged as an object, with the manifests it changes. The catalog is asked to
add a snapshot on top of the one the commit is based on, which succeeds for one
writer only. Then the commit is copied from its staged object into the Delta
log. A crash after the second step loses nothing: the next pod to read or write
does the third. The table is therefore a Delta table too, at most one commit
behind for a moment.

A snapshot's summary identifies the commit, and the commit is elsewhere, because
a catalog rewrites all its metadata for every commit. It keeps the newest 100
snapshots, and 32 manifests list the files, whatever the table's age (F46).
Data files have no field IDs, so the table has a name mapping
(`schema.name-mapping.default`), and it is not partitioned. Its property
`gc.enabled` is false, so that other tools delete nothing.

`CheckSettings` reports settings that break a backend: lifecycle rules,
retention, time to live, indexes, weak consistency (F34, F44).

## Adding a backend

1. Pick the port.
   - Storage for whole objects with an atomic create: `objstore.Store`.
   - A document database: `chunkstore.RecordStore`, nine small operations on
     records. `chunkstore.New` makes an `objstore.Store` of it.
   - Something that decides commits: `tablelog.Log`.
2. Get the errors right, since that is where data is lost. A write that did not
   happen, one that may have, and refused credentials are three different
   errors, and a record is missing only if the service says that it is the
   record. SDKs retry on their own: see `TestHiddenRetry` (F14, F32).
3. Pass the port's suite, `objstoretest`, `chunkstoretest` or `tablelogtest`, and
   `backendtest`, which runs several pods at once. Use the real SDK, against the
   service's emulator or a server that speaks its protocol, as
   `cosmosstore/fake_test.go` and `firestorestore/fake_test.go` do.
4. Register a driver for its URLs (`storagebackend.RegisterObjects` or
   `RegisterLog`), add it to `storagebackend/all` with a build tag, and check its
   settings.
5. List what it can get wrong in filestore-failure-modes.md first.

Nothing in the stores, the engine or the API changes.

## What has been tested where

| | Suites | Emulator | The service |
| --- | --- | --- | --- |
| GCS | yes | fake-gcs-server, in CI | `GOALERT_TEST_GCS_BUCKET` |
| Azure Blob Storage | yes | Azurite, in CI | `GOALERT_TEST_AZURE_CONNECTION_STRING` |
| Cosmos DB | yes, real SDK and a fake server | the preview emulator, in CI, for information | `GOALERT_TEST_COSMOS_ENDPOINT`, `GOALERT_TEST_COSMOS_KEY` |
| Firestore | yes, real SDK and a fake server | Google's emulator, in CI | `GOALERT_TEST_FIRESTORE_PROJECT` |
| Iceberg REST catalog | yes, real client and a fake server; pyiceberg and delta-rs read the table, in CI | Apache's reference server, in CI, for information | `GOALERT_TEST_ICEBERG_URL`, `GOALERT_TEST_ICEBERG_OBJECTS` |

Set the variables in the last column to run a backend's tests against the
service, for example:

    GOALERT_TEST_FIRESTORE_PROJECT=my-project go test -run Live ./objstore/firestorestore/

They write under prefixes, or to tables, of their own. No backend has been run
against its service yet; do so before relying on one.

## BigQuery

BigQuery reads GoAlert's tables and is not a backend. See
[bigquery.md](bigquery.md).

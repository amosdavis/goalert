# BigQuery

BigQuery can be GoAlert's backend, with the data in a Cloud Storage bucket as a
Delta table. It can also just query a table that another backend writes. Either
way GoAlert is the only writer (F11).

| Backend | `Objects` | `Log` |
| --- | --- | --- |
| BigQuery | `gs://BUCKET/PREFIX` | `bigquery://PROJECT/DATASET?location=US&connection=PROJECT.us.CONNECTION` |
| GCS | `gs://BUCKET/PREFIX` | empty |
| GCS and Iceberg | `gs://BUCKET/PREFIX` | `iceberg+https://biglake.googleapis.com/iceberg/v1/restcatalog?...` |

All three give the same Delta table in the bucket, and BigQuery can query it
([storage-backends.md](storage-backends.md)).

## BigQuery as the backend

BigQuery decides which commit holds each version of the table. The files and the
Delta log are in the bucket, and the Delta table is registered in BigQuery when
it is created, so `SELECT ... FROM DATASET.goalert` shows every commit at once.

**How a commit is made** (`bigquerylog`, `managedlog`):

1. The data files, and the commit itself, are stored in the bucket.
2. A job named after the table and the version is inserted. BigQuery "rejects
   any job creation request that reuses a previous job ID", so of the pods that
   want a version one gets it
   ([reliability](https://cloud.google.com/bigquery/docs/reliability-read)). The
   job's labels say for which commit.
3. The commit is copied into the Delta log.

A pod that crashes after step 2 has committed, and the next pod does step 3. If
the answer to step 2 is lost, the job says whose commit it is (F32). The job is
`SELECT 1` with batch priority: what counts is that it exists, not that it runs.
It processes no data, so queries billed by data cost nothing. A commit takes one
more request than on GCS alone.

Other ways to let BigQuery decide were ruled out. A conditional `UPDATE` is not
documented to be a compare-and-swap, and takes seconds. A stream of the Storage
Write API allows one connection. Transactions are not queued, and a failed one
should wait a minute.

**What it needs:**

- A dataset, and a
  [connection](https://cloud.google.com/bigquery/docs/create-cloud-resource-connection)
  in the same location, whose service account may read the bucket
  (`roles/storage.objectViewer`).
- For GoAlert's identity: `roles/storage.objectAdmin` on the bucket,
  `roles/bigquery.jobUser` on the project, and to register the table,
  `roles/bigquery.dataEditor` on the dataset and `roles/bigquery.connectionUser`
  on the connection.
- The same project and location for every pod, for good. They are in the table's
  metadata, and a pod with others is refused (F43).

**What to know** (F50):

- Nobody may delete the metadata of jobs (`bigquery.jobs.delete`). Only the
  newest commit ever depends on its job, and only for the moment until it is in
  the Delta log.
- BigQuery keeps jobs for six months. A table nobody used for longer is opened
  from its Delta log.
- A refusal by quota is tried again, and is not taken for a lost permission.
- Without a dataset (`bigquery://PROJECT?location=US`) BigQuery decides commits
  and the table is not registered.

**The history of commits,** for as long as BigQuery keeps jobs:

    SELECT creation_time, user_email,
      (SELECT value FROM UNNEST(labels) WHERE key = 'goalert_version') AS version,
      (SELECT value FROM UNNEST(labels) WHERE key = 'goalert_commit') AS commit_id
    FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
    WHERE job_id LIKE 'goalert-%' ORDER BY creation_time DESC

## Querying the table

A row holds one GoAlert table's columns, and `tbl` says which:

    SELECT alerts.* FROM `DATASET.goalert` WHERE tbl = 'alerts'

    CREATE VIEW `DATASET.alerts` AS
    SELECT alerts.* FROM `DATASET.goalert` WHERE tbl = 'alerts'

With another backend, [bigquery/delta-biglake.sql](bigquery/delta-biglake.sql)
registers a table in GCS, whether the Delta log or an Iceberg catalog decides its
commits. BigQuery reads the log whenever it is queried, so nothing has to be
refreshed. After a GoAlert upgrade that adds columns, detect the schema again, as
the file shows.

GoAlert uses Delta reader version 1 and classic checkpoints, which is less than
BigQuery supports. Its documentation says that the reader version must be in the
last log entry; GoAlert writes it in version 0 and in checkpoints, as Spark does.

Give those who query the table read access to the BigQuery table, and the
connection read access to the bucket. Never run DML, `OPTIMIZE` or `VACUUM` on
the table, and never make a BigQuery-managed Iceberg table of the prefix:
BigQuery deletes files it does not track, which are GoAlert's (F6).

## GCS and Iceberg

Google's Lakehouse runtime catalog (BigLake metastore) is an Iceberg REST
catalog. With it, one table in the bucket is both Delta and Iceberg:

    iceberg+https://biglake.googleapis.com/iceberg/v1/restcatalog
      ?warehouse=bl://projects/PROJECT/catalogs/CATALOG
      &namespace=NAMESPACE&auth=google&project=PROJECT

The namespace must exist, and `gs://BUCKET/PREFIX` must be under its path, since
the catalog allows tables nowhere else. The catalog takes metadata of up to 1 MB;
GoAlert's stays well below that (F46).

To query it, register the Delta table as above.
[bigquery/iceberg-external.sql](bigquery/iceberg-external.sql) shows it as
Iceberg, at a fixed version. GoAlert's files have no Iceberg field IDs. The
table's name mapping stands in for them, and pyiceberg and iceberg-go read it,
but BigQuery's documentation does not say whether BigQuery does. If it does not,
every column is null.

## References

- [Reliability: jobs are made at most once](https://cloud.google.com/bigquery/docs/reliability-read)
- [Job metadata is kept for six months](https://cloud.google.com/bigquery/docs/managing-jobs)
- [Delta Lake external tables](https://cloud.google.com/bigquery/docs/create-delta-lake-table)
- [The Iceberg REST catalog](https://cloud.google.com/biglake/docs/blms-rest-catalog)
- [Managed Iceberg tables](https://cloud.google.com/bigquery/docs/iceberg-tables),
  with their limits on concurrent DML and on writing from outside BigQuery

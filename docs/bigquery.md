# BigQuery

BigQuery can query GoAlert's data where it is, in a GCS bucket. It is **read
only**, and not a storage backend: a commit needs a conditional write that
answers in milliseconds, and BigQuery runs one mutating statement at a time on a
managed Iceberg table and queues the rest.

GoAlert is the only writer (F11). Give BigQuery's connection read access to the
table's prefix and nothing more. Never run DML, `OPTIMIZE` or `VACUUM` on the
table, and never make a BigQuery-managed Iceberg table of the prefix: BigQuery
deletes files it does not track, which are GoAlert's (F6).

## Delta tables, and Iceberg ones

[bigquery/delta-biglake.sql](bigquery/delta-biglake.sql) makes a BigLake external
table of the prefix that holds `_delta_log`. BigQuery reads the log whenever it
is queried, so nothing has to be refreshed.

Use it for a table whose commits an Iceberg catalog decides, too: that table has
a Delta log as well, over the same files ([storage-backends.md](storage-backends.md)).

A row holds one GoAlert table's columns, and `tbl` says which:

    SELECT alerts.* FROM `DATASET.goalert` WHERE tbl = 'alerts'

GoAlert uses Delta reader version 1 and a classic checkpoint, which is less than
BigQuery supports. Its documentation says that the reader version must be in the
last log entry, and GoAlert writes it in version 0 and in checkpoints, as Spark
does. Check a query against GoAlert before relying on one.

## As Iceberg

[bigquery/iceberg-external.sql](bigquery/iceberg-external.sql) is for those who
need Iceberg. It has drawbacks, so prefer the Delta table:

- It names one metadata file, and shows that version until it is pointed at
  another. Google recommends its runtime catalog instead.
- GoAlert's files have no Iceberg field IDs. The table's name mapping stands in
  for them, and pyiceberg and iceberg-go read it, but BigQuery's documentation
  does not say whether BigQuery does. If it does not, every column is null.

## References

- [Delta Lake external tables](https://cloud.google.com/bigquery/docs/create-delta-lake-table)
- [Iceberg external tables](https://cloud.google.com/bigquery/docs/iceberg-external-tables)
- [Managed Iceberg tables](https://cloud.google.com/bigquery/docs/iceberg-tables),
  with their limits on concurrent DML and on writing from outside BigQuery

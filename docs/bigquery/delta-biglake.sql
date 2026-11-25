-- Registers GoAlert's table in GCS with BigQuery, read only. The BigQuery backend does
-- this itself; this is for other backends, whether a table's commits are
-- decided by the Delta log or by an Iceberg catalog: see ../bigquery.md.
--
-- Replace every placeholder. The URI is the prefix that holds _delta_log.
CREATE EXTERNAL TABLE `PROJECT_ID.DATASET_ID.goalert`
WITH CONNECTION `PROJECT_ID.REGION.CONNECTION_ID`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://BUCKET/PREFIX']
);

-- BigQuery reads the log at query time, so nothing has to be refreshed. After a
-- GoAlert upgrade that adds columns:
--
-- bq update --autodetect_schema PROJECT_ID:DATASET_ID.goalert

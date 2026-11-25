-- Read-only BigQuery access to GoAlert's table in GCS, whether its commits are
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

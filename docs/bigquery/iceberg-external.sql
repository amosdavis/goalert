-- Read-only BigQuery access to GoAlert's Iceberg table. Prefer
-- delta-biglake.sql, which works for the same table: see ../bigquery.md.
--
-- Replace every placeholder. The URI is the current metadata file the Iceberg
-- catalog reports, not the table's location.
CREATE EXTERNAL TABLE `PROJECT_ID.DATASET_ID.goalert_iceberg`
WITH CONNECTION `PROJECT_ID.REGION.CONNECTION_ID`
OPTIONS (
  format = 'ICEBERG',
  uris = ['gs://BUCKET/PREFIX/metadata/CURRENT.metadata.json']
);

-- The table stays at that version. To show a newer one, point it at the newer
-- metadata file:
--
-- bq mkdef --source_format=ICEBERG \
--   "gs://BUCKET/PREFIX/metadata/CURRENT.metadata.json" > goalert-iceberg.json
-- bq update --autodetect_schema --external_table_definition=goalert-iceberg.json \
--   PROJECT_ID:DATASET_ID.goalert_iceberg

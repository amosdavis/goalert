// Package all registers every storage driver. Which one a deployment uses, if
// any, is its choice; see storagebackend.Config. To leave a driver and its SDK
// out of a build, use the build tags goalert_no_gcs, goalert_no_azure,
// goalert_no_cosmosdb, goalert_no_firestore, goalert_no_iceberg and
// goalert_no_bigquery.
package all

package slack

import "github.com/target/goalert/filestore"

// userSlackDataRow is a user_slack_data row on the Delta backend.
type userSlackDataRow struct {
	ID          string `parquet:"id"`
	AccessToken string `parquet:"access_token"`
}

// DeltaTables are this package's tables on the Delta backend.
type DeltaTables struct {
	userData *filestore.Table[userSlackDataRow]
}

// DefineDeltaTables adds user_slack_data, which nothing reads or writes, to
// the Delta backend's schema, keyed by id (user_slack_data_pkey).
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	return &DeltaTables{
		userData: filestore.NewTable(s, "user_slack_data", func(r userSlackDataRow) string { return r.ID }),
	}
}

// DeclareDeltaForeignKeys declares this package's foreign keys to other
// packages' tables.
func DeclareDeltaForeignKeys[U any](t *DeltaTables, users *filestore.Table[U]) {
	filestore.References(t.userData, "user_slack_data_id_fkey", users,
		func(r userSlackDataRow) (string, bool) { return r.ID, true }, filestore.Cascade, nil)
}

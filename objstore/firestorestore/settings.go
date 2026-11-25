package firestorestore

import (
	"context"
	"errors"
	"fmt"
	"path"

	admin "cloud.google.com/go/firestore/apiv1/admin"
	"cloud.google.com/go/firestore/apiv1/admin/adminpb"
	"github.com/target/goalert/objstore"
	"google.golang.org/api/iterator"
)

// CheckSettings reports database settings that break or weaken the store
// (F34). It needs permission to read the database's settings and its indexes
// (datastore.databases.getMetadata and datastore.indexes.list); without it, the
// error matches objstore.ErrDenied and the settings are unchecked. The emulator
// has no settings to check.
func (s *Store) CheckSettings(ctx context.Context) ([]objstore.SettingProblem, error) {
	client, err := admin.NewFirestoreAdminClient(ctx, s.opts...)
	if err != nil {
		return nil, opErr("read the database's settings", err)
	}
	defer func() { _ = client.Close() }()
	name := fmt.Sprintf("projects/%s/databases/%s", s.cfg.Project, s.cfg.Database)
	db, err := client.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: name})
	if err != nil {
		return nil, opErr("read the database's settings", err)
	}
	fields := make(map[string]*adminpb.Field)
	for _, group := range []string{manifestCollection, chunkCollection, "__default__"} {
		it := client.ListFields(ctx, &adminpb.ListFieldsRequest{
			Parent: name + "/collectionGroups/" + group,
			Filter: "indexConfig.usesAncestorConfig:false OR ttlConfig:*",
		})
		for {
			f, err := it.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				return nil, opErr("read the database's field settings", err)
			}
			fields[group+"/"+path.Base(f.GetName())] = f
		}
	}
	return checkDatabase(db, fields), nil
}

// checkDatabase checks a database and the fields that have settings of their
// own, by collection and name.
func checkDatabase(db *adminpb.Database, fields map[string]*adminpb.Field) []objstore.SettingProblem {
	var out []objstore.SettingProblem
	add := func(breaks bool, setting, format string, args ...any) {
		out = append(out, objstore.SettingProblem{Breaks: breaks, Setting: setting, Problem: fmt.Sprintf(format, args...)})
	}
	if t := db.GetType(); t != adminpb.Database_FIRESTORE_NATIVE {
		add(true, "database type", "it is %s, not Firestore in Native mode", t)
	}
	if db.GetDeleteProtectionState() != adminpb.Database_DELETE_PROTECTION_ENABLED {
		add(false, "delete protection", "not enabled, so one command can delete the database (F9)")
	}
	if db.GetPointInTimeRecoveryEnablement() != adminpb.Database_POINT_IN_TIME_RECOVERY_ENABLED {
		add(false, "point-in-time recovery", "not enabled, so a mistake can't be undone with it (F9)")
	}
	for key, f := range fields {
		if f.GetTtlConfig() != nil {
			add(true, "time to live", "a policy on %s deletes live commits and data (F6)", key)
		}
	}
	// A field without settings of its own has its database's, which the
	// default is to index.
	indexed := func(collection, field string) bool {
		for _, key := range []string{collection + "/" + field, collection + "/*", "__default__/*"} {
			if f, ok := fields[key]; ok && f.GetIndexConfig() != nil && !f.GetIndexConfig().GetUsesAncestorConfig() {
				return len(f.GetIndexConfig().GetIndexes()) > 0
			}
		}
		return true
	}
	if !indexed(manifestCollection, "name") {
		add(true, "field index", "%s/name is not indexed, which listing needs", manifestCollection)
	}
	if indexed(manifestCollection, "inline") || indexed(chunkCollection, "data") {
		add(false, "field index", "%s/inline and %s/data are indexed, which costs writes and storage and is never used; exempt them (F37)", manifestCollection, chunkCollection)
	}
	return out
}

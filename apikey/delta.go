package apikey

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"maps"
	"net"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/auth/authdelta"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/keyring"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/validation"
)

// keyRow is a gql_api_keys row on the Delta backend.
type keyRow struct {
	ID          string     `parquet:"id"`
	Name        string     `parquet:"name"`
	Description string     `parquet:"description"`
	Policy      []byte     `parquet:"policy"`
	ExpiresAt   time.Time  `parquet:"expires_at,timestamp(microsecond:utc)"`
	CreatedAt   time.Time  `parquet:"created_at,timestamp(microsecond:utc)"`
	CreatedBy   *string    `parquet:"created_by,optional"`
	UpdatedAt   time.Time  `parquet:"updated_at,timestamp(microsecond:utc)"`
	UpdatedBy   *string    `parquet:"updated_by,optional"`
	DeletedAt   *time.Time `parquet:"deleted_at,optional,timestamp(microsecond:utc)"`
	DeletedBy   *string    `parquet:"deleted_by,optional"`
}

// usageRow is a gql_api_key_usage row on the Delta backend.
type usageRow struct {
	ID        int64     `parquet:"id"`
	APIKeyID  *string   `parquet:"api_key_id,optional"`
	UsedAt    time.Time `parquet:"used_at,timestamp(microsecond:utc)"`
	UserAgent *string   `parquet:"user_agent,optional"`
	IPAddress *string   `parquet:"ip_address,optional"`
}

// liveKey is the key of every API key that isn't deleted in the index of
// those.
const liveKey = "live"

// maxExpires is later than any time a row can hold (F25).
var maxExpires = time.Date(2262, 4, 11, 0, 0, 0, 0, time.UTC)

// DeltaTables are the API key store's tables on the Delta backend.
type DeltaTables struct {
	keys       *filestore.Table[keyRow]
	live       *filestore.Index[keyRow]
	usage      *filestore.Table[usageRow]
	usageByKey *filestore.Unique[usageRow]
}

// DefineDeltaTables adds the API key store's tables to the Delta backend's
// schema: gql_api_keys, keyed by id (gql_api_keys_pkey), and
// gql_api_key_usage, keyed by id (gql_api_key_usage_pkey). The usage table
// has one small row for each API key and all of it changes together, so one
// bucket is enough (F39).
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	t := &DeltaTables{}
	t.keys = filestore.NewTable(s, "gql_api_keys", func(r keyRow) string { return r.ID })
	t.keys.Unique("gql_api_keys_name_key", func(r keyRow) (string, bool) { return r.Name, r.DeletedAt == nil })
	t.live = t.keys.Index("gql_api_keys_live", func(r keyRow) []string {
		if r.DeletedAt != nil {
			return nil
		}
		return []string{liveKey}
	})

	t.usage = filestore.NewTable(s, "gql_api_key_usage", func(r usageRow) string { return filestore.IntKey(r.ID) }).Buckets(1)
	t.usageByKey = t.usage.Unique("gql_api_key_usage_api_key_id_key", func(r usageRow) (string, bool) { return filestore.Ref(r.APIKeyID) })
	filestore.References(t.usage, "gql_api_key_usage_api_key_id_fkey", t.keys,
		func(r usageRow) (string, bool) { return filestore.Ref(r.APIKeyID) }, filestore.Cascade, nil)
	return t
}

// DeclareDeltaForeignKeys declares the API key store's foreign keys to users.
func DeclareDeltaForeignKeys[U any](t *DeltaTables, users *filestore.Table[U]) {
	filestore.References(t.keys, "gql_api_keys_created_by_fkey", users,
		func(r keyRow) (string, bool) { return filestore.Ref(r.CreatedBy) }, filestore.SetNull,
		func(r keyRow) keyRow { return r.withoutUser(*r.CreatedBy) })
	filestore.References(t.keys, "gql_api_keys_updated_by_fkey", users,
		func(r keyRow) (string, bool) { return filestore.Ref(r.UpdatedBy) }, filestore.SetNull,
		func(r keyRow) keyRow { return r.withoutUser(*r.UpdatedBy) })
	filestore.References(t.keys, "gql_api_keys_deleted_by_fkey", users,
		func(r keyRow) (string, bool) { return filestore.Ref(r.DeletedBy) }, filestore.SetNull,
		func(r keyRow) keyRow { return r.withoutUser(*r.DeletedBy) })
}

// withoutUser returns the row without any of its references to a user who was
// deleted. They all go at once, since writing a row checks each of them.
func (r keyRow) withoutUser(userID string) keyRow {
	for _, ref := range []**string{&r.CreatedBy, &r.UpdatedBy, &r.DeletedBy} {
		if *ref != nil && **ref == userID {
			*ref = nil
		}
	}
	return r
}

// NewDeltaStore returns an API key store on the Delta backend. Its Shutdown
// writes the last uses of keys that aren't written yet.
func NewDeltaStore(db txn.DB, tables *DeltaTables, key keyring.Keyring) *Store {
	d := &deltaStore{db: db, DeltaTables: tables, check: authdelta.NewChecker(db, authdelta.MaxStale)}
	d.uses = authdelta.NewRecorder(authdelta.RecordInterval, "API key usage", d.writeUsage)
	s := &Store{key: key, delta: d}
	s.initCaches()
	return s
}

// Shutdown writes the uses of API keys that aren't written yet.
func (s *Store) Shutdown(ctx context.Context) error {
	if s == nil || s.delta == nil {
		return nil
	}
	return s.delta.uses.Flush(ctx)
}

type deltaStore struct {
	db txn.DB
	*DeltaTables
	check *authdelta.Checker
	uses  *authdelta.Recorder[keyUse]
}

// keyUse is one use of an API key.
type keyUse struct {
	at time.Time
	ua string
	ip net.IP
}

func nullID(id uuid.NullUUID) *string {
	if !id.Valid {
		return nil
	}
	s := id.UUID.String()
	return &s
}

// parseNullID returns nil for no ID.
func parseNullID(s *string) (*uuid.UUID, error) {
	if s == nil {
		return nil, nil
	}
	id, err := uuid.Parse(*s)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

func (d *deltaStore) insert(ctx context.Context, id uuid.UUID, opt NewAdminGQLKeyOpts, policy []byte, by uuid.NullUUID) error {
	if !opt.Expires.Before(maxExpires) {
		return validation.NewFieldError("Expires", "must be before "+maxExpires.Format(time.DateOnly))
	}
	return deltatxn.Write(ctx, d.db, nil, func(s deltatxn.Scope) error {
		return d.keys.Insert(s.Tx, keyRow{
			ID: id.String(), Name: opt.Name, Description: opt.Desc, Policy: policy, ExpiresAt: opt.Expires,
			CreatedAt: s.Now, CreatedBy: nullID(by), UpdatedAt: s.Now, UpdatedBy: nullID(by),
		}, "gql_api_keys_pkey")
	})
}

// update sets the name and description that aren't nil. It returns
// sql.ErrNoRows if there is no such key, or if it was deleted.
func (d *deltaStore) update(ctx context.Context, id uuid.UUID, name, desc *string, by uuid.NullUUID) error {
	return deltatxn.Write(ctx, d.db, nil, func(s deltatxn.Scope) error {
		row, ok := d.keys.Get(s.Reader, id.String())
		if !ok || row.DeletedAt != nil {
			return sql.ErrNoRows
		}
		if name != nil {
			row.Name = *name
		}
		if desc != nil {
			row.Description = *desc
		}
		row.UpdatedAt, row.UpdatedBy = s.Now, nullID(by)
		return d.keys.Put(s.Tx, row)
	})
}

// revoke marks a key as deleted. The row stays, as it must for its ID never
// to work again.
func (d *deltaStore) revoke(ctx context.Context, id uuid.UUID, by uuid.NullUUID) error {
	return deltatxn.Write(ctx, d.db, nil, func(s deltatxn.Scope) error {
		row, ok := d.keys.Get(s.Reader, id.String())
		if !ok {
			return nil
		}
		row.DeletedAt, row.DeletedBy = &s.Now, nullID(by)
		return d.keys.Put(s.Tx, row)
	})
}

// find returns the key if it can be used: it wasn't deleted and has not
// expired.
func (d *deltaStore) find(s deltatxn.Scope, id uuid.UUID) (keyRow, error) {
	row, ok := d.keys.Get(s.Reader, id.String())
	if !ok || row.DeletedAt != nil || !row.ExpiresAt.After(s.Now) {
		return keyRow{}, sql.ErrNoRows
	}
	return row, nil
}

// policy returns the policy of a key that can be used, or sql.ErrNoRows. The
// caller remembers for good that a key doesn't work, so a key that this pod
// doesn't know must not exist at the log's latest version either (F29).
func (d *deltaStore) policy(ctx context.Context, id uuid.UUID) ([]byte, error) {
	var data []byte
	err := d.check.Read(ctx, func(_ txn.Tx, s deltatxn.Scope) error {
		row, err := d.find(s, id)
		data = row.Policy
		return err
	})
	if err != nil {
		return nil, err
	}
	return data, nil
}

// isValid reports whether a key can be used.
func (d *deltaStore) isValid(ctx context.Context, id uuid.UUID) (bool, error) {
	err := d.check.Read(ctx, func(_ txn.Tx, s deltatxn.Scope) error {
		_, err := d.find(s, id)
		return err
	})
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// list returns the keys that weren't deleted, oldest first, with their last
// use.
func (d *deltaStore) list(ctx context.Context) ([]APIKeyInfo, error) {
	var keys []keyRow
	uses := make(map[string]usageRow)
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		keys = nil
		d.live.Ascend(s.Reader, liveKey, func(row keyRow) bool {
			keys = append(keys, row)
			if u, ok := d.usageByKey.Get(s.Reader, row.ID); ok {
				uses[row.ID] = u
			}
			return true
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(keys, func(a, b keyRow) int {
		return cmp.Or(a.CreatedAt.Compare(b.CreatedAt), cmp.Compare(a.ID, b.ID))
	})

	res := make([]APIKeyInfo, 0, len(keys))
	for _, k := range keys {
		id, err := uuid.Parse(k.ID)
		if err != nil {
			return nil, err
		}
		p, ok := listPolicy(ctx, id, k.Policy)
		if !ok {
			continue
		}
		info := APIKeyInfo{
			ID: id, Name: k.Name, Description: k.Description, ExpiresAt: k.ExpiresAt,
			CreatedAt: k.CreatedAt, UpdatedAt: k.UpdatedAt, Query: p.Query, Role: p.Role,
		}
		if info.CreatedBy, err = parseNullID(k.CreatedBy); err != nil {
			return nil, err
		}
		if info.UpdatedBy, err = parseNullID(k.UpdatedBy); err != nil {
			return nil, err
		}
		if u, ok := uses[k.ID]; ok {
			info.LastUsed = &APIKeyUsage{Time: u.UsedAt}
			if u.UserAgent != nil {
				info.LastUsed.UserAgent = *u.UserAgent
			}
			if u.IPAddress != nil {
				info.LastUsed.IP = *u.IPAddress
			}
		}
		res = append(res, info)
	}
	return res, nil
}

// recordUsage records that a key was used now. It doesn't commit: a pod
// writes the uses of all keys together, at most once a minute.
func (d *deltaStore) recordUsage(ctx context.Context, id uuid.UUID, ua string, ip net.IP) error {
	return deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		d.uses.Add(id.String(), keyUse{at: s.Now, ua: ua, ip: ip})
		return nil
	})
}

// writeUsage writes uses to gql_api_key_usage, unless another pod has written
// a later one.
func (d *deltaStore) writeUsage(ctx context.Context, uses map[string]keyUse) error {
	return deltatxn.Write(ctx, d.db, nil, func(s deltatxn.Scope) error {
		for _, id := range slices.Sorted(maps.Keys(uses)) {
			use := uses[id]
			if _, ok := d.keys.Get(s.Reader, id); !ok {
				continue
			}
			row, ok := d.usageByKey.Get(s.Reader, id)
			if ok && !row.UsedAt.Before(use.at) {
				continue
			}
			if !ok {
				n, err := s.Tx.NextID("gql_api_key_usage_id_seq")
				if err != nil {
					return err
				}
				row = usageRow{ID: n, APIKeyID: &id}
			}
			row.UsedAt, row.UserAgent, row.IPAddress = use.at, &use.ua, nil
			if use.ip != nil {
				ip := use.ip.String()
				row.IPAddress = &ip
			}
			if err := d.usage.Put(s.Tx, row); err != nil {
				return err
			}
		}
		return nil
	})
}

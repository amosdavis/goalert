package integrationkey

import (
	"context"
	"database/sql"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
)

// keyRow is an integration_keys row on the Delta backend.
type keyRow struct {
	ID                 string  `parquet:"id"`
	Name               string  `parquet:"name"`
	Type               string  `parquet:"type"`
	ServiceID          string  `parquet:"service_id"`
	ExternalSystemName *string `parquet:"external_system_name,optional"`
}

// newKeyRow returns the row of a valid key. IDs are stored in the form
// Postgres gives UUIDs.
func newKeyRow(k *IntegrationKey) keyRow {
	return keyRow{
		ID:                 uuid.MustParse(k.ID).String(),
		Name:               k.Name,
		Type:               string(k.Type),
		ServiceID:          uuid.MustParse(k.ServiceID).String(),
		ExternalSystemName: optText(k.ExternalSystemName),
	}
}

func (r keyRow) key() IntegrationKey {
	return IntegrationKey{
		ID:        r.ID,
		Name:      r.Name,
		Type:      Type(r.Type),
		ServiceID: r.ServiceID,

		ExternalSystemName: text(r.ExternalSystemName),
	}
}

func optText(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func text(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// DeltaTables are the integration key store's tables on the Delta backend.
// Other packages declare their foreign keys to Keys.
type DeltaTables struct {
	Keys *filestore.Table[keyRow]

	byService *filestore.Index[keyRow]
}

// DefineDeltaTables adds the integration key store's table to the Delta
// backend's schema: integration_keys, keyed by id (integration_keys_pkey).
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	t := &DeltaTables{
		Keys: filestore.NewTable(s, "integration_keys", func(r keyRow) string { return r.ID }),
	}

	t.byService = t.Keys.Index("idx_integration_key_service", func(r keyRow) []string { return []string{r.ServiceID} })
	// A name has no slash (validate.IDName), so the key has only one reading.
	t.Keys.Unique("idx_int_key_name_svc_ext", func(r keyRow) (string, bool) {
		return strings.ToLower(r.Name) + "/" + r.ServiceID + "/" + text(r.ExternalSystemName), true
	})

	return t
}

// DeclareDeltaForeignKeys declares this package's foreign keys to other
// packages' tables.
func DeclareDeltaForeignKeys[Svc any](t *DeltaTables, services *filestore.Table[Svc]) {
	filestore.References(t.Keys, "integration_keys_services_id_fkey", services,
		func(r keyRow) (string, bool) { return r.ServiceID, true }, filestore.Cascade, nil)
}

// ServiceID returns, in r, the service of an integration key. The service
// store's search uses it.
func (t *DeltaTables) ServiceID(r filestore.Reader, keyID string) (string, bool) {
	row, ok := t.Keys.Get(r, keyID)
	return row.ServiceID, ok
}

// DeltaLookups are the limits (config_limits) that the store enforces, -1 for
// none. They run in the store's transaction, so they must do no I/O.
type DeltaLookups struct {
	KeysPerService func(r filestore.Reader) int // integration_keys_per_service
}

// NewDeltaStore returns an integration key store on the Delta backend.
func NewDeltaStore(db txn.DB, tables *DeltaTables, lookups DeltaLookups) *Store {
	if lookups.KeysPerService == nil {
		panic("integrationkey: NewDeltaStore needs its limits") // or they would not be enforced (F8)
	}
	return &Store{delta: &deltaKeys{db: db, DeltaTables: tables, DeltaLookups: lookups}}
}

type deltaKeys struct {
	db txn.DB
	*DeltaTables
	DeltaLookups
}

// checkLimit is a trigger's check of a limit on how many rows have key in an
// index, after one of them was inserted.
func checkLimit[V any](r filestore.Reader, table, constraint string, limit int, ix *filestore.Index[V], key string) error {
	if limit == -1 || ix.Count(r, key) <= limit {
		return nil
	}
	return &filestore.CheckError{
		Table: table, Constraint: constraint,
		Message: "limit exceeded", HintText: "max=" + strconv.Itoa(limit),
	}
}

// create adds a key.
func (d *deltaKeys) create(ctx context.Context, tx txn.Tx, k *IntegrationKey) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row := newKeyRow(k)
		if err := d.Keys.Insert(s.Tx, row, "integration_keys_pkey"); err != nil {
			return err
		}
		// trg_enforce_integration_key_limit
		err := checkLimit(s.Tx, "integration_keys", "integration_keys_per_service_limit",
			d.KeysPerService(s.Tx), d.byService, row.ServiceID)
		return err
	})
}

func (d *deltaKeys) delete(ctx context.Context, tx txn.Tx, ids []uuid.UUID) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		for _, id := range ids {
			if _, err := d.Keys.Delete(s.Tx, id.String()); err != nil {
				return err
			}
		}
		return nil
	})
}

// serviceID returns sql.ErrNoRows for a missing key, or one of another type.
func (d *deltaKeys) serviceID(ctx context.Context, id uuid.UUID, typ Type) (string, error) {
	var serviceID string
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		row, ok := d.Keys.Get(s.Reader, id.String())
		if !ok || Type(row.Type) != typ {
			return sql.ErrNoRows
		}
		serviceID = row.ServiceID
		return nil
	})
	return serviceID, err
}

// findOne returns nil for a missing key, as the Postgres store does.
func (d *deltaKeys) findOne(ctx context.Context, id uuid.UUID) (*IntegrationKey, error) {
	var key *IntegrationKey
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		key = nil
		if row, ok := d.Keys.Get(s.Reader, id.String()); ok {
			k := row.key()
			key = &k
		}
		return nil
	})
	return key, err
}

func (d *deltaKeys) findAllByService(ctx context.Context, serviceID uuid.UUID) ([]IntegrationKey, error) {
	var keys []IntegrationKey
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		keys = []IntegrationKey{} // not nil, as the Postgres store returns
		d.byService.Ascend(s.Reader, serviceID.String(), func(row keyRow) bool {
			keys = append(keys, row.key())
			return true
		})
		return nil
	})
	return keys, err
}

// search returns keys without their external system names, like the Postgres
// query. That query has no order, and After makes it fail; this one orders by
// name, so that After can page.
func (d *deltaKeys) search(ctx context.Context, opts InKeySearchOptions) ([]IntegrationKey, error) {
	omit := make(map[string]bool, len(opts.Omit))
	for _, id := range opts.Omit {
		parsed, err := uuid.Parse(id)
		if err != nil {
			return nil, err // the Postgres query fails too
		}
		omit[parsed.String()] = true
	}
	prefix, after := strings.ToLower(opts.Search), strings.ToLower(opts.After)

	var keys []IntegrationKey
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		keys = nil // as the Postgres store returns without rows
		// The search is a prefix of the ID, so the matches are next to each
		// other in the table.
		d.Keys.AscendFrom(s.Reader, prefix, func(row keyRow) bool {
			if !strings.HasPrefix(row.ID, prefix) {
				return false
			}
			if !omit[row.ID] && (after == "" || strings.ToLower(row.Name) > after) {
				keys = append(keys, IntegrationKey{ID: row.ID, Name: row.Name, Type: Type(row.Type), ServiceID: row.ServiceID})
			}
			return true
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortStableFunc(keys, func(a, b IntegrationKey) int {
		return strings.Compare(strings.ToLower(a.Name), strings.ToLower(b.Name))
	})
	if len(keys) > opts.Limit {
		keys = keys[:opts.Limit]
	}
	return keys, nil
}

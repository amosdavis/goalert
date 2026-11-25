package integrationkey

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/keyring"
	"github.com/target/goalert/notification/nfydest"
	"github.com/target/goalert/notificationchannel"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/validation"
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

// uikConfigRow is a uik_config row on the Delta backend: the config (a
// gadb.UIKConfig in JSON) and the tokens of a universal key.
type uikConfigRow struct {
	ID                 string  `parquet:"id"`
	Config             []byte  `parquet:"config"`
	PrimaryToken       *string `parquet:"primary_token,optional"`
	PrimaryTokenHint   *string `parquet:"primary_token_hint,optional"`
	SecondaryToken     *string `parquet:"secondary_token,optional"`
	SecondaryTokenHint *string `parquet:"secondary_token_hint,optional"`
}

// signalRow is a pending_signals row on the Delta backend. The signal is
// unsent until the engine gives it a message.
type signalRow struct {
	ID        int64     `parquet:"id"`
	CreatedAt time.Time `parquet:"created_at,timestamp(microsecond:utc)"`
	DestID    string    `parquet:"dest_id"`
	ServiceID string    `parquet:"service_id"`
	MessageID *string   `parquet:"message_id,optional"`
	Params    []byte    `parquet:"params"`
}

// AllUnsent is the key of every unsent signal in DeltaTables.Unsent.
const AllUnsent = "*"

func unsentByDest(serviceID, destID string) string { return serviceID + "/" + destID }

// DeltaTables are the integration key store's tables on the Delta backend.
// Other packages declare their foreign keys to Keys, and the engine sends what
// is in Signals.
type DeltaTables struct {
	Keys    *filestore.Table[keyRow]
	configs *filestore.Table[uikConfigRow]
	Signals *filestore.Table[signalRow]

	byService *filestore.Index[keyRow]

	// Unsent has the signals without a message, oldest first: all of them
	// under AllUnsent, and those of a service under its ID.
	Unsent *filestore.Index[signalRow]
	// SignalByMessage has the signals by the ID of their message.
	SignalByMessage *filestore.Unique[signalRow]
}

// DefineDeltaTables adds the integration key store's tables to the Delta
// backend's schema, all keyed by id: integration_keys (integration_keys_pkey),
// uik_config (uik_config_pkey), and pending_signals (pending_signals_pkey).
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	t := &DeltaTables{
		Keys:    filestore.NewTable(s, "integration_keys", func(r keyRow) string { return r.ID }),
		configs: filestore.NewTable(s, "uik_config", func(r uikConfigRow) string { return r.ID }),
		Signals: filestore.NewTable(s, "pending_signals", func(r signalRow) string { return filestore.IntKey(r.ID) }),
	}

	t.byService = t.Keys.Index("idx_integration_key_service", func(r keyRow) []string { return []string{r.ServiceID} })
	// A name has no slash (validate.IDName), so the key has only one reading.
	t.Keys.Unique("idx_int_key_name_svc_ext", func(r keyRow) (string, bool) {
		return strings.ToLower(r.Name) + "/" + r.ServiceID + "/" + text(r.ExternalSystemName), true
	})

	t.configs.Unique("uik_config_primary_token_key", func(r uikConfigRow) (string, bool) { return filestore.Ref(r.PrimaryToken) })
	t.configs.Unique("uik_config_secondary_token_key", func(r uikConfigRow) (string, bool) { return filestore.Ref(r.SecondaryToken) })
	filestore.References(t.configs, "uik_config_id_fkey", t.Keys,
		func(r uikConfigRow) (string, bool) { return r.ID, true }, filestore.Cascade, nil)

	t.Unsent = t.Signals.Index("pending_signals_unsent", func(r signalRow) []string {
		if r.MessageID != nil {
			return nil
		}
		return []string{AllUnsent, r.ServiceID, unsentByDest(r.ServiceID, r.DestID)}
	})
	t.SignalByMessage = t.Signals.Unique("pending_signals_message_id_key", func(r signalRow) (string, bool) { return filestore.Ref(r.MessageID) })
	return t
}

// DeclareDeltaForeignKeys declares this package's foreign keys to other
// packages' tables.
func DeclareDeltaForeignKeys[Svc, NC, Msg any](t *DeltaTables, services *filestore.Table[Svc], channels *filestore.Table[NC], messages *filestore.Table[Msg]) {
	filestore.References(t.Keys, "integration_keys_services_id_fkey", services,
		func(r keyRow) (string, bool) { return r.ServiceID, true }, filestore.Cascade, nil)
	filestore.References(t.Signals, "pending_signals_dest_id_fkey", channels,
		func(r signalRow) (string, bool) { return r.DestID, true }, filestore.Cascade, nil)
	filestore.References(t.Signals, "pending_signals_message_id_fkey", messages,
		func(r signalRow) (string, bool) { return filestore.Ref(r.MessageID) }, filestore.Cascade, nil)
	filestore.References(t.Signals, "pending_signals_service_id_fkey", services,
		func(r signalRow) (string, bool) { return r.ServiceID, true }, filestore.Cascade, nil)
}

// ServiceID returns, in r, the service of an integration key. The service
// store's search uses it.
func (t *DeltaTables) ServiceID(r filestore.Reader, keyID string) (string, bool) {
	row, ok := t.Keys.Get(r, keyID)
	return row.ServiceID, ok
}

// DeltaSignal is a signal that has no message yet.
type DeltaSignal struct {
	ID                int64
	ServiceID, DestID string
	CreatedAt         time.Time
}

// EachUnsent calls fn for each signal in r that has no message, oldest first,
// until it returns false.
func (t *DeltaTables) EachUnsent(r filestore.Reader, fn func(DeltaSignal) bool) {
	t.Unsent.Ascend(r, AllUnsent, func(row signalRow) bool {
		return fn(DeltaSignal{ID: row.ID, ServiceID: row.ServiceID, DestID: row.DestID, CreatedAt: row.CreatedAt})
	})
}

// SetSignalMessage records which message sends a signal.
func (t *DeltaTables) SetSignalMessage(tx *filestore.Tx, id int64, messageID string) error {
	row, ok := t.Signals.Get(tx, filestore.IntKey(id))
	if !ok {
		return nil
	}
	row.MessageID = &messageID
	return t.Signals.Put(tx, row)
}

// SignalParams returns, in r, the parameters of the signal that a message
// sends, as JSON.
func (t *DeltaTables) SignalParams(r filestore.Reader, messageID string) ([]byte, bool) {
	row, ok := t.SignalByMessage.Get(r, messageID)
	return row.Params, ok
}

// DeleteSignal deletes a signal.
func (t *DeltaTables) DeleteSignal(tx *filestore.Tx, id int64) error {
	_, err := t.Signals.Delete(tx, filestore.IntKey(id))
	return err
}

// DeltaLookups are the limits (config_limits) that the store enforces, -1 for
// none, and the job that follows a new signal. They run in the store's
// transaction, so they must do no I/O.
type DeltaLookups struct {
	KeysPerService           func(r filestore.Reader) int // integration_keys_per_service
	SignalsPerService        func(r filestore.Reader) int // pending_signals_per_service
	SignalsPerDestPerService func(r filestore.Reader) int // pending_signals_per_dest_per_service

	// NewSignal is trg_pending_signals_after_insert: it queues a job to send
	// the service's signals at once. Without it they wait for the engine's
	// periodic job, which sends those of every service.
	NewSignal func(tx *filestore.Tx, serviceID string) error
}

// NewDeltaStore returns an integration key store on the Delta backend.
func NewDeltaStore(db txn.DB, tables *DeltaTables, keys keyring.Keyring, reg *nfydest.Registry, ncStore *notificationchannel.Store, lookups DeltaLookups) *Store {
	if lookups.KeysPerService == nil || lookups.SignalsPerService == nil || lookups.SignalsPerDestPerService == nil {
		panic("integrationkey: NewDeltaStore needs its limits") // or they would not be enforced (F8)
	}
	return &Store{
		keys: keys, reg: reg, ncStore: ncStore,
		delta: &deltaKeys{db: db, DeltaTables: tables, DeltaLookups: lookups},
	}
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

// create adds a key, and for a universal one its empty config, in one
// transaction.
func (d *deltaKeys) create(ctx context.Context, tx txn.Tx, k *IntegrationKey) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row := newKeyRow(k)
		if err := d.Keys.Insert(s.Tx, row, "integration_keys_pkey"); err != nil {
			return err
		}
		// trg_enforce_integration_key_limit
		err := checkLimit(s.Tx, "integration_keys", "integration_keys_per_service_limit",
			d.KeysPerService(s.Tx), d.byService, row.ServiceID)
		if err != nil {
			return err
		}
		if k.Type != TypeUniversal {
			return nil
		}
		return d.putConfig(s.Tx, row.ID, &gadb.UIKConfigV1{})
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

// keyType returns sql.ErrNoRows for a missing key. A change follows, so this
// is a write, which reads the latest version: another pod may just have
// created the key (F29).
func (d *deltaKeys) keyType(ctx context.Context, tx txn.Tx, id uuid.UUID) (Type, error) {
	var typ Type
	err := deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.Keys.Get(s.Reader, id.String())
		if !ok {
			return sql.ErrNoRows
		}
		typ = Type(row.Type)
		return nil
	})
	return typ, err
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

// config returns sql.ErrNoRows for a key without one. In a transaction, no
// other change can come between this and the transaction's own, as with the
// Postgres query's FOR UPDATE (F4).
func (d *deltaKeys) config(ctx context.Context, tx txn.Tx, keyID uuid.UUID) (cfg gadb.UIKConfig, err error) {
	err = deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.configs.Get(s.Reader, keyID.String())
		if !ok {
			return sql.ErrNoRows
		}
		cfg = gadb.UIKConfig{}
		return cfg.Scan(row.Config)
	})
	return cfg, err
}

func (d *deltaKeys) setConfig(ctx context.Context, tx txn.Tx, keyID uuid.UUID, cfg *gadb.UIKConfigV1) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		return d.putConfig(s.Tx, keyID.String(), cfg)
	})
}

// putConfig sets a key's config, and keeps its tokens.
func (d *deltaKeys) putConfig(tx *filestore.Tx, keyID string, cfg *gadb.UIKConfigV1) error {
	data, err := json.Marshal(gadb.UIKConfig{Version: 1, V1: *cfg})
	if err != nil {
		return err
	}
	row, _ := d.configs.Get(tx, keyID)
	row.ID, row.Config = keyID, data
	return d.configs.Put(tx, row)
}

func (d *deltaKeys) deleteConfig(ctx context.Context, tx txn.Tx, keyID uuid.UUID) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		_, err := d.configs.Delete(s.Tx, keyID.String())
		return err
	})
}

// uikServiceID returns the service of a universal key, or sql.ErrNoRows if
// the token isn't one of the key's.
func (d *deltaKeys) uikServiceID(ctx context.Context, keyID, tokenID uuid.UUID) (string, error) {
	var serviceID string
	tok := tokenID.String()
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		cfg, ok := d.configs.Get(s.Reader, keyID.String())
		if !ok || (cfg.PrimaryToken == nil || *cfg.PrimaryToken != tok) && (cfg.SecondaryToken == nil || *cfg.SecondaryToken != tok) {
			return sql.ErrNoRows
		}
		key, ok := d.Keys.Get(s.Reader, cfg.ID)
		if !ok || Type(key.Type) != TypeUniversal {
			return sql.ErrNoRows
		}
		serviceID = key.ServiceID
		return nil
	})
	return serviceID, err
}

// tokenHints returns empty hints for a key without a config.
func (d *deltaKeys) tokenHints(ctx context.Context, tx txn.Tx, keyID uuid.UUID) (primary, secondary string, err error) {
	err = deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, _ := d.configs.Get(s.Reader, keyID.String())
		primary, secondary = text(row.PrimaryTokenHint), text(row.SecondaryTokenHint)
		return nil
	})
	return primary, secondary, err
}

// setToken sets the secondary token, or the primary one if there is none. If
// two pods give a key its first token at once, the one whose commit loses
// runs again, and sets the secondary (F4).
func (d *deltaKeys) setToken(ctx context.Context, tx txn.Tx, keyID, tokenID uuid.UUID, hint string) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.configs.Get(s.Reader, keyID.String())
		tok := tokenID.String()
		switch {
		case ok && row.PrimaryToken == nil:
			row.PrimaryToken, row.PrimaryTokenHint = &tok, &hint
		case ok && row.SecondaryToken == nil:
			row.SecondaryToken, row.SecondaryTokenHint = &tok, &hint
		default:
			return validation.NewGenericError("key not found, or already has primary and secondary tokens")
		}
		return d.configs.Put(s.Tx, row)
	})
}

// promoteSecondary makes the secondary token the primary one. It returns
// sql.ErrNoRows for a key without a config, and changes nothing without a
// secondary token.
func (d *deltaKeys) promoteSecondary(ctx context.Context, tx txn.Tx, keyID uuid.UUID) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.configs.Get(s.Reader, keyID.String())
		if !ok {
			return sql.ErrNoRows
		}
		if row.SecondaryToken == nil || row.SecondaryTokenHint == nil {
			return validation.NewGenericError("no secondary token to promote")
		}
		row.PrimaryToken, row.PrimaryTokenHint = row.SecondaryToken, row.SecondaryTokenHint
		row.SecondaryToken, row.SecondaryTokenHint = nil, nil
		return d.configs.Put(s.Tx, row)
	})
}

func (d *deltaKeys) deleteSecondary(ctx context.Context, tx txn.Tx, keyID uuid.UUID) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.configs.Get(s.Reader, keyID.String())
		if !ok || row.SecondaryToken == nil && row.SecondaryTokenHint == nil {
			return nil // nothing to change
		}
		row.SecondaryToken, row.SecondaryTokenHint = nil, nil
		return d.configs.Put(s.Tx, row)
	})
}

// insertSignal queues a signal.
func (d *deltaKeys) insertSignal(ctx context.Context, tx txn.Tx, p gadb.IntKeyInsertSignalMessageParams) error {
	if !json.Valid(p.Params) {
		return errors.New("signal params are not valid JSON") // the column is jsonb
	}
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		id, err := s.Tx.NextID("pending_signals_id_seq")
		if err != nil {
			return err
		}
		row := signalRow{ID: id, CreatedAt: s.Now, DestID: p.DestID.String(), ServiceID: p.ServiceID.String(), Params: p.Params}
		if err := d.Signals.Insert(s.Tx, row, "pending_signals_pkey"); err != nil {
			return err
		}
		// trg_enforce_signals_per_dest_per_service_limit and
		// trg_enforce_signals_per_service_limit, which count what is unsent
		err = checkLimit(s.Tx, "pending_signals", "pending_signals_per_dest_per_service_limit",
			d.SignalsPerDestPerService(s.Tx), d.Unsent, unsentByDest(row.ServiceID, row.DestID))
		if err != nil {
			return err
		}
		err = checkLimit(s.Tx, "pending_signals", "pending_signals_per_service_limit",
			d.SignalsPerService(s.Tx), d.Unsent, row.ServiceID)
		if err != nil {
			return err
		}
		if d.NewSignal == nil {
			return nil
		}
		return d.NewSignal(s.Tx, row.ServiceID)
	})
}

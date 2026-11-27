package notificationchannel

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/notification/nfydest"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
)

// ErrNotResolved is what MapDestToID returns on the Delta backend, inside a
// transaction, for a new destination that ResolveDests has not looked up.
var ErrNotResolved = errors.New("notificationchannel: a destination must be resolved with ResolveDests before the transaction that maps it")

// Row is a notification_channels row on the Delta backend.
type Row struct {
	ID        string    `parquet:"id"`
	Name      string    `parquet:"name"`
	Dest      []byte    `parquet:"dest"` // a gadb.DestV1 as destJSON writes it
	Type      string    `parquet:"type"`
	Value     string    `parquet:"value"`
	Meta      []byte    `parquet:"meta"`
	CreatedAt time.Time `parquet:"created_at,timestamp(microsecond:utc)"`
}

// DestV1 returns the row's destination.
func (r Row) DestV1() (gadb.DestV1, error) {
	var d gadb.DestV1
	if err := json.Unmarshal(r.Dest, &d); err != nil {
		return gadb.DestV1{}, fmt.Errorf("notification_channels %s: dest: %w", r.ID, err)
	}
	return d, nil
}

func (r Row) uuid() (uuid.UUID, error) {
	id, err := uuid.Parse(r.ID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("notification_channels %s: %w", r.ID, err)
	}
	return id, nil
}

func (r Row) channel() (Channel, error) {
	id, err := r.uuid()
	if err != nil {
		return Channel{}, err
	}
	dest, err := r.DestV1()
	if err != nil {
		return Channel{}, err
	}
	return Channel{ID: id, Name: r.Name, Dest: dest}, nil
}

// DuplicateRow is a notification_channel_duplicates row on the Delta backend:
// a channel that a Postgres migration merged into another. Nothing writes it
// on a new install.
type DuplicateRow struct {
	ID           int64     `parquet:"id"`
	OldID        string    `parquet:"old_id"`
	NewID        string    `parquet:"new_id"`
	OldCreatedAt time.Time `parquet:"old_created_at,timestamp(microsecond:utc)"`
}

// destJSON is a destination as the dest column holds it. Equal destinations
// have equal JSON, which is what jsonb equality gives in Postgres: object
// keys are sorted, and no arguments are an empty object.
func destJSON(d gadb.DestV1) []byte {
	if d.Args == nil {
		d.Args = map[string]string{}
	}
	data, err := json.Marshal(d)
	if err != nil {
		panic(err) // strings always encode
	}
	return data
}

// hashKey is an index key for data of any length or content. It also keeps
// webhook URLs, which can hold secrets, out of error messages (F20).
func hashKey(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// destKey is a destination's key in the notification_channels_dest_key
// index.
func destKey(d gadb.DestV1) string { return hashKey(destJSON(d)) }

// typeValueKey is the key of a type and value in the nc_unique_type_value
// index.
func typeValueKey(typ, value string) string { return typ + "/" + hashKey([]byte(value)) }

// typeValueToDest is nc_type_val_to_dest.
func typeValueToDest(typ, value string) (gadb.DestV1, error) {
	switch typ {
	case "SLACK":
		return gadb.NewDestV1("builtin-slack-channel", "slack_channel_id", value), nil
	case "WEBHOOK":
		return gadb.NewDestV1("builtin-webhook", "webhook_url", value), nil
	case "SLACK_USER_GROUP":
		parts := append(strings.Split(value, ":"), "")
		return gadb.NewDestV1("builtin-slack-usergroup", "slack_usergroup_id", parts[0], "slack_channel_id", parts[1]), nil
	}
	return gadb.DestV1{}, fmt.Errorf("notification_channels: unknown notification channel type: %s", typ)
}

// destToTypeValue is fn_nc_compat_set_type_val_on_insert. A destination type
// without a type of its own gets DEST and a value that is only unique.
func destToTypeValue(d gadb.DestV1) (typ, value string, err error) {
	arg := func(name string) string {
		v, ok := d.Args[name]
		if !ok {
			err = fmt.Errorf("notification_channels: value must not be null: a %s destination needs %s", d.Type, name)
		}
		return v
	}
	switch d.Type {
	case "builtin-slack-channel":
		typ, value = "SLACK", arg("slack_channel_id")
	case "builtin-slack-usergroup":
		typ, value = "SLACK_USER_GROUP", arg("slack_usergroup_id")+":"+arg("slack_channel_id")
	case "builtin-webhook":
		typ, value = "WEBHOOK", arg("webhook_url")
	default:
		typ, value = "DEST", uuid.NewString()
	}
	return typ, value, err
}

// DeltaTables are the notification channel store's tables on the Delta
// backend. Other packages declare their foreign keys to Channels, and read
// it. They write it only through Insert, which does what the Postgres
// triggers do, or delete from it.
type DeltaTables struct {
	Channels   *filestore.Table[Row]
	Duplicates *filestore.Table[DuplicateRow]

	byTypeValue *filestore.Unique[Row]
	byDest      *filestore.Unique[Row]
}

// DefineDeltaTables adds notification_channels, keyed by id
// (notification_channels_pkey), and notification_channel_duplicates, keyed by
// old_id (notification_channel_duplicates_pkey), to the Delta backend's
// schema.
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	t := &DeltaTables{
		Channels:   filestore.NewTable(s, "notification_channels", func(r Row) string { return r.ID }),
		Duplicates: filestore.NewTable(s, "notification_channel_duplicates", func(r DuplicateRow) string { return r.OldID }),
	}
	t.byTypeValue = t.Channels.Unique("nc_unique_type_value", func(r Row) (string, bool) {
		return typeValueKey(r.Type, r.Value), true
	})
	t.byDest = t.Channels.Unique("notification_channels_dest_key", func(r Row) (string, bool) {
		return hashKey(r.Dest), true
	})
	t.Duplicates.Unique("notification_channel_duplicates_id_key", func(r DuplicateRow) (string, bool) {
		return filestore.IntKey(r.ID), true
	})
	return t
}

// Insert adds a channel as the Postgres triggers do. Without a dest, it comes
// from the type and value; with one, the type and value come from it. (The
// triggers on updates have nothing to do: only a channel's name is updated.)
func (t *DeltaTables) Insert(tx *filestore.Tx, row Row) error {
	if row.CreatedAt.IsZero() {
		row.CreatedAt = tx.Now()
	}
	if len(row.Meta) == 0 {
		row.Meta = []byte("{}")
	}
	if len(row.Dest) == 0 {
		dest, err := typeValueToDest(row.Type, row.Value)
		if err != nil {
			return err
		}
		row.Dest = destJSON(dest)
	} else {
		dest, err := row.DestV1()
		if err != nil {
			return err
		}
		row.Dest = destJSON(dest)
		row.Type, row.Value, err = destToTypeValue(dest)
		if err != nil {
			return err
		}
	}
	return t.Channels.Insert(tx, row, "notification_channels_pkey")
}

// Dest returns, in r, a channel's destination, or sql.ErrNoRows.
func (t *DeltaTables) Dest(r filestore.Reader, id string) (gadb.DestV1, error) {
	row, ok := t.Channels.Get(r, id)
	if !ok {
		return gadb.DestV1{}, sql.ErrNoRows
	}
	return row.DestV1()
}

// FindByDest returns, in r, the channel with a destination.
func (t *DeltaTables) FindByDest(r filestore.Reader, dest gadb.DestV1) (Row, bool) {
	return t.byDest.Get(r, destKey(dest))
}

// NewDeltaStore returns a notification channel store on the Delta backend.
func NewDeltaStore(db txn.DB, tables *DeltaTables, reg *nfydest.Registry) *Store {
	return &Store{delta: &deltaChannels{db: db, reg: reg, DeltaTables: tables}}
}

type deltaChannels struct {
	db  txn.DB
	reg *nfydest.Registry
	*DeltaTables
}

// resolvedDests are the names of destinations that were validated, or why they
// could not be, by destKey. They travel in the context, through the stores that
// call MapDestToID in a transaction.
type resolvedDests map[string]resolvedDest

type resolvedDest struct {
	name string
	err  error
}

type resolvedDestsKey struct{}

func (d *deltaChannels) resolveDests(ctx context.Context, dests []gadb.DestV1) context.Context {
	resolved, _ := ctx.Value(resolvedDestsKey{}).(resolvedDests)
	resolved = maps.Clone(resolved)
	if resolved == nil {
		resolved = make(resolvedDests, len(dests))
	}
	for _, dest := range dests {
		key := destKey(dest)
		if _, ok := resolved[key]; ok {
			continue
		}
		name, err := destName(ctx, d.reg, dest)
		resolved[key] = resolvedDest{name, err}
	}
	return context.WithValue(ctx, resolvedDestsKey{}, resolved)
}

// mapDestToID looks a destination up before its own transaction. In a
// caller's transaction it can't, so ResolveDests must have done it (F3). Two
// pods mapping a new destination at once get the same channel: the one whose
// commit loses runs again and finds it (F4).
func (d *deltaChannels) mapDestToID(ctx context.Context, tx txn.Tx, dest gadb.DestV1) (uuid.UUID, error) {
	resolved, _ := ctx.Value(resolvedDestsKey{}).(resolvedDests)
	r, ok := resolved[destKey(dest)]
	if !ok && tx == nil {
		r.name, r.err = destName(ctx, d.reg, dest)
		ok = true
	}
	if r.err != nil {
		return uuid.Nil, r.err
	}

	newID := uuid.New()
	var id uuid.UUID
	err := deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) (err error) {
		row, exists := d.FindByDest(s.Reader, dest)
		if !exists && !ok {
			return ErrNotResolved
		}
		if !exists {
			id = newID
			return d.Insert(s.Tx, Row{ID: newID.String(), Dest: destJSON(dest), Name: r.name})
		}
		if id, err = row.uuid(); err != nil || !ok || row.Name == r.name {
			return err
		}
		row.Name = r.name
		return d.Channels.Put(s.Tx, row)
	})
	if err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

func (d *deltaChannels) lookupDestID(ctx context.Context, tx txn.Tx, dest gadb.DestV1) (uuid.UUID, error) {
	var id uuid.UUID
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) (err error) {
		row, ok := d.FindByDest(s.Reader, dest)
		if !ok {
			return sql.ErrNoRows
		}
		id, err = row.uuid()
		return err
	})
	return id, err
}

func (d *deltaChannels) findDest(ctx context.Context, tx txn.Tx, id uuid.UUID) (gadb.DestV1, error) {
	var dest gadb.DestV1
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) (err error) {
		dest, err = d.Dest(s.Reader, id.String())
		return err
	})
	return dest, err
}

func (d *deltaChannels) findOne(ctx context.Context, id uuid.UUID) (*Channel, error) {
	var c Channel
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) (err error) {
		row, ok := d.Channels.Get(s.Reader, id.String())
		if !ok {
			return sql.ErrNoRows
		}
		c, err = row.channel()
		return err
	})
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// findMany returns each channel once, like `where id = any($1)`.
func (d *deltaChannels) findMany(ctx context.Context, ids []uuid.UUID) ([]Channel, error) {
	var channels []Channel
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		channels = make([]Channel, 0, len(ids))
		seen := make(map[uuid.UUID]bool, len(ids))
		for _, id := range ids {
			if seen[id] {
				continue
			}
			seen[id] = true
			row, ok := d.Channels.Get(s.Reader, id.String())
			if !ok {
				continue
			}
			c, err := row.channel()
			if err != nil {
				return err
			}
			channels = append(channels, c)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return channels, nil
}

func (d *deltaChannels) deleteMany(ctx context.Context, tx txn.Tx, ids []uuid.UUID) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		for _, id := range ids {
			if _, err := d.Channels.Delete(s.Tx, id.String()); err != nil {
				return err
			}
		}
		return nil
	})
}

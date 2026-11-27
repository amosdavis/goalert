package contactmethod

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/notification/nfydest"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/validation"
)

// ErrNotNormalized is what Create returns on the Delta backend, inside a
// transaction, for a contact method that Normalize has not validated.
// Validating a destination can call the provider's API, which a transaction
// must not do (F3), so the caller normalizes before the transaction starts
// and passes the result in.
var ErrNotNormalized = errors.New("contactmethod: a contact method must be normalized before the transaction that creates it")

// Row is a user_contact_methods row on the Delta backend.
type Row struct {
	ID                  string     `parquet:"id"`
	UserID              string     `parquet:"user_id"`
	Name                string     `parquet:"name"`
	Dest                []byte     `parquet:"dest"` // a gadb.DestV1 as destJSON writes it
	Type                string     `parquet:"type"`
	Value               string     `parquet:"value"`
	Disabled            bool       `parquet:"disabled"`
	Pending             bool       `parquet:"pending"` // true by default in Postgres
	Private             bool       `parquet:"private"`
	EnableStatusUpdates bool       `parquet:"enable_status_updates"`
	LastTestVerifyAt    *time.Time `parquet:"last_test_verify_at,optional,timestamp(microsecond:utc)"`
	Metadata            []byte     `parquet:"metadata,optional"`
}

// DestV1 returns the row's destination.
func (r Row) DestV1() (gadb.DestV1, error) {
	var d gadb.DestV1
	if err := json.Unmarshal(r.Dest, &d); err != nil {
		return gadb.DestV1{}, fmt.Errorf("user_contact_methods %s: dest: %w", r.ID, err)
	}
	return d, nil
}

// setDisabled is an UPDATE OF disabled. Enabling a pending contact method
// also verifies it (trg_cm_set_not_pending_on_verify).
func (r *Row) setDisabled(disabled bool) {
	r.Disabled = disabled
	if !disabled {
		r.Pending = false
	}
}

func (r Row) contactMethod() (ContactMethod, error) {
	id, err := uuid.Parse(r.ID)
	if err != nil {
		return ContactMethod{}, fmt.Errorf("user_contact_methods %s: %w", r.ID, err)
	}
	dest, err := r.DestV1()
	if err != nil {
		return ContactMethod{}, err
	}
	c := ContactMethod{
		ID:            id,
		Name:          r.Name,
		Dest:          dest,
		Disabled:      r.Disabled,
		UserID:        r.UserID,
		Pending:       r.Pending,
		StatusUpdates: r.EnableStatusUpdates,
		Private:       r.Private,
	}
	if r.LastTestVerifyAt != nil {
		c.lastTestVerifyAt = sql.NullTime{Time: *r.LastTestVerifyAt, Valid: true}
	}
	return c, nil
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
// phone numbers and addresses out of error messages (F20).
func hashKey(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// destKey is a destination's key in the user_contact_methods_dest_key index.
func destKey(d gadb.DestV1) string { return hashKey(destJSON(d)) }

// typeValueKey is the key of a type and value in the
// user_contact_methods_type_value_key index.
func typeValueKey(typ, value string) string { return typ + "/" + hashKey([]byte(value)) }

// destArgsKey is the key of a destination's arguments, whatever its type.
func destArgsKey(args map[string]string) string {
	return hashKey(destJSON(gadb.DestV1{Args: args}))
}

// compatTypes are the destination types with a value of their own in
// enum_user_contact_method_type, and the argument in their value column.
var compatTypes = []struct{ typ, destType, arg string }{
	{"EMAIL", "builtin-smtp-email", "email_address"},
	{"VOICE", "builtin-twilio-voice", "phone_number"},
	{"SMS", "builtin-twilio-sms", "phone_number"},
	{"WEBHOOK", "builtin-webhook", "webhook_url"},
	{"SLACK_DM", "builtin-slack-dm", "slack_user_id"},
}

// typeValueToDest is cm_type_val_to_dest.
func typeValueToDest(typ, value string) (gadb.DestV1, error) {
	for _, c := range compatTypes {
		if c.typ == typ {
			return gadb.NewDestV1(c.destType, c.arg, value), nil
		}
	}
	return gadb.DestV1{}, fmt.Errorf("user_contact_methods: unknown contact method type: %s", typ)
}

// destToTypeValue is fn_cm_compat_set_type_val_on_insert. A destination type
// without a type of its own gets DEST and a value that is only unique.
func destToTypeValue(d gadb.DestV1) (typ, value string, err error) {
	for _, c := range compatTypes {
		if c.destType != d.Type {
			continue
		}
		value, ok := d.Args[c.arg]
		if !ok {
			return "", "", fmt.Errorf("user_contact_methods: value must not be null: a %s destination needs %s", d.Type, c.arg)
		}
		return c.typ, value, nil
	}
	return "DEST", uuid.NewString(), nil
}

// DeltaTables are the contact method store's tables on the Delta backend.
// Other packages declare their foreign keys to Methods, and read it. They
// write it only through the methods below, which do what the Postgres
// triggers do, or delete from it.
type DeltaTables struct {
	Methods *filestore.Table[Row]
	ByUser  *filestore.Index[Row] // by user_id

	byType      *filestore.Index[Row]
	byDestArgs  *filestore.Index[Row]
	byTypeValue *filestore.Unique[Row]
	byDest      *filestore.Unique[Row]
}

// DefineDeltaTables adds user_contact_methods to the Delta backend's schema,
// keyed by id (user_contact_methods_pkey).
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	t := &DeltaTables{Methods: filestore.NewTable(s, "user_contact_methods", func(r Row) string { return r.ID })}
	t.ByUser = t.Methods.Index("idx_contact_method_users", func(r Row) []string { return []string{r.UserID} })
	t.byType = t.Methods.Index("user_contact_methods_type", func(r Row) []string { return []string{r.Type} })
	t.byDestArgs = t.Methods.Index("user_contact_methods_dest_args", func(r Row) []string {
		d, err := r.DestV1()
		if err != nil {
			return nil
		}
		return []string{destArgsKey(d.Args)}
	})
	// A duplicate breaks both, and the first is the one Postgres reports,
	// which the user's message depends on.
	t.byTypeValue = t.Methods.Unique("user_contact_methods_type_value_key", func(r Row) (string, bool) {
		return typeValueKey(r.Type, r.Value), true
	})
	t.byDest = t.Methods.Unique("user_contact_methods_dest_key", func(r Row) (string, bool) {
		return hashKey(r.Dest), true
	})
	return t
}

// DeclareDeltaForeignKeys declares this package's foreign keys to other
// packages' tables.
func DeclareDeltaForeignKeys[U any](t *DeltaTables, users *filestore.Table[U]) {
	filestore.References(t.Methods, "user_contact_methods_user_id_fkey", users,
		func(r Row) (string, bool) { return r.UserID, true }, filestore.Cascade, nil)
}

// Insert adds a contact method as the Postgres triggers do. Without a dest,
// it comes from the type and value; with one, the type and value come from
// it. An empty ID gets a new one. max is the contact_methods_per_user limit,
// or -1 for none.
func (t *DeltaTables) Insert(tx *filestore.Tx, row Row, max int) error {
	if row.ID == "" {
		row.ID = uuid.NewString()
	}
	var dest gadb.DestV1
	var err error
	if len(row.Dest) == 0 {
		dest, err = typeValueToDest(row.Type, row.Value)
	} else {
		dest, err = row.DestV1()
	}
	if err != nil {
		return err
	}
	row.Dest = destJSON(dest)
	row.Type, row.Value, err = destToTypeValue(dest)
	if err != nil {
		return err
	}
	if err := t.Methods.Insert(tx, row, "user_contact_methods_pkey"); err != nil {
		return err
	}
	if max == -1 || t.ByUser.Count(tx, row.UserID) <= max {
		return nil
	}
	// Take the row back, in case the caller goes on with a transaction that
	// Postgres would have aborted.
	if _, err := t.Methods.Delete(tx, row.ID); err != nil {
		return err
	}
	return &filestore.CheckError{
		Table: "user_contact_methods", Constraint: "contact_methods_per_user_limit",
		Message: "limit exceeded", HintText: fmt.Sprintf("max=%d", max),
	}
}

// SetDisabled enables or disables a contact method and reports whether it
// exists. Enabling one also verifies it.
func (t *DeltaTables) SetDisabled(tx *filestore.Tx, id string, disabled bool) (bool, error) {
	row, ok := t.Methods.Get(tx, id)
	if !ok {
		return false, nil
	}
	row.setDisabled(disabled)
	return true, t.Methods.Put(tx, row)
}

// SetLastTestVerify records that a test or verification message is sent now,
// unless the last one was sent within minAge, and reports whether it did.
func (t *DeltaTables) SetLastTestVerify(tx *filestore.Tx, id string, minAge time.Duration) (bool, error) {
	row, ok := t.Methods.Get(tx, id)
	if !ok {
		return false, nil
	}
	now := tx.Now()
	if last := row.LastTestVerifyAt; last != nil && !last.Add(minAge).Before(now) {
		return false, nil
	}
	row.LastTestVerifyAt = &now
	return true, t.Methods.Put(tx, row)
}

// Owner returns, in r, the id of the user a contact method belongs to.
func (t *DeltaTables) Owner(r filestore.Reader, id string) (userID string, ok bool) {
	row, ok := t.Methods.Get(r, id)
	return row.UserID, ok
}

// Dest returns, in r, a contact method's destination, or sql.ErrNoRows.
func (t *DeltaTables) Dest(r filestore.Reader, id string) (gadb.DestV1, error) {
	row, ok := t.Methods.Get(r, id)
	if !ok {
		return gadb.DestV1{}, sql.ErrNoRows
	}
	return row.DestV1()
}

// FindByDest returns, in r, the contact method with a destination.
func (t *DeltaTables) FindByDest(r filestore.Reader, dest gadb.DestV1) (Row, bool) {
	return t.byDest.Get(r, destKey(dest))
}

// FindByTypeValue returns, in r, the contact method with a type and value.
func (t *DeltaTables) FindByTypeValue(r filestore.Reader, typ, value string) (Row, bool) {
	return t.byTypeValue.Get(r, typeValueKey(typ, value))
}

// EachOfType calls fn, in r, for each contact method of a type, until it returns
// false.
func (t *DeltaTables) EachOfType(r filestore.Reader, typ string, fn func(Row) bool) {
	t.byType.Ascend(r, typ, fn)
}

// FindByDestArgs returns, in r, the contact methods whose destination has
// exactly these arguments and, unless it is empty, this type. The user search
// uses it.
func (t *DeltaTables) FindByDestArgs(r filestore.Reader, destType string, args map[string]string) ([]Row, error) {
	var rows []Row
	var err error
	t.byDestArgs.Ascend(r, destArgsKey(args), func(row Row) bool {
		var d gadb.DestV1
		if d, err = row.DestV1(); err != nil {
			return false
		}
		if destType == "" || d.Type == destType {
			rows = append(rows, row)
		}
		return true
	})
	return rows, err
}

// NewDeltaStore returns a contact method store on the Delta backend.
// maxPerUser is the contact_methods_per_user limit in r, or -1 for none.
func NewDeltaStore(db txn.DB, tables *DeltaTables, reg *nfydest.Registry, maxPerUser func(r filestore.Reader) int) *Store {
	return &Store{delta: &deltaMethods{db: db, reg: reg, DeltaTables: tables, maxPerUser: maxPerUser}}
}

type deltaMethods struct {
	db  txn.DB
	reg *nfydest.Registry
	*DeltaTables
	maxPerUser func(r filestore.Reader) int
}

// metadataByDest returns a contact method's metadata, and the time it was
// read.
func (d *deltaMethods) metadataByDest(ctx context.Context, tx txn.Tx, dest gadb.DestV1) (data []byte, now time.Time, err error) {
	err = deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.FindByDest(s.Reader, dest)
		if !ok {
			return sql.ErrNoRows
		}
		data, now = row.Metadata, s.Now
		if len(data) == 0 {
			data = []byte("{}")
		}
		return nil
	})
	return data, now, err
}

// setCarrierV1 sets the carrier in a contact method's metadata, if there is
// one with the destination. (The Postgres query stores nothing while the
// metadata is null, as it always is, and a time that can't be read back; this
// stores the carrier.)
func (d *deltaMethods) setCarrierV1(ctx context.Context, tx txn.Tx, dest gadb.DestV1, m *Metadata) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.FindByDest(s.Reader, dest)
		if !ok {
			return nil
		}
		var meta map[string]json.RawMessage
		if len(row.Metadata) > 0 {
			if err := json.Unmarshal(row.Metadata, &meta); err != nil {
				return fmt.Errorf("user_contact_methods %s: metadata: %w", row.ID, err)
			}
		}
		if meta == nil {
			meta = make(map[string]json.RawMessage)
		}
		carrier := m.CarrierV1
		carrier.UpdatedAt = s.Now
		data, err := json.Marshal(carrier)
		if err != nil {
			return err
		}
		meta["CarrierV1"] = data
		row.Metadata, err = json.Marshal(meta)
		if err != nil {
			return err
		}
		return d.Methods.Put(s.Tx, row)
	})
}

func (d *deltaMethods) findDest(ctx context.Context, tx txn.Tx, id uuid.UUID) (gadb.DestV1, error) {
	var dest gadb.DestV1
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) (err error) {
		dest, err = d.Dest(s.Reader, id.String())
		return err
	})
	return dest, err
}

// setDisabledByDest enables or disables the contact method with a
// destination and returns its id, or sql.ErrNoRows.
func (d *deltaMethods) setDisabledByDest(ctx context.Context, tx txn.Tx, dest gadb.DestV1, disabled bool) (uuid.UUID, error) {
	var id uuid.UUID
	err := deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.FindByDest(s.Reader, dest)
		if !ok {
			return sql.ErrNoRows
		}
		var err error
		if id, err = uuid.Parse(row.ID); err != nil {
			return err
		}
		_, err = d.SetDisabled(s.Tx, row.ID, disabled)
		return err
	})
	return id, err
}

// create validates the destination before its own transaction. In a caller's
// transaction it can't, so Normalize must have done it (F3).
func (d *deltaMethods) create(ctx context.Context, tx txn.Tx, c *ContactMethod) (*ContactMethod, error) {
	var n *ContactMethod
	var err error
	if tx == nil {
		n, err = c.Normalize(ctx, d.reg)
	} else {
		n, err = c.normalizeFields(ctx, d.reg)
		if err == nil && n.validDest != destKey(n.Dest) {
			err = ErrNotNormalized
		}
	}
	if err != nil {
		return nil, err
	}
	userID, err := uuid.Parse(n.UserID)
	if err != nil {
		return nil, validation.NewFieldError("UserID", "must be a valid UUID: "+err.Error())
	}
	row := Row{
		ID:                  n.ID.String(),
		UserID:              userID.String(),
		Name:                n.Name,
		Dest:                destJSON(n.Dest),
		Disabled:            n.Disabled,
		Pending:             true,
		Private:             n.Private,
		EnableStatusUpdates: n.StatusUpdates,
	}
	err = deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		return d.Insert(s.Tx, row, d.maxPerUser(s.Reader))
	})
	if err != nil {
		return nil, err
	}
	return n, nil
}

// delete removes contact methods. A user who is not an admin must own every
// one of them.
func (d *deltaMethods) delete(ctx context.Context, tx txn.Tx, ids []uuid.UUID) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		if !permission.Admin(ctx) {
			for _, id := range ids {
				owner, ok := d.Owner(s.Reader, id.String())
				if !ok {
					continue
				}
				if err := permission.LimitCheckAny(ctx, permission.MatchUser(owner)); err != nil {
					return err
				}
			}
		}
		for _, id := range ids {
			if _, err := d.Methods.Delete(s.Tx, id.String()); err != nil {
				return err
			}
		}
		return nil
	})
}

func (d *deltaMethods) findOne(ctx context.Context, tx txn.Tx, id uuid.UUID) (*ContactMethod, error) {
	var c ContactMethod
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) (err error) {
		row, ok := d.Methods.Get(s.Reader, id.String())
		if !ok {
			return sql.ErrNoRows
		}
		c, err = row.contactMethod()
		return err
	})
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// update sets a contact method's name, status updates and flags. Its
// destination can't change, so it is not validated again: that can call the
// provider's API, and update runs in a caller's transaction (F3).
func (d *deltaMethods) update(ctx context.Context, tx txn.Tx, c *ContactMethod) error {
	n, err := c.normalizeFields(ctx, d.reg)
	if err != nil {
		return err
	}
	info, err := d.reg.TypeInfo(ctx, n.Dest.Type)
	if err != nil {
		return err
	}
	if !info.Enabled {
		return nfydest.ErrNotEnabled
	}
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.Methods.Get(s.Reader, n.ID.String())
		if !ok {
			return sql.ErrNoRows
		}
		dest, err := row.DestV1()
		if err != nil {
			return err
		}
		if !n.Dest.Equal(dest) {
			return validation.NewFieldError("Dest", "cannot update destination of contact method")
		}
		if n.UserID != row.UserID {
			return validation.NewFieldError("UserID", "cannot update owner of contact method")
		}
		if !permission.Admin(ctx) {
			if err := permission.LimitCheckAny(ctx, permission.MatchUser(row.UserID)); err != nil {
				return err
			}
		}
		row.Name, row.EnableStatusUpdates, row.Private = n.Name, n.StatusUpdates, n.Private
		row.setDisabled(n.Disabled)
		return d.Methods.Put(s.Tx, row)
	})
}

// findMany returns each contact method once, like `where id = any($1)`.
func (d *deltaMethods) findMany(ctx context.Context, tx txn.Tx, ids []uuid.UUID) ([]ContactMethod, error) {
	var cms []ContactMethod
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		cms = make([]ContactMethod, 0, len(ids))
		seen := make(map[uuid.UUID]bool, len(ids))
		for _, id := range ids {
			if seen[id] {
				continue
			}
			seen[id] = true
			row, ok := d.Methods.Get(s.Reader, id.String())
			if !ok {
				continue
			}
			c, err := row.contactMethod()
			if err != nil {
				return err
			}
			cms = append(cms, c)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return cms, nil
}

// findAll returns the user's contact methods, and how many private ones of
// another user were left out.
func (d *deltaMethods) findAll(ctx context.Context, tx txn.Tx, userID uuid.UUID) ([]ContactMethod, int, error) {
	all, err := d.findByUsers(ctx, tx, userID)
	if err != nil {
		return nil, 0, err
	}
	me := permission.UserNullUUID(ctx).UUID.String()
	result := make([]ContactMethod, 0, len(all))
	for _, c := range all {
		if !c.Private || c.UserID == me {
			result = append(result, c)
		}
	}
	return result, len(all) - len(result), nil
}

// findByUsers returns the users' contact methods, each once.
func (d *deltaMethods) findByUsers(ctx context.Context, tx txn.Tx, userIDs ...uuid.UUID) ([]ContactMethod, error) {
	var cms []ContactMethod
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		cms = []ContactMethod{}
		seen := make(map[uuid.UUID]bool, len(userIDs))
		var err error
		for _, id := range userIDs {
			if seen[id] {
				continue
			}
			seen[id] = true
			d.ByUser.Ascend(s.Reader, id.String(), func(row Row) bool {
				var c ContactMethod
				if c, err = row.contactMethod(); err != nil {
					return false
				}
				cms = append(cms, c)
				return true
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return cms, nil
}

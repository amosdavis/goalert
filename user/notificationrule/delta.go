package notificationrule

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
)

// Row is a user_notification_rules row on the Delta backend.
type Row struct {
	ID              string     `parquet:"id"`
	UserID          string     `parquet:"user_id"`
	ContactMethodID string     `parquet:"contact_method_id"`
	DelayMinutes    int32      `parquet:"delay_minutes"`
	CreatedAt       *time.Time `parquet:"created_at,optional,timestamp(microsecond:utc)"`
}

func (r Row) rule() (NotificationRule, error) {
	cmID, err := uuid.Parse(r.ContactMethodID)
	if err != nil {
		return NotificationRule{}, fmt.Errorf("user_notification_rules %s: contact_method_id: %w", r.ID, err)
	}
	return NotificationRule{ID: r.ID, UserID: r.UserID, DelayMinutes: int(r.DelayMinutes), ContactMethodID: cmID}, nil
}

// DeltaTables are the notification rule store's tables on the Delta backend.
// Other packages read Rules, and write it only through Insert, which does
// what the Postgres triggers do, or delete from it.
type DeltaTables struct {
	Rules  *filestore.Table[Row]
	ByUser *filestore.Index[Row] // by user_id
}

// DefineDeltaTables adds user_notification_rules to the Delta backend's
// schema, keyed by id (user_notification_rules_pkey).
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	t := &DeltaTables{Rules: filestore.NewTable(s, "user_notification_rules", func(r Row) string { return r.ID })}
	t.ByUser = t.Rules.Index("idx_notification_rule_users", func(r Row) []string { return []string{r.UserID} })
	t.Rules.Unique("user_notification_rules_contact_method_id_delay_minutes_key", func(r Row) (string, bool) {
		return fmt.Sprintf("%s/%d", r.ContactMethodID, r.DelayMinutes), true
	})
	return t
}

// DeclareDeltaForeignKeys declares this package's foreign keys to other
// packages' tables.
func DeclareDeltaForeignKeys[M, U any](t *DeltaTables, contactMethods *filestore.Table[M], users *filestore.Table[U]) {
	filestore.References(t.Rules, "user_notification_rules_contact_method_id_fkey", contactMethods,
		func(r Row) (string, bool) { return r.ContactMethodID, true }, filestore.Cascade, nil)
	filestore.References(t.Rules, "user_notification_rules_user_id_fkey", users,
		func(r Row) (string, bool) { return r.UserID, true }, filestore.Cascade, nil)
}

// OwnerFunc returns, in r, the id of the user a contact method belongs to.
// The contact methods package provides it.
type OwnerFunc func(r filestore.Reader, contactMethodID string) (userID string, ok bool)

// Insert adds a notification rule as the Postgres triggers do: its contact
// method must be the same user's, and the user's rules can't exceed max, the
// notification_rules_per_user limit, or -1 for none. An empty ID gets a new
// one. (trg_notification_rule_same_user also runs on updates, but rules are
// never updated.)
func (t *DeltaTables) Insert(tx *filestore.Tx, row Row, owner OwnerFunc, max int) error {
	if row.ID == "" {
		row.ID = uuid.NewString()
	}
	if row.CreatedAt == nil {
		now := tx.Now()
		row.CreatedAt = &now
	}
	if userID, ok := owner(tx, row.ContactMethodID); ok && userID != row.UserID {
		return &filestore.CheckError{
			Table: "user_notification_rules", Constraint: "notification_rule_user_id_match",
			Message: "wrong user_id",
		}
	}
	if err := t.Rules.Insert(tx, row, "user_notification_rules_pkey"); err != nil {
		return err
	}
	if max == -1 || t.ByUser.Count(tx, row.UserID) <= max {
		return nil
	}
	// Take the row back, in case the caller goes on with a transaction that
	// Postgres would have aborted.
	if _, err := t.Rules.Delete(tx, row.ID); err != nil {
		return err
	}
	return &filestore.CheckError{
		Table: "user_notification_rules", Constraint: "notification_rules_per_user_limit",
		Message: "limit exceeded", HintText: fmt.Sprintf("max=%d", max),
	}
}

// NewDeltaStore returns a notification rule store on the Delta backend. owner
// finds a contact method's user, and maxPerUser is the
// notification_rules_per_user limit in r, or -1 for none.
func NewDeltaStore(db txn.DB, tables *DeltaTables, owner OwnerFunc, maxPerUser func(r filestore.Reader) int) *Store {
	return &Store{delta: &deltaRules{db: db, DeltaTables: tables, owner: owner, maxPerUser: maxPerUser}}
}

type deltaRules struct {
	db txn.DB
	*DeltaTables
	owner      OwnerFunc
	maxPerUser func(r filestore.Reader) int
}

// key is a validated UUID as it is stored, and as Postgres returns it.
func key(id string) string { return strings.ToLower(id) }

func (d *deltaRules) insert(ctx context.Context, tx txn.Tx, n *NotificationRule) error {
	row := Row{
		ID:              n.ID,
		UserID:          key(n.UserID),
		ContactMethodID: n.ContactMethodID.String(),
		DelayMinutes:    int32(n.DelayMinutes),
	}
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		return d.Insert(s.Tx, row, d.owner, d.maxPerUser(s.Reader))
	})
}

// delete removes notification rules. A user who is not an admin must own
// every one of them.
func (d *deltaRules) delete(ctx context.Context, tx txn.Tx, ids []string) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		if !permission.Admin(ctx) {
			for _, id := range ids {
				row, ok := d.Rules.Get(s.Reader, key(id))
				if !ok {
					continue
				}
				if err := permission.LimitCheckAny(ctx, permission.MatchUser(row.UserID)); err != nil {
					return err
				}
			}
		}
		for _, id := range ids {
			if _, err := d.Rules.Delete(s.Tx, key(id)); err != nil {
				return err
			}
		}
		return nil
	})
}

// findByUsers returns the users' rules, each once, a user's oldest first.
func (d *deltaRules) findByUsers(ctx context.Context, userIDs ...string) ([]NotificationRule, error) {
	var rules []NotificationRule
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		rules = []NotificationRule{}
		seen := make(map[string]bool, len(userIDs))
		for _, id := range userIDs {
			id = key(id)
			if seen[id] {
				continue
			}
			seen[id] = true
			var rows []Row
			d.ByUser.Ascend(s.Reader, id, func(r Row) bool {
				rows = append(rows, r)
				return true
			})
			slices.SortStableFunc(rows, func(a, b Row) int { return cmp.Compare(a.created(), b.created()) })
			for _, r := range rows {
				n, err := r.rule()
				if err != nil {
					return err
				}
				rules = append(rules, n)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rules, nil
}

func (r Row) created() int64 {
	if r.CreatedAt == nil {
		return 0
	}
	return r.CreatedAt.UnixMicro()
}

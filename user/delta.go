package user

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
)

// userRow is a users row on the Delta backend.
type userRow struct {
	ID              string  `parquet:"id"`
	Name            string  `parquet:"name"`
	Email           string  `parquet:"email"`
	AvatarURL       string  `parquet:"avatar_url"`
	Bio             string  `parquet:"bio"`
	Role            string  `parquet:"role"`
	AlertStatusCMID *string `parquet:"alert_status_log_contact_method_id,optional"`
}

// DeltaTables are the user store's tables on the Delta backend. Other
// packages declare their foreign keys to Users. They write users with
// PutUser, and auth subjects as deltasubjects.go says.
type DeltaTables struct {
	Users        *filestore.Table[userRow]
	AuthSubjects *filestore.Table[authSubjectRow]

	AuthSubjectsByID            *filestore.Unique[authSubjectRow] // by filestore.IntKey(id)
	AuthSubjectsByUser          *filestore.Index[authSubjectRow]
	AuthSubjectsByContactMethod *filestore.Index[authSubjectRow]

	byEmail *filestore.Index[userRow] // by lower(email)

	cmOwner ContactMethodOwnerFunc // set by DeclareDeltaForeignKeys
}

// maxEmailKey is longer than any email that Search can be asked for, so
// longer ones need no index entry.
const maxEmailKey = 1024

// DefineDeltaTables adds the user store's tables to the Delta backend's
// schema: users, keyed by id (goalert_user_pkey), and auth_subjects, keyed by
// provider and subject (auth_subjects_pkey).
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	t := &DeltaTables{}
	t.Users = filestore.NewTable(s, "users", func(u userRow) string { return u.ID })
	t.byEmail = t.Users.Index("users_lower_email", func(u userRow) []string {
		k := strings.ToLower(u.Email)
		if k == "" || len(k) > maxEmailKey {
			return nil
		}
		return []string{k}
	})
	t.defineAuthSubjects(s)
	return t
}

// ContactMethodOwnerFunc returns, in r, the user a contact method belongs to
// (user_contact_methods.user_id), and false if there is no such contact
// method.
type ContactMethodOwnerFunc func(r filestore.Reader, contactMethodID string) (userID string, ok bool)

// DeclareDeltaForeignKeys declares the user store's foreign keys to
// user_contact_methods. owner is for the rule of the trigger
// trg_enforce_status_update_same_user (PutUser).
func DeclareDeltaForeignKeys[C any](t *DeltaTables, contactMethods *filestore.Table[C], owner ContactMethodOwnerFunc) {
	t.cmOwner = owner
	filestore.References(t.Users, "users_alert_status_log_contact_method_id_fkey", contactMethods,
		func(u userRow) (string, bool) { return filestore.Ref(u.AlertStatusCMID) }, filestore.SetNull,
		func(u userRow) userRow { u.AlertStatusCMID = nil; return u })
	filestore.References(t.AuthSubjects, "auth_subjects_cm_id_fkey", contactMethods,
		func(a authSubjectRow) (string, bool) { return filestore.Ref(a.CMID) }, filestore.Cascade, nil)
}

// PutUser inserts or replaces a user, with the role's default if it has none,
// and only with an id and a role their Postgres types allow. Like the trigger
// trg_enforce_status_update_same_user, it refuses an alert status contact
// method of another user. One that doesn't exist is left to the foreign key,
// as there.
func (t *DeltaTables) PutUser(tx *filestore.Tx, u userRow) error {
	id, err := userKey(u.ID)
	if err != nil {
		return err
	}
	u.ID = id
	switch permission.Role(u.Role) {
	case permission.RoleUnknown, permission.RoleUser, permission.RoleAdmin:
	case "":
		u.Role = string(permission.RoleUnknown)
	default:
		return fmt.Errorf("user: invalid role %q", u.Role)
	}
	if u.AlertStatusCMID != nil {
		if t.cmOwner == nil {
			return errors.New("user: an alert status contact method was set, but DeclareDeltaForeignKeys was not called")
		}
		if owner, ok := t.cmOwner(tx, *u.AlertStatusCMID); ok && owner != u.ID {
			return &filestore.CheckError{Table: "users", Constraint: "alert_status_user_id_match", Message: "wrong user_id"}
		}
	}
	return t.Users.Put(tx, u)
}

// UpdateProfile sets the user's name and email, each only if it isn't empty,
// as logging in with an auth provider does. A user that doesn't exist is left
// alone.
func (t *DeltaTables) UpdateProfile(tx *filestore.Tx, userID, name, email string) error {
	userID, err := userKey(userID)
	if err != nil {
		return err
	}
	row, ok := t.Users.Get(tx, userID)
	if !ok {
		return nil
	}
	old := row
	if name != "" {
		row.Name = name
	}
	if email != "" {
		row.Email = email
	}
	if row == old {
		return nil // no need for a commit on every login
	}
	return t.PutUser(tx, row)
}

// FavoriteFunc reports, in r, whether userID has targetUserID as a
// favorite. The favorites package provides it, since it can't be imported
// here.
type FavoriteFunc func(r filestore.Reader, userID, targetUserID string) bool

// DestUsersFunc returns, in r, the IDs of the users who have a contact method
// (user_contact_methods, disabled or not) whose destination has exactly the
// given args and, unless it is empty, the given type.
type DestUsersFunc func(r filestore.Reader, destType string, destArgs map[string]string) []string

// RotationCleanupFunc takes a user who is about to be deleted out of every
// rotation (rotation_participants and rotation_state), as the Postgres
// store's removeUserFromRotation does: the participants after them move up,
// and the participant on call stays the same, or if it was them, becomes the
// one after them.
type RotationCleanupFunc func(tx *filestore.Tx, userID string) error

// OnCallFuncs are the lookups OnCallOverview needs, all in r.
type OnCallFuncs struct {
	// Steps returns the IDs of the escalation policy steps the user is on
	// call for now (ep_step_on_call_users rows without an end time).
	Steps func(r filestore.Reader, userID string) []string
	// Step returns an escalation policy step (escalation_policy_steps), with
	// its policy's name (escalation_policies), and false if there is none.
	Step func(r filestore.Reader, stepID string) (OnCallStep, bool)
	// Services returns the services that use an escalation policy (services).
	Services func(r filestore.Reader, policyID string) []OnCallService
}

// OnCallStep is an escalation policy step, for OnCallFuncs.
type OnCallStep struct {
	PolicyID, PolicyName string
	StepNumber           int
}

// OnCallService is a service, for OnCallFuncs.
type OnCallService struct{ ID, Name string }

// A DeltaOption gives the user store a part that belongs to another
// package's tables. A method that needs one it wasn't given returns
// deltatxn.ErrNotPorted.
type DeltaOption func(*deltaUsers)

// WithContactMethods lets Search look for users by destination.
func WithContactMethods(fn DestUsersFunc) DeltaOption {
	return func(d *deltaUsers) { d.destUsers = fn }
}

// WithRotations lets DeleteManyTx delete users.
func WithRotations(fn RotationCleanupFunc) DeltaOption {
	return func(d *deltaUsers) { d.leaveRotations = fn }
}

// WithOnCall lets OnCallOverview find what a user is on call for. Every func
// must be set.
func WithOnCall(fns OnCallFuncs) DeltaOption {
	return func(d *deltaUsers) {
		if fns.Steps != nil && fns.Step != nil && fns.Services != nil {
			d.onCall = &fns
		}
	}
}

// NewDeltaStore returns a user store on the Delta backend. isFavorite may be
// nil, in which case no user is a favorite.
func NewDeltaStore(db txn.DB, tables *DeltaTables, isFavorite FavoriteFunc, opts ...DeltaOption) *Store {
	if isFavorite == nil {
		isFavorite = func(filestore.Reader, string, string) bool { return false }
	}
	d := &deltaUsers{db: db, DeltaTables: tables, isFavorite: isFavorite}
	for _, o := range opts {
		o(d)
	}
	return &Store{delta: d}
}

type deltaUsers struct {
	db txn.DB
	*DeltaTables
	isFavorite FavoriteFunc

	destUsers      DestUsersFunc
	leaveRotations RotationCleanupFunc
	onCall         *OnCallFuncs
}

// userKey returns the key of the user with the given id. Keys are in the form
// that Postgres's uuid type gives an id, so a lookup is not case-sensitive,
// and like there, an id that isn't a UUID is an error.
func userKey(id string) (string, error) {
	u, err := uuid.Parse(id)
	if err != nil {
		return "", fmt.Errorf("user id %q: %w", id, err)
	}
	return u.String(), nil
}

// userKeys returns the keys of the users with the given ids, each once, like
// `where id = any($1)`.
func userKeys(ids []string) ([]string, error) {
	keys := make([]string, 0, len(ids))
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		k, err := userKey(id)
		if err != nil {
			return nil, err
		}
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	return keys, nil
}

// user returns the row as a User, without the alert status contact method,
// which the Postgres store doesn't read either.
func (r userRow) user(isFavorite bool) User {
	return User{ID: r.ID, Name: r.Name, Email: r.Email, AvatarURL: r.AvatarURL, Role: permission.Role(r.Role), isUserFavorite: isFavorite}
}

// insert adds a user, without an alert status contact method, like the
// Postgres store.
func (d *deltaUsers) insert(ctx context.Context, tx txn.Tx, u *User) error {
	id, err := userKey(u.ID)
	if err != nil {
		return err
	}
	row := userRow{ID: id, Name: u.Name, Email: u.Email, AvatarURL: u.AvatarURL, Role: string(u.Role)}
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		return d.Users.Insert(s.Tx, row, "goalert_user_pkey")
	})
}

// update sets the name and email, like the Postgres store's update.
func (d *deltaUsers) update(ctx context.Context, tx txn.Tx, u *User) error {
	id, err := userKey(u.ID)
	if err != nil {
		return err
	}
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.Users.Get(s.Reader, id)
		if !ok {
			return nil // an UPDATE of no rows
		}
		row.Name, row.Email = u.Name, u.Email
		return d.PutUser(s.Tx, row)
	})
}

func (d *deltaUsers) setRole(ctx context.Context, tx txn.Tx, id string, role permission.Role) error {
	id, err := userKey(id)
	if err != nil {
		return err
	}
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.Users.Get(s.Reader, id)
		if !ok {
			return nil
		}
		row.Role = string(role)
		return d.PutUser(s.Tx, row)
	})
}

// deleteMany deletes users, and with them, through the foreign keys, all that
// is theirs. They leave their rotations first, which keeps the others'
// positions and who is on call right (F5); what a cascade alone would do to
// rotations is not the same.
//
// The Postgres store locks the rotation tables and retries foreign key
// errors, because someone may add the user somewhere at the same time. Here
// the transaction runs again if that happens (F4).
func (d *deltaUsers) deleteMany(ctx context.Context, tx txn.Tx, ids []string) error {
	if d.leaveRotations == nil {
		return deltatxn.ErrNotPorted
	}
	ids, err := userKeys(ids)
	if err != nil {
		return err
	}
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		for _, id := range ids {
			if err := d.leaveRotations(s.Tx, id); err != nil {
				return fmt.Errorf("remove user '%s' from rotations: %w", id, err)
			}
			if _, err := d.Users.Delete(s.Tx, id); err != nil {
				return fmt.Errorf("delete user row: %w", err)
			}
		}
		return nil
	})
}

// findOne returns sql.ErrNoRows for a missing user, as the Postgres store
// does. With forUpdate, the favorite flag is false, as there.
func (d *deltaUsers) findOne(ctx context.Context, tx txn.Tx, id string, forUpdate bool) (*User, error) {
	id, err := userKey(id)
	if err != nil {
		return nil, err
	}
	var u User
	err = deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.Users.Get(s.Reader, id)
		if !ok {
			return sql.ErrNoRows
		}
		fav := !forUpdate && d.isFavorite(s.Reader, permission.UserID(ctx), id)
		u = row.user(fav)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &u, nil
}

// findMany returns the users in the order of ids, and an empty slice, not
// nil, for none.
func (d *deltaUsers) findMany(ctx context.Context, ids []string) ([]User, error) {
	ids, err := userKeys(ids)
	if err != nil {
		return nil, err
	}
	var users []User
	err = deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		users = make([]User, 0, len(ids))
		me := permission.UserID(ctx)
		for _, id := range ids {
			if row, ok := d.Users.Get(s.Reader, id); ok {
				users = append(users, row.user(d.isFavorite(s.Reader, me, id)))
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return users, nil
}

// deltaChecker is an ExistanceChecker for one view of the users. Keeping the
// view costs nothing, unlike a set of all user IDs, and it isn't a minute old,
// as the Postgres store's may be.
type deltaChecker struct {
	users *filestore.Table[userRow]
	view  filestore.Reader
}

func (c deltaChecker) UserExistsString(id string) bool {
	u, err := uuid.Parse(id)
	return err == nil && c.UserExistsUUID(u)
}

func (c deltaChecker) UserExistsUUID(id uuid.UUID) bool {
	_, ok := c.users.Get(c.view, id.String())
	return ok
}

func (d *deltaUsers) userExists(ctx context.Context) (ExistanceChecker, error) {
	c := deltaChecker{users: d.Users}
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		c.view = s.Reader // not in a transaction, so an immutable view
		return nil
	})
	if err != nil {
		return nil, err
	}
	return c, nil
}

// onCallOverview joins the tables like the GQLUserOnCallOverview query.
func (d *deltaUsers) onCallOverview(ctx context.Context, userID uuid.UUID) ([]gadb.GQLUserOnCallOverviewRow, error) {
	if d.onCall == nil {
		return nil, deltatxn.ErrNotPorted
	}
	var rows []gadb.GQLUserOnCallOverviewRow
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		rows = nil
		for _, stepID := range d.onCall.Steps(s.Reader, userID.String()) {
			step, ok := d.onCall.Step(s.Reader, stepID)
			if !ok {
				continue
			}
			policyID, err := uuid.Parse(step.PolicyID)
			if err != nil {
				return fmt.Errorf("escalation policy id %q: %w", step.PolicyID, err)
			}
			for _, svc := range d.onCall.Services(s.Reader, step.PolicyID) {
				serviceID, err := uuid.Parse(svc.ID)
				if err != nil {
					return fmt.Errorf("service id %q: %w", svc.ID, err)
				}
				rows = append(rows, gadb.GQLUserOnCallOverviewRow{
					ServiceID: serviceID, ServiceName: svc.Name,
					PolicyID: policyID, PolicyName: step.PolicyName, StepNumber: int32(step.StepNumber),
				})
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

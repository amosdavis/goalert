package user

import (
	"context"
	"database/sql"

	"github.com/target/goalert/filestore"
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
// packages declare their foreign keys to Users.
type DeltaTables struct {
	Users *filestore.Table[userRow]
}

// DefineDeltaTables adds the user store's tables to the Delta backend's
// schema: users, keyed by id (goalert_user_pkey).
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	return &DeltaTables{
		Users: filestore.NewTable(s, "users", func(u userRow) string { return u.ID }),
	}
}

// FavoriteFunc reports, in r, whether userID has targetUserID as a
// favorite. The favorites package provides it, since it can't be imported
// here.
type FavoriteFunc func(r filestore.Reader, userID, targetUserID string) bool

// NewDeltaStore returns a user store on the Delta backend. isFavorite may be
// nil, in which case no user is a favorite.
func NewDeltaStore(db txn.DB, tables *DeltaTables, isFavorite FavoriteFunc) *Store {
	if isFavorite == nil {
		isFavorite = func(filestore.Reader, string, string) bool { return false }
	}
	return &Store{delta: &deltaUsers{db: db, DeltaTables: tables, isFavorite: isFavorite}}
}

type deltaUsers struct {
	db txn.DB
	*DeltaTables
	isFavorite FavoriteFunc
}

func toUserRow(u *User) userRow {
	r := userRow{ID: u.ID, Name: u.Name, Email: u.Email, AvatarURL: u.AvatarURL, Role: string(u.Role)}
	if u.AlertStatusCMID != "" {
		r.AlertStatusCMID = &u.AlertStatusCMID
	}
	return r
}

func (r userRow) user(isFavorite bool) User {
	u := User{ID: r.ID, Name: r.Name, Email: r.Email, AvatarURL: r.AvatarURL, Role: permission.Role(r.Role), isUserFavorite: isFavorite}
	if r.AlertStatusCMID != nil {
		u.AlertStatusCMID = *r.AlertStatusCMID
	}
	return u
}

func (d *deltaUsers) insert(ctx context.Context, tx txn.Tx, u *User) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		return d.Users.Insert(s.Tx, toUserRow(u), "goalert_user_pkey")
	})
}

// update sets the name and email, like the Postgres store's update.
func (d *deltaUsers) update(ctx context.Context, tx txn.Tx, u *User) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.Users.Get(s.Reader, u.ID)
		if !ok {
			return nil // an UPDATE of no rows
		}
		row.Name, row.Email = u.Name, u.Email
		return d.Users.Put(s.Tx, row)
	})
}

func (d *deltaUsers) setRole(ctx context.Context, tx txn.Tx, id string, role permission.Role) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.Users.Get(s.Reader, id)
		if !ok {
			return nil
		}
		row.Role = string(role)
		return d.Users.Put(s.Tx, row)
	})
}

// findOne returns sql.ErrNoRows for a missing user, as the Postgres store
// does. With forUpdate, the favorite flag is false, as there.
func (d *deltaUsers) findOne(ctx context.Context, tx txn.Tx, id string, forUpdate bool) (*User, error) {
	var u User
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
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

func (d *deltaUsers) findMany(ctx context.Context, ids []string) ([]User, error) {
	var users []User
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		users = nil
		me := permission.UserID(ctx)
		seen := make(map[string]bool, len(ids))
		for _, id := range ids {
			if seen[id] {
				continue // like `where id = any($1)`
			}
			seen[id] = true
			if row, ok := d.Users.Get(s.Reader, id); ok {
				users = append(users, row.user(d.isFavorite(s.Reader, me, id)))
			}
		}
		return nil
	})
	return users, err
}

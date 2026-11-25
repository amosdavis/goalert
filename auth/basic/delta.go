package basic

import (
	"context"
	"database/sql"
	"strings"

	"github.com/target/goalert/auth/authdelta"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/user"
)

// subjectProvider is the provider ID of basic auth's auth subjects.
const subjectProvider = "basic"

// basicRow is an auth_basic_users row on the Delta backend.
type basicRow struct {
	UserID       string `parquet:"user_id"`
	ID           int64  `parquet:"id"`
	Username     string `parquet:"username"`
	PasswordHash string `parquet:"password_hash"`
}

// DeltaTables are the basic auth store's tables on the Delta backend.
type DeltaTables struct {
	users      *filestore.Table[basicRow]
	byUsername *filestore.Unique[basicRow]
}

// DefineDeltaTables adds the basic auth store's tables to the Delta backend's
// schema: auth_basic_users, keyed by user_id (auth_basic_users_pkey).
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	t := &DeltaTables{}
	t.users = filestore.NewTable(s, "auth_basic_users", func(r basicRow) string { return r.UserID })
	t.users.Unique("auth_basic_users_uniq_id", func(r basicRow) (string, bool) { return filestore.IntKey(r.ID), true })
	t.byUsername = t.users.Unique("auth_basic_users_username_key", func(r basicRow) (string, bool) { return r.Username, true })
	return t
}

// DeclareDeltaForeignKeys declares the basic auth store's foreign key to
// users.
func DeclareDeltaForeignKeys[U any](t *DeltaTables, users *filestore.Table[U]) {
	filestore.References(t.users, "auth_basic_users_user_id_fkey", users,
		func(r basicRow) (string, bool) { return r.UserID, true }, filestore.Cascade, nil)
}

// SetSubjectFunc adds an auth subject (auth_subjects), or gives the one that
// exists to a.UserID. user.DeltaTables.SetAuthSubject is one.
type SetSubjectFunc func(tx *filestore.Tx, a user.AuthSubject) error

// NewDeltaStore returns a basic auth store on the Delta backend.
func NewDeltaStore(db txn.DB, tables *DeltaTables, setSubject SetSubjectFunc) *Store {
	return &Store{delta: &deltaStore{
		db: db, DeltaTables: tables, setSubject: setSubject,
		// Logging in is rare and slow already, so it always asks the log: a
		// changed password counts on every pod at once (F29).
		check: authdelta.NewChecker(db, 0),
	}}
}

type deltaStore struct {
	db txn.DB
	*DeltaTables
	setSubject SetSubjectFunc
	check      *authdelta.Checker
}

// insert adds a user's username and password, and the auth subject, as the
// trigger trg_insert_basic_user does.
func (d *deltaStore) insert(ctx context.Context, tx txn.Tx, userID, username, hash string) error {
	userID = strings.ToLower(userID)
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		id, err := s.Tx.NextID("auth_basic_users_id_seq")
		if err != nil {
			return err
		}
		err = d.users.Insert(s.Tx, basicRow{UserID: userID, ID: id, Username: username, PasswordHash: hash}, "auth_basic_users_pkey")
		if err != nil {
			return err
		}
		return d.setSubject(s.Tx, user.AuthSubject{ProviderID: subjectProvider, SubjectID: username, UserID: userID})
	})
}

// update sets a user's password hash and returns how many rows it changed.
func (d *deltaStore) update(ctx context.Context, tx txn.Tx, userID, hash string) (n int64, err error) {
	userID = strings.ToLower(userID)
	err = deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		n = 0
		row, ok := d.users.Get(s.Reader, userID)
		if !ok {
			return nil
		}
		n = 1
		row.PasswordHash = hash
		return d.users.Put(s.Tx, row)
	})
	return n, err
}

// byUsernameHash returns the user with the username and their password hash,
// or sql.ErrNoRows.
func (d *deltaStore) byUsernameHash(ctx context.Context, username string) (userID, hash string, err error) {
	err = d.check.Read(ctx, func(_ txn.Tx, s deltatxn.Scope) error {
		row, ok := d.byUsername.Get(s.Reader, username)
		if !ok {
			return sql.ErrNoRows
		}
		userID, hash = row.UserID, row.PasswordHash
		return nil
	})
	return userID, hash, err
}

// hash returns a user's password hash, or sql.ErrNoRows.
func (d *deltaStore) hash(ctx context.Context, userID string) (hash string, err error) {
	userID = strings.ToLower(userID)
	err = d.check.Read(ctx, func(_ txn.Tx, s deltatxn.Scope) error {
		row, ok := d.users.Get(s.Reader, userID)
		if !ok {
			return sql.ErrNoRows
		}
		hash = row.PasswordHash
		return nil
	})
	return hash, err
}

package auth

import (
	"cmp"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/auth/authdelta"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/user"
	"github.com/target/goalert/validation/validate"
)

// sessionRow is an auth_user_sessions row on the Delta backend.
type sessionRow struct {
	ID         string    `parquet:"id"`
	UserID     *string   `parquet:"user_id,optional"`
	UserAgent  string    `parquet:"user_agent"`
	CreatedAt  time.Time `parquet:"created_at,timestamp(microsecond:utc)"`
	LastAccess time.Time `parquet:"last_access_at,timestamp(microsecond:utc)"`
}

// accessInterval is how old the recorded last access of a session must be
// before another one is recorded.
const accessInterval = time.Minute

// sessionKey is a session's key in its table. Keys end up in error messages,
// and session IDs must not (F20).
func sessionKey(id string) string {
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:])
}

// DeltaTables are the auth handler's tables on the Delta backend.
type DeltaTables struct {
	Sessions *filestore.Table[sessionRow]

	byUser *filestore.Index[sessionRow]
}

// DefineDeltaTables adds the auth handler's tables to the Delta backend's
// schema: auth_user_sessions, keyed by a hash of its id
// (auth_user_sessions_pkey).
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	t := &DeltaTables{}
	t.Sessions = filestore.NewTable(s, "auth_user_sessions", func(r sessionRow) string { return sessionKey(r.ID) })
	t.byUser = t.Sessions.Index("auth_user_sessions_user_id", func(r sessionRow) []string {
		if r.UserID == nil {
			return nil
		}
		return []string{*r.UserID}
	})
	return t
}

// DeclareDeltaForeignKeys declares the auth handler's foreign key to users.
func DeclareDeltaForeignKeys[U any](t *DeltaTables, users *filestore.Table[U]) {
	filestore.References(t.Sessions, "auth_user_sessions_user_id_fkey", users,
		func(r sessionRow) (string, bool) { return filestore.Ref(r.UserID) }, filestore.Cascade, nil)
}

// DeleteOld deletes up to limit sessions last used before a time, and returns
// how many. It is the cleanup manager's CleanupMgrDeleteOldSessions.
func (t *DeltaTables) DeleteOld(tx *filestore.Tx, before time.Time, limit int) (int, error) {
	var old []string
	t.Sessions.Ascend(tx, func(r sessionRow) bool {
		if len(old) >= limit {
			return false
		}
		if r.LastAccess.Before(before) {
			old = append(old, sessionKey(r.ID))
		}
		return true
	})
	for _, key := range old {
		if _, err := t.Sessions.Delete(tx, key); err != nil {
			return 0, err
		}
	}
	return len(old), nil
}

// DeltaUsers are what the auth handler needs of users and auth_subjects.
// user.DeltaTables has them all.
type DeltaUsers struct {
	// AuthSubjectUserID returns the user an auth subject belongs to, and
	// false if there is no such subject.
	AuthSubjectUserID func(r filestore.Reader, providerID, subjectID string) (userID string, ok bool)
	// InsertAuthSubject adds an auth subject. One that exists already is an
	// error.
	InsertAuthSubject func(tx *filestore.Tx, a user.AuthSubject) error
	// UpdateProfile sets the user's name and email, each only if it isn't
	// empty.
	UpdateProfile func(tx *filestore.Tx, userID, name, email string) error
}

// NewDeltaHandler creates a new Handler on the Delta backend; cfg.UserStore
// must be too. Its Shutdown writes the last accesses that aren't written yet.
func NewDeltaHandler(db txn.DB, tables *DeltaTables, cfg HandlerConfig, users DeltaUsers) *Handler {
	d := &deltaAuth{
		db: db, DeltaTables: tables, users: users, userStore: cfg.UserStore,
		check: authdelta.NewChecker(db, authdelta.MaxStale),
		// Logging in is rare, so it always asks the log.
		loginCheck: authdelta.NewChecker(db, 0),
	}
	d.access = authdelta.NewRecorder(authdelta.RecordInterval, "session access", d.writeAccess)
	return &Handler{providers: make(map[string]IdentityProvider), cfg: cfg, txdb: db, delta: d}
}

// Shutdown writes the last accesses of sessions that aren't written yet.
func (h *Handler) Shutdown(ctx context.Context) error {
	if h == nil {
		return nil
	}
	return h.delta.access.Flush(ctx)
}

type deltaAuth struct {
	db txn.DB
	*DeltaTables
	users      DeltaUsers
	userStore  *user.Store
	check      *authdelta.Checker
	loginCheck *authdelta.Checker
	access     *authdelta.Recorder[time.Time] // by session key
}

// start adds a session.
func (d *deltaAuth) start(ctx context.Context, id uuid.UUID, userAgent, userID string) error {
	uid, err := validate.ParseUUID("UserID", userID)
	if err != nil {
		return err
	}
	owner := uid.String()
	return deltatxn.Write(ctx, d.db, nil, func(s deltatxn.Scope) error {
		return d.Sessions.Insert(s.Tx, sessionRow{
			ID: id.String(), UserID: &owner, UserAgent: validText(userAgent), CreatedAt: s.Now, LastAccess: s.Now,
		}, "auth_user_sessions_pkey")
	})
}

// validText returns s as text a data file can hold (F35).
func validText(s string) string { return strings.ToValidUTF8(s, "�") }

// session returns the user of a session and their role, or sql.ErrNoRows.
//
// A session that another pod just started works at once. One that was ended
// is refused once this pod knows, which is within authdelta.MaxStale (F29).
//
// The access is recorded, but not committed: a pod writes the accesses of all
// sessions together, at most once a minute.
func (d *deltaAuth) session(ctx context.Context, id uuid.UUID) (userID uuid.UUID, role permission.Role, err error) {
	key := sessionKey(id.String())
	var last, now time.Time
	err = d.check.Read(ctx, func(tx txn.Tx, s deltatxn.Scope) error {
		row, ok := d.Sessions.Get(s.Reader, key)
		if !ok || row.UserID == nil {
			return sql.ErrNoRows
		}
		u, err := d.findUser(ctx, tx, *row.UserID)
		if err != nil {
			return err
		}
		if userID, err = uuid.Parse(u.ID); err != nil {
			return err
		}
		role, last, now = u.Role, row.LastAccess, s.Now
		return nil
	})
	if err != nil {
		return uuid.UUID{}, "", err
	}

	if at, ok := d.access.Pending(key); ok {
		last = at
	}
	if now.Sub(last) > accessInterval {
		d.access.Add(key, now)
	}
	return userID, role, nil
}

// findUser returns a user, or sql.ErrNoRows.
func (d *deltaAuth) findUser(ctx context.Context, tx txn.Tx, id string) (u *user.User, err error) {
	permission.SudoContext(ctx, func(ctx context.Context) {
		u, err = d.userStore.FindOneTx(ctx, tx, id, true) // without the favorite flag
	})
	return u, err
}

// writeAccess writes last accesses, unless another pod has written a later
// one. A session that was ended meanwhile stays ended (F29).
func (d *deltaAuth) writeAccess(ctx context.Context, at map[string]time.Time) error {
	return deltatxn.Write(ctx, d.db, nil, func(s deltatxn.Scope) error {
		for _, key := range slices.Sorted(maps.Keys(at)) {
			row, ok := d.Sessions.Get(s.Reader, key)
			if !ok || !row.LastAccess.Before(at[key]) {
				continue
			}
			row.LastAccess = at[key]
			if err := d.Sessions.Put(s.Tx, row); err != nil {
				return err
			}
		}
		return nil
	})
}

func (d *deltaAuth) setUserAgent(ctx context.Context, id uuid.UUID, userAgent string) error {
	return deltatxn.Write(ctx, d.db, nil, func(s deltatxn.Scope) error {
		row, ok := d.Sessions.Get(s.Reader, sessionKey(id.String()))
		if !ok {
			return nil
		}
		row.UserAgent = validText(userAgent)
		return d.Sessions.Put(s.Tx, row)
	})
}

// end ends sessions, whoever's they are.
func (d *deltaAuth) end(ctx context.Context, tx txn.Tx, ids []uuid.UUID) error {
	return d.endIf(ctx, tx, ids, func(sessionRow) bool { return true })
}

// endOwn ends those of the sessions that are the user's.
func (d *deltaAuth) endOwn(ctx context.Context, tx txn.Tx, userID uuid.NullUUID, ids []uuid.UUID) error {
	if !userID.Valid {
		return nil
	}
	owner := userID.UUID.String()
	return d.endIf(ctx, tx, ids, func(r sessionRow) bool { return r.UserID != nil && *r.UserID == owner })
}

func (d *deltaAuth) endIf(ctx context.Context, tx txn.Tx, ids []uuid.UUID, match func(sessionRow) bool) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		for _, id := range ids {
			key := sessionKey(id.String())
			row, ok := d.Sessions.Get(s.Reader, key)
			if !ok || !match(row) {
				continue
			}
			if _, err := d.Sessions.Delete(s.Tx, key); err != nil {
				return err
			}
		}
		return nil
	})
}

// endAllOther ends the user's sessions, except the current one. That includes
// sessions started on other pods that this one hasn't seen yet, since it runs
// on the latest version.
func (d *deltaAuth) endAllOther(ctx context.Context, tx txn.Tx, userID uuid.NullUUID, current *permission.SourceInfo) error {
	if !userID.Valid {
		return nil
	}
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		var keys []string
		d.byUser.Ascend(s.Reader, userID.UUID.String(), func(r sessionRow) bool {
			if current == nil || r.ID != current.ID {
				keys = append(keys, sessionKey(r.ID))
			}
			return true
		})
		for _, key := range keys {
			if _, err := d.Sessions.Delete(s.Tx, key); err != nil {
				return err
			}
		}
		return nil
	})
}

// userSessions returns the user's sessions, oldest first.
func (d *deltaAuth) userSessions(ctx context.Context, userID string) ([]UserSession, error) {
	uid, err := validate.ParseUUID("UserID", userID)
	if err != nil {
		return nil, err
	}
	var rows []sessionRow
	err = deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		rows = nil
		d.byUser.Ascend(s.Reader, uid.String(), func(r sessionRow) bool {
			rows = append(rows, r)
			return true
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(rows, func(a, b sessionRow) int {
		return cmp.Or(a.CreatedAt.Compare(b.CreatedAt), cmp.Compare(a.ID, b.ID))
	})
	var sessions []UserSession
	for _, r := range rows {
		sessions = append(sessions, UserSession{
			ID: r.ID, UserAgent: r.UserAgent, UserID: userID,
			CreatedAt: r.CreatedAt.Truncate(time.Minute), LastAccessAt: r.LastAccess.Truncate(time.Minute),
		})
	}
	return sessions, nil
}

// subjectUser returns the user an auth subject belongs to, or sql.ErrNoRows.
func (d *deltaAuth) subjectUser(ctx context.Context, providerID, subjectID string) (userID string, err error) {
	err = d.loginCheck.Read(ctx, func(_ txn.Tx, s deltatxn.Scope) error {
		var ok bool
		userID, ok = d.users.AuthSubjectUserID(s.Reader, providerID, subjectID)
		if !ok {
			return sql.ErrNoRows
		}
		return nil
	})
	return userID, err
}

func (d *deltaAuth) addSubject(ctx context.Context, tx txn.Tx, a user.AuthSubject) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error { return d.users.InsertAuthSubject(s.Tx, a) })
}

func (d *deltaAuth) updateProfile(ctx context.Context, userID, name, email string) error {
	return deltatxn.Write(ctx, d.db, nil, func(s deltatxn.Scope) error {
		return d.users.UpdateProfile(s.Tx, userID, name, email)
	})
}

package user

import (
	"cmp"
	"context"
	"slices"
	"strings"

	"github.com/target/goalert/filestore"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
)

// authSubjectRow is an auth_subjects row on the Delta backend.
type authSubjectRow struct {
	ID         int64   `parquet:"id"`
	ProviderID string  `parquet:"provider_id"`
	SubjectID  string  `parquet:"subject_id"`
	UserID     string  `parquet:"user_id"`
	CMID       *string `parquet:"cm_id,optional"`
}

func (r authSubjectRow) subject() AuthSubject {
	return AuthSubject{ProviderID: r.ProviderID, SubjectID: r.SubjectID, UserID: r.UserID}
}

var providerEscaper = strings.NewReplacer("%", "%25", "/", "%2F")

// AuthSubjectKey is the key of an auth_subjects row. Provider and subject IDs
// may both contain "/", so it is escaped in the provider, and the first one
// in a key ends the provider. The subject is left as it is, which keeps a
// provider's rows together and ordered by subject.
func AuthSubjectKey(providerID, subjectID string) string {
	return providerEscaper.Replace(providerID) + "/" + subjectID
}

// defineAuthSubjects defines auth_subjects. Its foreign key to
// user_contact_methods is in DeclareDeltaForeignKeys.
func (t *DeltaTables) defineAuthSubjects(s *filestore.Schema) {
	t.AuthSubjects = filestore.NewTable(s, "auth_subjects", func(r authSubjectRow) string {
		return AuthSubjectKey(r.ProviderID, r.SubjectID)
	})
	t.AuthSubjectsByID = t.AuthSubjects.Unique("auth_subjects_uniq_id", func(r authSubjectRow) (string, bool) {
		return filestore.IntKey(r.ID), true
	})
	t.AuthSubjectsByUser = t.AuthSubjects.Index("auth_subjects_user_id", func(r authSubjectRow) []string {
		return []string{r.UserID}
	})
	t.AuthSubjectsByContactMethod = t.AuthSubjects.Index("auth_subjects_cm_id", func(r authSubjectRow) []string {
		if r.CMID == nil {
			return nil
		}
		return []string{*r.CMID}
	})
	filestore.References(t.AuthSubjects, "auth_subjects_user_id_fkey", t.Users,
		func(r authSubjectRow) (string, bool) { return r.UserID, true }, filestore.Cascade, nil)
}

// AscendProvider calls fn for a provider's auth subjects whose subject ID is
// after afterSubjectID, ordered by subject ID, until fn returns false.
func (t *DeltaTables) AscendProvider(r filestore.Reader, providerID, afterSubjectID string, fn func(authSubjectRow) bool) {
	t.AuthSubjects.AscendFrom(r, AuthSubjectKey(providerID, afterSubjectID), func(row authSubjectRow) bool {
		if row.ProviderID != providerID {
			return false
		}
		return row.SubjectID <= afterSubjectID || fn(row)
	})
}

// AuthSubjectUserID returns the user an auth subject belongs to, and false if
// there is no such subject.
func (t *DeltaTables) AuthSubjectUserID(r filestore.Reader, providerID, subjectID string) (userID string, ok bool) {
	row, ok := t.AuthSubjects.Get(r, AuthSubjectKey(providerID, subjectID))
	return row.UserID, ok
}

// InsertAuthSubject adds an auth subject. One that exists already is an error
// (auth_subjects_pkey).
func (t *DeltaTables) InsertAuthSubject(tx *filestore.Tx, a AuthSubject) error {
	userID, err := userKey(a.UserID)
	if err != nil {
		return err
	}
	id, err := tx.NextID("auth_subjects_id_seq")
	if err != nil {
		return err
	}
	return t.AuthSubjects.Insert(tx, authSubjectRow{ID: id, ProviderID: a.ProviderID, SubjectID: a.SubjectID, UserID: userID}, "auth_subjects_pkey")
}

// SetAuthSubject adds an auth subject, or gives the one that exists to
// a.UserID (ON CONFLICT (provider_id, subject_id) DO UPDATE SET user_id). Its
// contact method stays, as on Postgres.
func (t *DeltaTables) SetAuthSubject(tx *filestore.Tx, a AuthSubject) error {
	row, ok := t.AuthSubjects.Get(tx, AuthSubjectKey(a.ProviderID, a.SubjectID))
	if !ok {
		return t.InsertAuthSubject(tx, a)
	}
	userID, err := userKey(a.UserID)
	if err != nil {
		return err
	}
	if row.UserID == userID {
		return nil
	}
	row.UserID = userID
	return t.AuthSubjects.Put(tx, row)
}

func (d *deltaUsers) setAuthSubject(ctx context.Context, a AuthSubject) error {
	return deltatxn.Write(ctx, d.db, nil, func(s deltatxn.Scope) error {
		return d.SetAuthSubject(s.Tx, a)
	})
}

// addAuthSubject adds an auth subject, or does nothing if it exists, even for
// another user (ON CONFLICT DO NOTHING).
func (d *deltaUsers) addAuthSubject(ctx context.Context, tx txn.Tx, a AuthSubject) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		if _, ok := d.AuthSubjectUserID(s.Reader, a.ProviderID, a.SubjectID); ok {
			return nil
		}
		return d.InsertAuthSubject(s.Tx, a)
	})
}

// deleteAuthSubject deletes an auth subject if it is the user's.
func (d *deltaUsers) deleteAuthSubject(ctx context.Context, tx txn.Tx, a AuthSubject) error {
	userID, err := userKey(a.UserID)
	if err != nil {
		return err
	}
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		if owner, ok := d.AuthSubjectUserID(s.Reader, a.ProviderID, a.SubjectID); !ok || owner != userID {
			return nil
		}
		_, err := d.AuthSubjects.Delete(s.Tx, AuthSubjectKey(a.ProviderID, a.SubjectID))
		return err
	})
}

// findOneBySubject returns nil if there is no such subject, as the Postgres
// store does.
func (d *deltaUsers) findOneBySubject(ctx context.Context, providerID, subjectID string) (*User, error) {
	var found *User
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		found = nil
		userID, ok := d.AuthSubjectUserID(s.Reader, providerID, subjectID)
		if !ok {
			return nil
		}
		row, ok := d.Users.Get(s.Reader, userID)
		if !ok {
			return nil
		}
		u := row.user(false)
		found = &u
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

func (d *deltaUsers) findSomeAuthSubjects(ctx context.Context, limit int, afterSubjectID, providerID string) ([]AuthSubject, error) {
	var found []AuthSubject
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		found = nil
		d.AscendProvider(s.Reader, providerID, afterSubjectID, func(row authSubjectRow) bool {
			found = append(found, row.subject())
			return len(found) < limit
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

// authSubjectsFunc calls fn for each auth subject of the provider and the
// users, either of which may be empty for all, oldest first. It reads them
// all before the first call, so that fn may take its time.
func (d *deltaUsers) authSubjectsFunc(ctx context.Context, providerID string, userIDs []string, fn func(AuthSubject) error) error {
	userIDs, err := userKeys(userIDs)
	if err != nil {
		return err
	}
	var rows []authSubjectRow
	err = deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		rows = nil
		add := func(row authSubjectRow) bool {
			if providerID == "" || row.ProviderID == providerID {
				rows = append(rows, row)
			}
			return true
		}
		switch {
		case len(userIDs) > 0:
			for _, id := range userIDs {
				d.AuthSubjectsByUser.Ascend(s.Reader, id, add)
			}
		case providerID != "":
			d.AscendProvider(s.Reader, providerID, "", add)
		default:
			d.AuthSubjects.Ascend(s.Reader, add)
		}
		return nil
	})
	if err != nil {
		return err
	}
	slices.SortFunc(rows, func(a, b authSubjectRow) int { return cmp.Compare(a.ID, b.ID) })
	for _, row := range rows {
		if err := fn(row.subject()); err != nil {
			return err
		}
	}
	return nil
}

// withoutAuthProviderFunc calls fn for each user without an auth subject of
// the provider. Like authSubjectsFunc, it reads them all before the first
// call.
func (d *deltaUsers) withoutAuthProviderFunc(ctx context.Context, providerID string, fn func(User) error) error {
	var users []User
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		users = nil
		has := make(map[string]bool)
		d.AscendProvider(s.Reader, providerID, "", func(row authSubjectRow) bool {
			has[row.UserID] = true
			return true
		})
		d.Users.Ascend(s.Reader, func(row userRow) bool {
			if !has[row.ID] {
				users = append(users, row.user(false))
			}
			return true
		})
		return nil
	})
	if err != nil {
		return err
	}
	for _, u := range users {
		if err := fn(u); err != nil {
			return err
		}
	}
	return nil
}

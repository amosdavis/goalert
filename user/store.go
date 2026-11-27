package user

import (
	"context"

	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/validation/validate"
)

// Store allows managing users on the Delta backend.
type Store struct {
	delta *deltaUsers
}

// SetAuthSubject will add or update the auth subject for the provider/subject pair to point to the provided user ID.
func (s *Store) SetAuthSubject(ctx context.Context, providerID, subjectID, userID string) error {
	err := permission.LimitCheckAny(ctx, permission.System, permission.Admin)
	if err != nil {
		return err
	}

	err = validate.Many(
		validate.SubjectID("ProviderID", providerID),
		validate.SubjectID("SubjectID", subjectID),
		validate.UUID("UserID", userID),
	)
	if err != nil {
		return err
	}
	return s.delta.setAuthSubject(ctx, AuthSubject{ProviderID: providerID, SubjectID: subjectID, UserID: userID})
}

// WithoutAuthProviderFunc will call forEachFn for each user that is missing an auth subject for the given provider ID.
// If an error is returned by forEachFn it will stop reading and be returned. Favorites information will not be included (always false).
func (s *Store) WithoutAuthProviderFunc(ctx context.Context, providerID string, forEachFn func(User) error) error {
	err := permission.LimitCheckAny(ctx, permission.System, permission.Admin)
	if err != nil {
		return err
	}
	err = validate.SubjectID("ProviderID", providerID)
	if err != nil {
		return err
	}
	return s.delta.withoutAuthProviderFunc(ctx, providerID, forEachFn)
}

// AuthSubjectsFunc will call the provided forEachFn for each AuthSubject.
// If an error is returned by forEachFn it will stop reading subjects and be returned.
//
// providerID, if not empty, will limit AuthSubjects to those with the same providerID.
// userID, if not empty, will limit AuthSubjects to those assigned to the given userID(s).
func (s *Store) AuthSubjectsFunc(ctx context.Context, providerID string, userIDs []string, forEachFn func(AuthSubject) error) error {
	checks := []permission.Checker{permission.Admin}
	if len(userIDs) == 1 {
		checks = append(checks, permission.MatchUser(userIDs[0]))
	}
	err := permission.LimitCheckAny(ctx, checks...)
	if err != nil {
		return err
	}
	if providerID != "" {
		err = validate.SubjectID("ProviderID", providerID)
	}
	err = validate.Many(err, validate.ManyUUID("UserID", userIDs, 100))
	if err != nil {
		return err
	}
	return s.delta.authSubjectsFunc(ctx, providerID, userIDs, forEachFn)
}

// DeleteManyTx will delete multiple users within the same transaction. If tx is nil,
// a transaction will be started and committed before returning.
func (s *Store) DeleteManyTx(ctx context.Context, tx txn.Tx, ids []string) error {
	err := permission.LimitCheckAny(ctx, permission.System, permission.Admin)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}

	err = validate.Range("Count", len(ids), 1, 100)
	if err != nil {
		return err
	}
	return s.delta.deleteMany(ctx, tx, ids)
}

// InsertTx creates a new User.
func (s *Store) InsertTx(ctx context.Context, tx txn.Tx, u *User) (*User, error) {
	err := permission.LimitCheckAny(ctx, permission.System, permission.Admin)
	if err != nil {
		return nil, err
	}

	n, err := u.Normalize()
	if err != nil {
		return nil, err
	}

	if err := s.delta.insert(ctx, tx, n); err != nil {
		return nil, err
	}

	return n, nil
}

// Insert is equivalent to calling InsertTx(ctx, nil, u).
func (s *Store) Insert(ctx context.Context, u *User) (*User, error) { return s.InsertTx(ctx, nil, u) }

// activeIndexWithout returns the active index of a rotation after removing a
// user, who may be in it more than once, from its participants: the one of
// the same participant, or if that was the user, of the next one left.
func activeIndexWithout(userIDs []string, activeIndex int, userID string) int {
	var left, newIndex int
	for i, id := range userIDs {
		if id == userID {
			continue
		}
		left++
		if i < activeIndex {
			newIndex++
		}
	}
	if newIndex >= left {
		return 0
	}
	return newIndex
}

// UpdateTx allows updating a user name and email.
func (s *Store) UpdateTx(ctx context.Context, tx txn.Tx, u *User) error {
	err := permission.LimitCheckAny(ctx, permission.System, permission.Admin, permission.MatchUser(u.ID))
	if err != nil {
		return err
	}

	n, err := u.Normalize()
	if err != nil {
		return err
	}

	return s.delta.update(ctx, tx, n)
}

// SetUserRoleTx allows updating the role of the given user ID.
func (s *Store) SetUserRoleTx(ctx context.Context, tx txn.Tx, id string, role permission.Role) error {
	err := permission.LimitCheckAny(ctx, permission.System, permission.Admin)
	if err != nil {
		return err
	}

	err = validate.Many(
		validate.UUID("UserID", id),
		validate.OneOf("Role", role, permission.RoleAdmin, permission.RoleUser),
	)
	if err != nil {
		return err
	}

	return s.delta.setRole(ctx, tx, id, role)
}

// FindMany will return all users matching the provided IDs.
//
// There is no guarantee the returned users will be in the same order or
// that the number of returned users matches the number of provided IDs.
func (s *Store) FindMany(ctx context.Context, ids []string) ([]User, error) {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}

	err = validate.ManyUUID("UserID", ids, 200)
	if err != nil {
		return nil, err
	}

	return s.delta.findMany(ctx, ids)
}

// FindOne is equivalent to calling FindOneTx(ctx, nil, id, false).
func (s *Store) FindOne(ctx context.Context, id string) (*User, error) {
	return s.FindOneTx(ctx, nil, id, false)
}

// FindOneTx will return a single user. When `forUpdate` is true, favorite
// information is omitted (always false).
func (s *Store) FindOneTx(ctx context.Context, tx txn.Tx, id string, forUpdate bool) (*User, error) {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}

	err = validate.UUID("UserID", id)
	if err != nil {
		return nil, err
	}

	return s.delta.findOne(ctx, tx, id, forUpdate)
}

// FindOneBySubject will find a user matching the subjectID for the given providerID.
func (s *Store) FindOneBySubject(ctx context.Context, providerID, subjectID string) (*User, error) {
	err := permission.LimitCheckAny(ctx, permission.Admin)
	if err != nil {
		return nil, err
	}

	err = validate.Many(
		validate.SubjectID("ProviderID", providerID),
		validate.SubjectID("SubjectID", subjectID),
	)
	if err != nil {
		return nil, err
	}
	return s.delta.findOneBySubject(ctx, providerID, subjectID)
}

// FindSomeAuthSubjectsForProvider returns up to `limit` auth subjects associated with a given providerID.
//
// afterSubjectID can be specified for paginating responses. Results are sorted by subject id.
func (s *Store) FindSomeAuthSubjectsForProvider(ctx context.Context, limit int, afterSubjectID, providerID string) ([]AuthSubject, error) {
	err := permission.LimitCheckAny(ctx, permission.System, permission.Admin)
	if err != nil {
		return nil, err
	}

	// treat as a subject ID for now
	err = validate.Many(
		validate.SubjectID("ProviderID", providerID),
		validate.Range("Limit", limit, 0, 9000),
	)
	if afterSubjectID != "" {
		err = validate.Many(err, validate.SubjectID("AfterID", afterSubjectID))
	}
	if err != nil {
		return nil, err
	}
	if limit == 0 {
		limit = 50
	}
	return s.delta.findSomeAuthSubjects(ctx, limit, afterSubjectID, providerID)
}

// FindAllAuthSubjectsForUser returns all auth subjects associated with a given userID.
func (s *Store) FindAllAuthSubjectsForUser(ctx context.Context, userID string) ([]AuthSubject, error) {
	var result []AuthSubject
	err := s.AuthSubjectsFunc(ctx, "", []string{userID}, func(sub AuthSubject) error {
		result = append(result, sub)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

// AddAuthSubjectTx adds an auth subject for a user.
func (s *Store) AddAuthSubjectTx(ctx context.Context, tx txn.Tx, a *AuthSubject) error {
	var userID string
	if a != nil {
		userID = a.UserID
	}
	err := permission.LimitCheckAny(ctx, permission.System, permission.Admin, permission.MatchUser(userID))
	if err != nil {
		return err
	}

	n, err := a.Normalize()
	if err != nil {
		return err
	}
	return s.delta.addAuthSubject(ctx, tx, *n)
}

// DeleteAuthSubjectTx removes an auth subject for a user.
//
// If the subject does not exist, nil is returned.
func (s *Store) DeleteAuthSubjectTx(ctx context.Context, tx txn.Tx, a *AuthSubject) error {
	err := permission.LimitCheckAny(ctx, permission.System, permission.Admin)
	if err != nil {
		return err
	}

	n, err := a.Normalize()
	if err != nil {
		return err
	}
	return s.delta.deleteAuthSubject(ctx, tx, *n)
}

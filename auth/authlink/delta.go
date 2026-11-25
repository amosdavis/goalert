package authlink

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/auth/authdelta"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/keyring"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/user"
	"github.com/target/goalert/validation"
)

// requestRow is an auth_link_requests row on the Delta backend.
type requestRow struct {
	ID         string    `parquet:"id"`
	ProviderID string    `parquet:"provider_id"`
	SubjectID  string    `parquet:"subject_id"`
	ExpiresAt  time.Time `parquet:"expires_at,timestamp(microsecond:utc)"`
	Metadata   []byte    `parquet:"metadata"`
	CreatedAt  time.Time `parquet:"created_at,timestamp(microsecond:utc)"`
}

// DeltaTables are the auth link store's tables on the Delta backend.
type DeltaTables struct {
	requests *filestore.Table[requestRow]
}

// DefineDeltaTables adds the auth link store's tables to the Delta backend's
// schema: auth_link_requests, keyed by id (auth_link_requests_pkey). It only
// holds the requests of the last minutes, so one bucket is enough (F39).
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	return &DeltaTables{
		requests: filestore.NewTable(s, "auth_link_requests", func(r requestRow) string { return r.ID }).Buckets(1),
	}
}

// InsertSubjectFunc adds an auth subject (auth_subjects). One that exists
// already is an error. user.DeltaTables.InsertAuthSubject is one.
type InsertSubjectFunc func(tx *filestore.Tx, a user.AuthSubject) error

// NewDeltaStore returns an auth link store on the Delta backend.
func NewDeltaStore(db txn.DB, tables *DeltaTables, k keyring.Keyring, insertSubject InsertSubjectFunc) *Store {
	return &Store{k: k, delta: &deltaStore{
		db: db, DeltaTables: tables,
		insertSubject: insertSubject,
		check:         authdelta.NewChecker(db, authdelta.MaxStale),
	}}
}

type deltaStore struct {
	db txn.DB
	*DeltaTables
	insertSubject InsertSubjectFunc
	check         *authdelta.Checker
}

// find returns the request if there is one that hasn't expired.
func (d *deltaStore) find(s deltatxn.Scope, id uuid.UUID) (requestRow, error) {
	row, ok := d.requests.Get(s.Reader, id.String())
	if !ok || !row.ExpiresAt.After(s.Now) {
		return requestRow{}, sql.ErrNoRows
	}
	return row, nil
}

// metadata returns the metadata of a request, or sql.ErrNoRows. The request
// was probably made on another pod, seconds ago.
func (d *deltaStore) metadata(ctx context.Context, id uuid.UUID) ([]byte, error) {
	var data []byte
	err := d.check.Read(ctx, func(_ txn.Tx, s deltatxn.Scope) error {
		row, err := d.find(s, id)
		data = row.Metadata
		return err
	})
	if err != nil {
		return nil, err
	}
	return data, nil
}

// link uses up a request, and links its subject to the user in the same
// transaction. Two pods that use one request at once can't both succeed: the
// one whose commit loses runs again and no longer finds it (F4).
func (d *deltaStore) link(ctx context.Context, id uuid.UUID, userID string) error {
	return deltatxn.Write(ctx, d.db, nil, func(s deltatxn.Scope) error {
		row, err := d.find(s, id)
		if err != nil {
			return validation.NewGenericError("invalid link token")
		}
		if _, err := d.requests.Delete(s.Tx, row.ID); err != nil {
			return err
		}
		return d.insertSubject(s.Tx, user.AuthSubject{ProviderID: row.ProviderID, SubjectID: row.SubjectID, UserID: userID})
	})
}

// add adds a request that expires after valid, by the same clock that decides
// whether it has (F13). It also removes the requests that expired unused,
// which nothing else does (F16); that leaves only a few rows to look through.
func (d *deltaStore) add(ctx context.Context, id uuid.UUID, providerID, subjectID string, valid time.Duration, metadata []byte) error {
	return deltatxn.Write(ctx, d.db, nil, func(s deltatxn.Scope) error {
		var expired []string
		d.requests.Ascend(s.Reader, func(r requestRow) bool {
			if !r.ExpiresAt.After(s.Now) {
				expired = append(expired, r.ID)
			}
			return true
		})
		for _, id := range expired {
			if _, err := d.requests.Delete(s.Tx, id); err != nil {
				return err
			}
		}
		return d.requests.Insert(s.Tx, requestRow{
			ID: id.String(), ProviderID: providerID, SubjectID: subjectID,
			ExpiresAt: s.Now.Add(valid), Metadata: metadata, CreatedAt: s.Now,
		}, "auth_link_requests_pkey")
	})
}

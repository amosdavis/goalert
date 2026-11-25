package authlink

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/auth/authdelta"
	"github.com/target/goalert/config"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/keyring"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/user"
	"github.com/target/goalert/validation"
)

// testKeys signs JWTs with a fixed key.
type testKeys struct{ keyring.Keyring }

func (testKeys) SignJWT(c jwt.Claims) (string, error) {
	return jwt.NewWithClaims(jwt.SigningMethodHS256, c).SignedString([]byte("test"))
}

func (testKeys) VerifyJWT(token string, c jwt.Claims, iss, aud string) (bool, error) {
	_, err := jwt.ParseWithClaims(token, c, func(*jwt.Token) (any, error) { return []byte("test"), nil },
		jwt.WithIssuer(iss), jwt.WithAudience(aud))
	return false, err
}

// subjectRow stands in for auth_subjects.
type subjectRow struct {
	Key    string `parquet:"key"`
	UserID string `parquet:"user_id"`
}

type env struct {
	store    objstore.Store
	schema   *filestore.Schema
	tables   *DeltaTables
	subjects *filestore.Table[subjectRow]
	clock    *filestore.FakeClock
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{store: objstore.NewMem(), schema: filestore.NewSchema(), clock: filestore.NewFakeClock(time.Now())}
	e.clock.SetSpeed(0)
	e.clock.Set(time.Now().Truncate(time.Second))
	e.tables = DefineDeltaTables(e.schema)
	e.subjects = filestore.NewTable(e.schema, "subjects", func(s subjectRow) string { return s.Key })
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{WriterID: "admin", Clock: e.clock}))
	return e
}

type pod struct {
	*Store
	db *filestore.DeltaDB
}

func (e *env) pod(t *testing.T, name string) *pod {
	t.Helper()
	db, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		WriterID: name, Clock: e.clock, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	s := NewDeltaStore(deltatxn.DB{DB: db}, e.tables, testKeys{}, func(tx *filestore.Tx, a user.AuthSubject) error {
		return e.subjects.Insert(tx, subjectRow{Key: a.ProviderID + "/" + a.SubjectID, UserID: a.UserID}, "auth_subjects_pkey")
	})
	return &pod{Store: s, db: db}
}

func (e *env) linked(t *testing.T) map[string]string {
	t.Helper()
	p := e.pod(t, "reader-"+uuid.NewString())
	m := make(map[string]string)
	e.subjects.Ascend(p.db.View(), func(s subjectRow) bool { m[s.Key] = s.UserID; return true })
	return m
}

func (e *env) requests(t *testing.T) int {
	t.Helper()
	return e.tables.requests.Len(e.pod(t, "reader-"+uuid.NewString()).db.View())
}

func systemCtx() context.Context {
	return config.Config{}.Context(permission.SystemContext(context.Background(), "Test"))
}

func userCtx(id string) context.Context {
	return permission.UserContext(context.Background(), id, permission.RoleUser)
}

var meta = Metadata{UserDetails: "bob", AlertID: 5, AlertAction: "ResultAcknowledge"}

func (p *pod) token(t *testing.T, subject string) string {
	t.Helper()
	link, err := p.AuthLinkURL(systemCtx(), "slack:T1", subject, meta)
	require.NoError(t, err)
	u, err := url.Parse(link)
	require.NoError(t, err)
	tok := u.Query().Get("authLinkToken")
	require.NotEmpty(t, tok)
	return tok
}

func TestDeltaLink(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	alice, bob := uuid.NewString(), uuid.NewString()
	tok := p.token(t, "U1")

	m, err := p.FindLinkMetadata(userCtx(alice), tok)
	require.NoError(t, err)
	assert.Equal(t, &meta, m)
	assert.Empty(t, e.linked(t), "looking at a request doesn't use it")

	require.NoError(t, p.LinkAccount(userCtx(alice), tok))
	assert.Equal(t, map[string]string{"slack:T1/U1": alice}, e.linked(t))

	m, err = p.FindLinkMetadata(userCtx(alice), tok)
	require.NoError(t, err)
	assert.Nil(t, m, "a used request is gone")
	err = p.LinkAccount(userCtx(bob), tok)
	require.Error(t, err)
	assert.True(t, validation.IsClientError(err))
	assert.ErrorContains(t, err, "invalid link token")
	assert.Zero(t, e.requests(t))

	m, err = p.FindLinkMetadata(userCtx(alice), "not a token")
	require.NoError(t, err)
	assert.Nil(t, m)
	require.Error(t, p.LinkAccount(userCtx(alice), "not a token"))
}

func TestDeltaLinkSubjectTaken(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	alice, bob := uuid.NewString(), uuid.NewString()
	require.NoError(t, p.LinkAccount(userCtx(alice), p.token(t, "U1")))

	tok := p.token(t, "U1")
	err := p.LinkAccount(userCtx(bob), tok)
	var uerr *filestore.UniqueError
	require.ErrorAs(t, err, &uerr)
	assert.Equal(t, "auth_subjects_pkey", uerr.ConstraintName())
	assert.Equal(t, map[string]string{"slack:T1/U1": alice}, e.linked(t))
	assert.Equal(t, 1, e.requests(t), "the request and the link are one transaction, so the request is still there (F2)")
}

func TestDeltaLinkExpires(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	alice := uuid.NewString()
	start := e.clock.Now()
	tok := p.token(t, "U1")

	e.clock.Set(start.Add(linkTTL - time.Second))
	m, err := p.FindLinkMetadata(userCtx(alice), tok)
	require.NoError(t, err)
	assert.NotNil(t, m)

	e.clock.Set(start.Add(linkTTL))
	m, err = p.FindLinkMetadata(userCtx(alice), tok)
	require.NoError(t, err)
	assert.Nil(t, m, "expired")
	assert.ErrorContains(t, p.LinkAccount(userCtx(alice), tok), "invalid link token")
	assert.Empty(t, e.linked(t))

	assert.Equal(t, 1, e.requests(t))
	p.token(t, "U2")
	assert.Equal(t, 1, e.requests(t), "a new request removes the expired ones (F16)")
}

func TestDeltaLinkOnAnotherPod(t *testing.T) {
	e := newEnv(t)
	a, b := e.pod(t, "a"), e.pod(t, "b")
	alice := uuid.NewString()
	_, err := b.FindLinkMetadata(userCtx(alice), a.token(t, "U0"))
	require.NoError(t, err)

	tok := a.token(t, "U1")
	m, err := b.FindLinkMetadata(userCtx(alice), tok)
	require.NoError(t, err)
	assert.Equal(t, &meta, m, "a pod that hasn't followed the log yet finds a new request (F29)")

	require.NoError(t, a.LinkAccount(userCtx(alice), tok))
	require.Error(t, b.LinkAccount(userCtx(alice), tok))
	b.delta.check = authdelta.NewChecker(b.delta.db, 0)
	m, err = b.FindLinkMetadata(userCtx(alice), tok)
	require.NoError(t, err)
	assert.Nil(t, m)
}

func TestDeltaLinkRace(t *testing.T) {
	e := newEnv(t)
	pods := []*pod{e.pod(t, "a"), e.pod(t, "b")}
	tok := pods[0].token(t, "U1")

	var ok atomic.Int32
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := pods[i%2].LinkAccount(userCtx(uuid.NewString()), tok)
			if err == nil {
				ok.Add(1)
				return
			}
			assert.ErrorContains(t, err, "invalid link token")
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 1, ok.Load(), "a request links one account, however many pods use it at once (F4)")
	assert.Len(t, e.linked(t), 1)
	assert.Zero(t, e.requests(t))
}

func TestDeltaLinkPermissions(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	tok := p.token(t, "U1")
	_, err := p.FindLinkMetadata(context.Background(), tok)
	assert.True(t, permission.IsPermissionError(err))
	assert.True(t, permission.IsPermissionError(p.LinkAccount(context.Background(), tok)))
	_, err = p.AuthLinkURL(userCtx(uuid.NewString()), "slack:T1", "U1", meta)
	assert.True(t, permission.IsPermissionError(err))
	_, err = p.AuthLinkURL(systemCtx(), "slack:T1", "", meta)
	assert.True(t, validation.IsClientError(err))
}

package auth

import (
	"bytes"
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/auth/authdelta"
	"github.com/target/goalert/auth/authtoken"
	"github.com/target/goalert/config"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/keyring"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/user"
)

// testKeys signs with a fixed signature, and knows an old one.
type testKeys struct{ keyring.Keyring }

func (testKeys) Sign([]byte) ([]byte, error) { return []byte("new"), nil }

func (testKeys) Verify(_, sig []byte) (valid, oldKey bool) {
	old := bytes.Equal(sig, []byte("old"))
	return old || bytes.Equal(sig, []byte("new")), old
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
	users    *user.DeltaTables
	subjects *filestore.Table[subjectRow]
	clock    *filestore.FakeClock
	start    time.Time
}

func newEnv(t *testing.T) *env {
	t.Helper()
	start := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	e := &env{store: objstore.NewMem(), schema: filestore.NewSchema(), clock: filestore.NewFakeClock(start), start: start}
	e.clock.SetSpeed(0)
	e.clock.Set(start)
	e.tables = DefineDeltaTables(e.schema)
	e.users = user.DefineDeltaTables(e.schema)
	e.subjects = filestore.NewTable(e.schema, "test_subjects", func(s subjectRow) string { return s.Key })
	DeclareDeltaForeignKeys(e.tables, e.users.Users)
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{WriterID: "admin", Clock: e.clock}))
	return e
}

type pod struct {
	*Handler
	db       *filestore.DeltaDB
	profiles atomic.Int32 // calls of UpdateProfile
}

func subjectKey(providerID, subjectID string) string { return providerID + "/" + subjectID }

func (e *env) pod(t *testing.T, name string) *pod {
	t.Helper()
	fdb, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		WriterID: name, Clock: e.clock, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fdb.Close() })
	p := &pod{db: fdb}
	db := deltatxn.DB{DB: fdb}
	p.Handler = NewDeltaHandler(db, e.tables, HandlerConfig{UserStore: user.NewDeltaStore(db, e.users, nil), SessionKeyring: testKeys{}, APIKeyring: testKeys{}}, DeltaUsers{
		AuthSubjectUserID: func(r filestore.Reader, providerID, subjectID string) (string, bool) {
			row, ok := e.subjects.Get(r, subjectKey(providerID, subjectID))
			return row.UserID, ok
		},
		InsertAuthSubject: func(tx *filestore.Tx, a user.AuthSubject) error {
			return e.subjects.Insert(tx, subjectRow{Key: subjectKey(a.ProviderID, a.SubjectID), UserID: a.UserID}, "auth_subjects_pkey")
		},
		UpdateProfile: func(tx *filestore.Tx, userID, name, email string) error {
			p.profiles.Add(1)
			row, ok := e.users.Users.Get(tx, userID)
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
				return nil
			}
			return e.users.Users.Put(tx, row)
		},
	})
	return p
}

// strict makes the pod check every session at the latest version.
func (p *pod) strict() *pod {
	p.delta.check = authdelta.NewChecker(p.delta.db, 0)
	return p
}

var (
	ctx    = config.Config{}.Context(context.Background())
	sysCtx = permission.SystemContext(ctx, "Test")
)

func (e *env) addUser(t *testing.T, p *pod, role permission.Role) string {
	t.Helper()
	u, err := p.cfg.UserStore.Insert(sysCtx, &user.User{Name: "Joe", Role: role})
	require.NoError(t, err)
	return u.ID
}

func (p *pod) newSession(t *testing.T, userID string) uuid.UUID {
	t.Helper()
	tok, err := p.CreateSession(ctx, "test agent", userID)
	require.NoError(t, err)
	assert.Equal(t, authtoken.TypeSession, tok.Type)
	return tok.ID
}

// sessions returns the sessions by ID, as a fresh pod has them.
func (e *env) sessions(t *testing.T) map[string]sessionRow {
	t.Helper()
	m := make(map[string]sessionRow)
	e.tables.Sessions.Ascend(e.pod(t, "reader-"+uuid.NewString()).db.View(), func(r sessionRow) bool { m[r.ID] = r; return true })
	return m
}

func (p *pod) check(id uuid.UUID) (string, permission.Role, error) {
	userID, role, err := p.delta.session(ctx, id)
	return userID.String(), role, err
}

func requireEnded(t *testing.T, p *pod, id uuid.UUID) {
	t.Helper()
	_, _, err := p.check(id)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func sessionCtx(userID string, role permission.Role, sessionID uuid.UUID) context.Context {
	return permission.UserSourceContext(ctx, userID, role, &permission.SourceInfo{Type: permission.SourceTypeAuthProvider, ID: sessionID.String()})
}

func TestDeltaSession(t *testing.T) {
	e := newEnv(t)
	a, b := e.pod(t, "a"), e.pod(t, "b")
	alice := e.addUser(t, a, permission.RoleUser)
	_, _, err := b.check(a.newSession(t, alice))
	require.NoError(t, err)

	id := a.newSession(t, alice)
	row := e.sessions(t)[id.String()]
	assert.Equal(t, sessionRow{ID: id.String(), UserID: &alice, UserAgent: "test agent", CreatedAt: e.start, LastAccess: e.start}, row)
	for _, p := range []*pod{a, b} {
		userID, role, err := p.check(id)
		require.NoError(t, err, "also on a pod that hasn't followed the log yet (F29)")
		assert.Equal(t, alice, userID)
		assert.Equal(t, permission.RoleUser, role)
	}
	requireEnded(t, a, uuid.New())

	b.strict()
	require.NoError(t, a.cfg.UserStore.SetUserRoleTx(sysCtx, nil, alice, permission.RoleAdmin))
	_, role, err := b.check(id)
	require.NoError(t, err)
	assert.Equal(t, permission.RoleAdmin, role, "the role is the user's now")

	_, err = a.db.Update(ctx, func(tx *filestore.Tx) error {
		_, err := e.users.Users.Delete(tx, alice)
		return err
	})
	require.NoError(t, err)
	assert.Empty(t, e.sessions(t), "ON DELETE CASCADE")
	requireEnded(t, a, id)
	requireEnded(t, b, id)
}

func TestDeltaSessionOfMissingUser(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	id := uuid.New()
	err := p.delta.start(ctx, id, "agent", uuid.NewString())
	var fkErr *filestore.ForeignKeyError
	require.ErrorAs(t, err, &fkErr)
	assert.Equal(t, "auth_user_sessions_user_id_fkey", fkErr.ConstraintName())
	assert.NotContains(t, err.Error(), id.String(), "a session's ID is not for logs (F20)")

	alice := e.addUser(t, p, permission.RoleUser)
	require.NoError(t, p.delta.start(ctx, id, "agent", alice))
	err = p.delta.start(ctx, id, "agent", alice)
	var uerr *filestore.UniqueError
	require.ErrorAs(t, err, &uerr)
	assert.Equal(t, "auth_user_sessions_pkey", uerr.ConstraintName())
	assert.NotContains(t, err.Error(), id.String())

	_, err = p.CreateSession(ctx, "agent", "not a user")
	require.Error(t, err)

	// Rows with a user agent no data file can hold, and without a user.
	id2 := uuid.New()
	require.NoError(t, p.delta.start(ctx, id2, "bad\xff", alice))
	assert.Equal(t, "bad�", e.sessions(t)[id2.String()].UserAgent)
	id3 := uuid.New()
	_, err = p.db.Update(ctx, func(tx *filestore.Tx) error {
		return e.tables.Sessions.Put(tx, sessionRow{ID: id3.String(), CreatedAt: tx.Now(), LastAccess: tx.Now()})
	})
	require.NoError(t, err)
	requireEnded(t, p, id3)
}

func TestDeltaEndedSessionOnOtherPods(t *testing.T) {
	e := newEnv(t)
	a, b, strict := e.pod(t, "a"), e.pod(t, "b"), e.pod(t, "c").strict()
	alice := e.addUser(t, a, permission.RoleUser)
	id := a.newSession(t, alice)
	for _, p := range []*pod{a, b, strict} {
		_, _, err := p.check(id)
		require.NoError(t, err)
	}
	b.delta.check = authdelta.NewChecker(b.delta.db, 100*time.Millisecond)

	require.NoError(t, a.EndUserSessionTx(sessionCtx(alice, permission.RoleUser, uuid.New()), nil, id.String()))
	requireEnded(t, a, id)
	requireEnded(t, strict, id)
	requireEnded(t, e.pod(t, "d"), id)
	assert.Eventually(t, func() bool { _, _, err := b.check(id); return err != nil }, 10*time.Second, 10*time.Millisecond,
		"refused within MaxStale by a pod that doesn't follow the log (F29)")
}

func TestDeltaEndSessions(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	alice, bob := e.addUser(t, p, permission.RoleUser), e.addUser(t, p, permission.RoleUser)
	s := make([]uuid.UUID, 6)
	for i := range s {
		owner := alice
		if i >= 4 {
			owner = bob
		}
		s[i] = p.newSession(t, owner)
	}
	left := func(want ...int) {
		t.Helper()
		got := e.sessions(t)
		assert.Len(t, got, len(want))
		for _, i := range want {
			assert.Contains(t, got, s[i].String())
		}
	}
	asAlice := sessionCtx(alice, permission.RoleUser, s[0])

	require.NoError(t, p.EndUserSessionTx(asAlice, nil, s[4].String(), uuid.NewString()))
	left(0, 1, 2, 3, 4, 5)
	require.NoError(t, p.EndUserSessionTx(asAlice, nil, s[1].String(), strings.ToUpper(s[2].String())))
	left(0, 3, 4, 5)
	require.NoError(t, p.EndUserSessionTx(permission.UserContext(ctx, "", permission.RoleUser), nil, s[0].String()), "without a user, nothing is theirs")
	left(0, 3, 4, 5)
	require.Error(t, p.EndUserSessionTx(asAlice, nil, "not an ID"))
	assert.True(t, permission.IsPermissionError(p.EndUserSessionTx(ctx, nil, s[0].String())))

	require.NoError(t, p.txdb.Run(ctx, nil, func(tx txn.Tx) error {
		return p.EndUserSessionTx(sessionCtx(bob, permission.RoleAdmin, s[4]), tx, s[3].String(), s[5].String())
	}))
	left(0, 4)
}

func TestDeltaEndAllSessions(t *testing.T) {
	e := newEnv(t)
	a, b := e.pod(t, "a"), e.pod(t, "b")
	alice, bob := e.addUser(t, a, permission.RoleUser), e.addUser(t, a, permission.RoleUser)
	current, other := a.newSession(t, alice), a.newSession(t, bob)
	a.newSession(t, alice)
	// b hasn't seen any of these; a doesn't see this one.
	b.newSession(t, alice)

	require.NoError(t, a.EndAllUserSessionsTx(sessionCtx(alice, permission.RoleUser, current), nil))
	got := e.sessions(t)
	assert.Len(t, got, 2)
	assert.Contains(t, got, current.String())
	assert.Contains(t, got, other.String())

	require.NoError(t, b.EndAllUserSessionsTx(sysCtx, nil), "without a user, nothing is theirs")
	assert.Len(t, e.sessions(t), 2)
	require.NoError(t, b.EndAllUserSessionsTx(permission.UserContext(ctx, alice, permission.RoleUser), nil), "without a current session, all of them end")
	assert.Len(t, e.sessions(t), 1)
	assert.True(t, permission.IsPermissionError(b.EndAllUserSessionsTx(ctx, nil)))
}

func TestDeltaFindAllSessions(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	alice := e.addUser(t, p, permission.RoleUser)
	e.clock.Set(e.start.Add(90 * time.Second))
	asAlice := permission.UserContext(ctx, alice, permission.RoleUser)
	none, err := p.FindAllUserSessions(asAlice, alice)
	require.NoError(t, err)
	assert.Nil(t, none)

	id := p.newSession(t, alice)
	e.clock.Set(e.start.Add(time.Hour))
	id2 := p.newSession(t, alice)
	p.newSession(t, e.addUser(t, p, permission.RoleUser))
	require.NoError(t, p.delta.writeAccess(ctx, map[string]time.Time{sessionKey(id.String()): e.start.Add(2*time.Hour + time.Second)}))
	got, err := e.pod(t, "b").FindAllUserSessions(asAlice, alice)
	require.NoError(t, err)
	assert.Equal(t, []UserSession{
		{ID: id.String(), UserAgent: "test agent", UserID: alice, CreatedAt: e.start.Add(time.Minute), LastAccessAt: e.start.Add(2 * time.Hour)},
		{ID: id2.String(), UserAgent: "test agent", UserID: alice, CreatedAt: e.start.Add(time.Hour), LastAccessAt: e.start.Add(time.Hour)},
	}, got, "oldest first")
	_, err = p.FindAllUserSessions(asAlice, uuid.NewString())
	assert.True(t, permission.IsPermissionError(err))
}

func (p *pod) flush(t *testing.T, id uuid.UUID) {
	t.Helper()
	require.NoError(t, p.Shutdown(ctx), "shutting down writes what isn't written yet")
	_, pending := p.delta.access.Pending(sessionKey(id.String()))
	require.False(t, pending)
}

func TestDeltaLastAccess(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	alice := e.addUser(t, p, permission.RoleUser)
	id, id2 := p.newSession(t, alice), p.newSession(t, alice)
	version := p.db.View().Index()
	use := func(id uuid.UUID) {
		t.Helper()
		_, _, err := p.check(id)
		require.NoError(t, err)
	}

	e.clock.Set(e.start.Add(time.Minute))
	use(id)
	p.flush(t, id)
	assert.Equal(t, version, p.db.View().Index(), "a use within a minute of the last recorded one commits nothing (F37)")

	e.clock.Set(e.start.Add(time.Hour))
	use(id)
	p.flush(t, id)
	assert.Equal(t, version+1, p.db.View().Index())
	assert.Equal(t, e.start.Add(time.Hour), e.sessions(t)[id.String()].LastAccess)
	assert.Equal(t, e.start, e.sessions(t)[id2.String()].LastAccess)

	// The next minute's uses, of both sessions, are one commit.
	e.clock.Set(e.start.Add(2 * time.Hour))
	for range 20 {
		use(id)
		use(id2)
	}
	e.clock.Set(e.start.Add(2*time.Hour + 30*time.Second))
	use(id)
	_, pending := p.delta.access.Pending(sessionKey(id.String()))
	assert.True(t, pending)
	assert.Equal(t, version+1, p.db.View().Index(), "a request never waits for a commit")
	p.flush(t, id)
	assert.Equal(t, version+2, p.db.View().Index())
	for _, id := range []uuid.UUID{id, id2} {
		assert.Equal(t, e.start.Add(2*time.Hour), e.sessions(t)[id.String()].LastAccess)
	}

	// A pod that writes late doesn't undo a later access.
	require.NoError(t, e.pod(t, "b").delta.writeAccess(ctx, map[string]time.Time{sessionKey(id.String()): e.start.Add(time.Hour)}))
	assert.Equal(t, e.start.Add(2*time.Hour), e.sessions(t)[id.String()].LastAccess)
}

func TestDeltaLastAccessOfEndedSession(t *testing.T) {
	e := newEnv(t)
	a, b := e.pod(t, "a"), e.pod(t, "b")
	alice := e.addUser(t, a, permission.RoleUser)
	id := a.newSession(t, alice)
	e.clock.Set(e.start.Add(time.Hour))
	_, _, err := b.check(id)
	require.NoError(t, err)
	b.flush(t, id)
	e.clock.Set(e.start.Add(2 * time.Hour))
	_, _, err = b.check(id)
	require.NoError(t, err)

	require.NoError(t, a.EndUserSessionTx(sessionCtx(alice, permission.RoleUser, id), nil, id.String()))
	b.flush(t, id)
	assert.Empty(t, e.sessions(t), "an access that b recorded, and wrote after a ended the session, doesn't bring it back (F29)")
	requireEnded(t, b, id)
}

func TestDeltaLastAccessRace(t *testing.T) {
	e := newEnv(t)
	a, b := e.pod(t, "a"), e.pod(t, "b")
	alice := e.addUser(t, a, permission.RoleUser)
	var ids []uuid.UUID
	access := make(map[string]time.Time)
	for range 5 {
		id := a.newSession(t, alice)
		ids = append(ids, id)
		access[sessionKey(id.String())] = e.start.Add(time.Hour)
	}
	var wg sync.WaitGroup
	for _, id := range ids[1:] {
		wg.Add(2)
		go func() {
			defer wg.Done()
			assert.NoError(t, a.EndUserSessionTx(sysCtx, nil, id.String()))
		}()
		go func() {
			defer wg.Done()
			assert.NoError(t, b.delta.writeAccess(ctx, access))
		}()
	}
	wg.Wait()
	got := e.sessions(t)
	require.Len(t, got, 1, "sessions ended on one pod stay ended, whatever another writes meanwhile (F4, F29)")
	assert.Equal(t, e.start.Add(time.Hour), got[ids[0].String()].LastAccess)
}

func TestDeltaDeleteOld(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	alice := e.addUser(t, p, permission.RoleUser)
	used := p.newSession(t, alice)
	for range 3 {
		p.newSession(t, alice)
	}
	e.clock.Set(e.start.Add(48 * time.Hour))
	fresh := p.newSession(t, alice)
	require.NoError(t, p.delta.writeAccess(ctx, map[string]time.Time{sessionKey(used.String()): e.start.Add(47 * time.Hour)}))

	del := func(limit int) (n int) {
		t.Helper()
		_, err := p.db.Update(ctx, func(tx *filestore.Tx) error {
			var err error
			n, err = e.tables.DeleteOld(tx, e.start.Add(24*time.Hour), limit)
			return err
		})
		require.NoError(t, err)
		return n
	}
	assert.Equal(t, 2, del(2))
	assert.Equal(t, 1, del(100))
	assert.Equal(t, 0, del(100))
	got := e.sessions(t)
	assert.Len(t, got, 2)
	assert.Contains(t, got, used.String())
	assert.Contains(t, got, fresh.String())
}

// request sends a request with a session cookie through WrapHandler, and
// returns the context the wrapped handler got.
func (p *pod) request(t *testing.T, cookie string) (context.Context, *httptest.ResponseRecorder) {
	t.Helper()
	var got context.Context
	h := p.WrapHandler(http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) { got = req.Context() }))
	req := httptest.NewRequest("GET", "/api/graphql", nil).WithContext(ctx)
	req.Header.Set("User-Agent", "new agent")
	req.AddCookie(&http.Cookie{Name: CookieName, Value: cookie})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.NotNil(t, got)
	return got, w
}

func sessionCookie(t *testing.T, id uuid.UUID, sig string) string {
	t.Helper()
	tok := authtoken.Token{Version: 1, Type: authtoken.TypeSession, ID: id}
	s, err := tok.Encode(func([]byte) ([]byte, error) { return []byte(sig), nil })
	require.NoError(t, err)
	return s
}

func TestDeltaWrapHandler(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	alice := e.addUser(t, p, permission.RoleAdmin)
	id := p.newSession(t, alice)

	got, w := p.request(t, sessionCookie(t, id, "new"))
	assert.Equal(t, alice, permission.UserID(got))
	assert.True(t, permission.Admin(got))
	assert.Equal(t, &permission.SourceInfo{Type: permission.SourceTypeAuthProvider, ID: id.String()}, permission.Source(got))
	assert.Empty(t, w.Result().Cookies())
	assert.Equal(t, "test agent", e.sessions(t)[id.String()].UserAgent)

	got, w = p.request(t, sessionCookie(t, id, "old"))
	assert.Equal(t, alice, permission.UserID(got))
	if assert.Len(t, w.Result().Cookies(), 1, "signed with an old key, so it gets a new one") {
		assert.Equal(t, sessionCookie(t, id, "new"), w.Result().Cookies()[0].Value)
	}
	assert.Equal(t, "new agent", e.sessions(t)[id.String()].UserAgent)
	require.NoError(t, p.delta.setUserAgent(ctx, uuid.New(), "agent"), "an UPDATE of no rows")

	got, _ = p.request(t, sessionCookie(t, id, "forged"))
	assert.False(t, permission.All(got))

	// Log out.
	req := httptest.NewRequest("POST", "/api/v2/identity/logout", nil)
	req = req.WithContext(sessionCtx(alice, permission.RoleAdmin, id))
	req.AddCookie(&http.Cookie{Name: CookieName, Value: sessionCookie(t, id, "new")})
	p.ServeLogout(httptest.NewRecorder(), req)
	assert.Empty(t, e.sessions(t))
	got, _ = p.request(t, sessionCookie(t, id, "new"))
	assert.False(t, permission.All(got))
}

// testProvider identifies everyone as ID.
type testProvider struct{ id Identity }

func (testProvider) Info(context.Context) ProviderInfo {
	return ProviderInfo{Title: "Test", Enabled: true}
}

func (p testProvider) ExtractIdentity(*RouteInfo, http.ResponseWriter, *http.Request) (*Identity, error) {
	return &p.id, nil
}

// login logs in through a test provider, and returns the session's token,
// or an error message.
func (p *pod) login(t *testing.T, id Identity, newUsers bool) (token, errMsg string) {
	t.Helper()
	p.providers["oidc"] = testProvider{id}
	var cfg config.Config
	cfg.OIDC.NewUsers = newUsers
	req := httptest.NewRequest("POST", "http://localhost/api/v2/identity/providers/oidc?noRedirect=1", nil)
	req.Header.Set("Referer", "http://localhost/")
	w := httptest.NewRecorder()
	p.IdentityProviderHandler("oidc")(w, req.WithContext(cfg.Context(context.Background())))
	if w.Code != 200 {
		return "", w.Body.String()
	}
	return w.Body.String(), ""
}

func tokenID(t *testing.T, token string) uuid.UUID {
	t.Helper()
	tok, _, err := authtoken.Parse(token, nil)
	require.NoError(t, err)
	return tok.ID
}

func (e *env) findUser(t *testing.T, id string) *user.User {
	t.Helper()
	u, err := e.pod(t, "reader-"+uuid.NewString()).cfg.UserStore.FindOne(sysCtx, id)
	require.NoError(t, err)
	return u
}

func TestDeltaLogin(t *testing.T) {
	e := newEnv(t)
	a, b := e.pod(t, "a"), e.pod(t, "b")
	joe := Identity{SubjectID: "joe", Name: "Joe", Email: "joe@example.com"}

	_, msg := a.login(t, joe, false)
	assert.Contains(t, msg, "New users are not allowed")
	assert.Empty(t, e.sessions(t))

	tok, msg := a.login(t, joe, true)
	require.Empty(t, msg)
	userID, role, err := b.check(tokenID(t, tok))
	require.NoError(t, err, "the next request may reach a pod that hasn't followed the log yet (F29)")
	assert.Equal(t, permission.RoleUser, role)
	u := e.findUser(t, userID)
	assert.Equal(t, "Joe", u.Name)
	assert.Equal(t, "joe@example.com", u.Email)
	assert.Zero(t, a.profiles.Load())

	// Again, on another pod, where new users are not allowed.
	joe.Name, joe.Email = "Joseph", ""
	tok, msg = e.pod(t, "c").login(t, joe, false)
	require.Empty(t, msg)
	again, _, err := b.check(tokenID(t, tok))
	require.NoError(t, err)
	assert.Equal(t, userID, again)
	u = e.findUser(t, userID)
	assert.Equal(t, "Joseph", u.Name)
	assert.Equal(t, "joe@example.com", u.Email, "an empty one changes nothing")
	assert.Len(t, e.sessions(t), 2)

	// Nothing to update, so logging in is one commit.
	version := b.db.View().Index()
	_, msg = b.login(t, joe, false)
	require.Empty(t, msg)
	assert.Equal(t, version+1, b.db.View().Index())
}

func TestDeltaLoginRace(t *testing.T) {
	e := newEnv(t)
	pods := []*pod{e.pod(t, "a"), e.pod(t, "b")}
	var ok atomic.Int32
	var wg sync.WaitGroup
	for i := range pods {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, msg := pods[i].login(t, Identity{SubjectID: "joe", Name: "Joe"}, true)
			if msg == "" {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()
	// Either both create the user, and one commit loses and fails, or the
	// second one already finds it.
	assert.Positive(t, ok.Load())
	v := e.pod(t, "c").db.View()
	assert.Equal(t, 1, e.users.Users.Len(v), "a new user's first logins on two pods create one user (F4, F8)")
	assert.Equal(t, 1, e.subjects.Len(v))
	assert.EqualValues(t, ok.Load(), e.tables.Sessions.Len(v))
}

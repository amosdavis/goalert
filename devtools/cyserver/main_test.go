package main

import (
	"context"
	"log/slog"
	"net/http"
	"testing"
	"time"

	sloglogrus "github.com/samber/slog-logrus/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/app"
	"github.com/target/goalert/filestore"
	utillog "github.com/target/goalert/util/log"
)

const testUser = "11111111-1111-1111-1111-111111111111"

const initialUsers = `
insert into users (id, name, email, role) values
('` + testUser + `', 'Cypress User', 'cypress@example.com', 'admin');
insert into auth_basic_users (user_id, username, password_hash) values
('` + testUser + `', 'cypress', '$2y$04$KfvjkXmDPYhrmSukrJWQ.OmmeofzMkJA8SPmkQL2lUOxE8fXwu9LG');
`

func newTestServer(t *testing.T) *deltaServer {
	t.Helper()
	legacy := utillog.NewLogger()
	legacy.ErrorsOnly()
	cfg := app.Defaults()
	cfg.LegacyLogger = legacy
	cfg.Logger = slog.New(sloglogrus.Option{Logger: legacy.Logrus()}.NewLogrusHandler())
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.StubNotifiers = true
	s := &deltaServer{cfg: cfg}
	t.Cleanup(func() { _ = s.close(context.Background()) })
	return s
}

func TestServerRestartAndReset(t *testing.T) {
	s := newTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, s.reset(ctx, initialUsers))
	require.NoError(t, s.start(ctx, nil))

	resp, err := http.Get(s.app.URL() + "/health")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	id, err := s.fixtureStores.AuthBasic.Validate(ctx, "cypress", "hunter2")
	require.NoError(t, err)
	assert.Equal(t, testUser, id)

	// Restarting opens another pod on the same table and preserves the login.
	require.NoError(t, s.start(ctx, []string{"--api-only"}))
	id, err = s.fixtureStores.AuthBasic.Validate(ctx, "cypress", "hunter2")
	require.NoError(t, err)
	assert.Equal(t, testUser, id)

	require.NoError(t, s.reset(ctx, ""))
	assert.Equal(t, 0, s.fixtureStores.Tables.User.Users.Len(s.fixtureDB.View()))
}

func TestStartConfig(t *testing.T) {
	cfg, err := startConfig(app.Defaults(), []string{"--api-only", "--experimental=example,other"})
	require.NoError(t, err)
	assert.True(t, cfg.APIOnly)
	assert.Len(t, cfg.ExpFlags, 2)
	_, err = startConfig(app.Defaults(), []string{"--storage-objects=invalid"})
	assert.Error(t, err)
}

func TestClock(t *testing.T) {
	s := newTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, s.reset(ctx, ""))
	stopped := 0.0
	require.NoError(t, s.setClock(clockRequest{Speed: &stopped}))
	before := s.clock.Now()
	require.NoError(t, s.setClock(clockRequest{Advance: "-21h"}))
	assert.WithinDuration(t, before.Add(-21*time.Hour), s.clock.Now(), time.Millisecond)
	assert.WithinDuration(t, before.Add(-21*time.Hour), s.fixtureDB.Now(), time.Millisecond)
	_, err := s.fixtureDB.Update(ctx, func(tx *filestore.Tx) error {
		assert.WithinDuration(t, before.Add(-21*time.Hour), tx.Now(), time.Millisecond)
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, s.setClock(clockRequest{Reset: true}))
	assert.WithinDuration(t, time.Now(), s.clock.Now(), time.Second)
}

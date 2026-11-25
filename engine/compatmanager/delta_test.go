package compatmanager_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/deltastores/deltastorestest"
	"github.com/target/goalert/engine/compatmanager"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/notification/slack"
	"github.com/target/goalert/user"
	"github.com/target/goalert/user/contactmethod"
)

type fakeSlack struct {
	asked int
	err   error
}

func (f *fakeSlack) Team(_ context.Context, id string) (*slack.Team, error) {
	f.asked++
	if f.err != nil {
		return nil, f.err
	}
	return &slack.Team{ID: id, Name: "Team " + id}, nil
}

func (f *fakeSlack) User(_ context.Context, id string) (*slack.User, error) {
	f.asked++
	if f.err != nil {
		return nil, f.err
	}
	return &slack.User{ID: id, TeamID: "T1"}, nil
}

func cycle(p *deltastorestest.Pod, db *compatmanager.DeltaDB) {
	p.T.Helper()
	db.Prepare(p.Ctx())
	p.Update(func(tx *filestore.Tx) error { return db.UpdateAll(p.Ctx(), tx) })
}

func TestDeltaSync(t *testing.T) {
	in := deltastorestest.New(t)
	p := in.Pod(t)
	var f fakeSlack
	db := compatmanager.NewDeltaDB(p.Delta, p.Tables, &f)
	alice, bob := p.AddUser("alice"), p.AddUser("bob")
	cycle(p, db)
	assert.Zero(t, f.asked, "there is nothing to ask")

	// An account gets a contact method.
	require.NoError(t, p.User.AddAuthSubjectTx(p.Admin(), nil, &user.AuthSubject{ProviderID: "slack:T1", SubjectID: "U1", UserID: alice}))
	require.NoError(t, p.User.AddAuthSubjectTx(p.Admin(), nil, &user.AuthSubject{ProviderID: "oidc", SubjectID: "U1", UserID: alice}))
	cycle(p, db)
	m, ok := p.Tables.ContactMethod.FindByTypeValue(p.View(), "SLACK_DM", "U1")
	require.True(t, ok)
	assert.Equal(t, "Team T1", m.Name)
	assert.Equal(t, alice, m.UserID)
	assert.False(t, m.Pending)
	dest, err := m.DestV1()
	require.NoError(t, err)
	assert.Equal(t, slack.NewDirectMessageDest("U1"), dest)
	assert.Equal(t, 1, p.Tables.User.AuthSubjectsByContactMethod.Count(p.View(), m.ID))

	// A contact method gets an account, which becomes its user's.
	_, err = p.Delta.Update(t.Context(), func(tx *filestore.Tx) error {
		return p.Tables.ContactMethod.Insert(tx, contactmethod.Row{Name: "dm", Type: "SLACK_DM", Value: "U2", UserID: bob}, -1)
	})
	require.NoError(t, err)
	cycle(p, db)
	owner, ok := p.Tables.User.AuthSubjectUserID(p.View(), "slack:T1", "U2")
	require.True(t, ok)
	assert.Equal(t, bob, owner)

	asked, before := f.asked, p.Delta.View().Index()
	cycle(p, db)
	assert.Equal(t, asked, f.asked)
	assert.Equal(t, before, p.Delta.View().Index())

	// The contact method of somebody else is used for the account.
	require.NoError(t, p.User.AddAuthSubjectTx(p.Admin(), nil, &user.AuthSubject{ProviderID: "slack:T2", SubjectID: "U2", UserID: alice}))
	cycle(p, db)
	m, _ = p.Tables.ContactMethod.FindByTypeValue(p.View(), "SLACK_DM", "U2")
	assert.Equal(t, 2, p.Tables.User.AuthSubjectsByContactMethod.Count(p.View(), m.ID))
}

// TestDeltaSlackDown checks that Slack is asked again after a while, not all the
// time.
func TestDeltaSlackDown(t *testing.T) {
	in := deltastorestest.New(t)
	p := in.Pod(t)
	f := fakeSlack{err: errors.New("down")}
	db := compatmanager.NewDeltaDB(p.Delta, p.Tables, &f)
	alice := p.AddUser("alice")
	require.NoError(t, p.User.AddAuthSubjectTx(p.Admin(), nil, &user.AuthSubject{ProviderID: "slack:T1", SubjectID: "U1", UserID: alice}))
	for range 3 {
		cycle(p, db)
	}
	assert.Equal(t, 1, f.asked)
	f.err = nil
	in.Clock.Advance(time.Minute)
	cycle(p, db)
	assert.Equal(t, 2, f.asked)
	_, ok := p.Tables.ContactMethod.FindByTypeValue(p.View(), "SLACK_DM", "U1")
	assert.True(t, ok)
}

package verifymanager_test

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/target/goalert/deltastores/deltastorestest"
	"github.com/target/goalert/engine/verifymanager"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/notification"
)

func update(p *deltastorestest.Pod) {
	p.T.Helper()
	p.Update(func(tx *filestore.Tx) error { return verifymanager.NewDeltaDB(p.Tables).UpdateAll(p.Ctx(), tx) })
}

// addCode adds a contact method, pending or not, with a code.
func addCode(p *deltastorestest.Pod, name string, pending bool, expires time.Time) (methodID, codeID string) {
	p.T.Helper()
	methodID, codeID = p.AddMethod(p.AddUser(name), name), uuid.NewString()
	p.Update(func(tx *filestore.Tx) error {
		row, _ := p.Tables.ContactMethod.Methods.Get(tx, methodID)
		row.Pending, row.Disabled = pending, pending
		if err := p.Tables.ContactMethod.Methods.Put(tx, row); err != nil {
			return err
		}
		return p.Tables.Notification.Codes.Put(tx, notification.VerificationCodeRow{
			ID: codeID, ContactMethodID: methodID, Code: 123456, ExpiresAt: expires,
		})
	})
	return methodID, codeID
}

func TestDeltaCodes(t *testing.T) {
	in := deltastorestest.New(t)
	p := in.Pod(t)
	now := in.Clock.Now()
	goneCM, gone := addCode(p, "gone", true, now)
	keptCM, kept := addCode(p, "kept", false, now.Add(-time.Hour))
	newCM, code := addCode(p, "new", true, now.Add(time.Second))

	update(p)
	r := p.View()
	_, ok := p.Tables.ContactMethod.Methods.Get(r, goneCM)
	assert.False(t, ok, "it was waiting for the code")
	_, ok = p.Tables.ContactMethod.Methods.Get(r, keptCM)
	assert.True(t, ok)
	for _, id := range []string{gone, kept} {
		_, ok = p.Tables.Notification.Codes.Get(r, id)
		assert.False(t, ok)
	}
	c, _ := p.Tables.Notification.Codes.Get(r, code)
	assert.True(t, c.Sent)
	msgs := p.Messages()
	if assert.Len(t, msgs, 1) {
		assert.Equal(t, "verification_message", msgs[0].MessageType)
		assert.Equal(t, newCM, *msgs[0].ContactMethodID)
		assert.Equal(t, code, *msgs[0].UserVerificationCodeID)
		assert.NotNil(t, msgs[0].UserID)
	}
	before := p.Delta.View().Index()
	update(p)
	assert.Equal(t, before, p.Delta.View().Index(), "a code is sent once")
	in.Clock.Advance(time.Second)
	update(p)
	_, ok = p.Tables.ContactMethod.Methods.Get(p.View(), newCM)
	assert.False(t, ok)
}

// TestDeltaRace checks that a code is sent once, however many pods look at it.
func TestDeltaRace(t *testing.T) {
	in := deltastorestest.New(t)
	a, b := in.Pod(t), in.Pod(t)
	for _, name := range []string{"first", "second", "third"} {
		addCode(a, name, true, in.Clock.Now().Add(time.Hour))
	}
	var wg sync.WaitGroup
	for _, p := range []*deltastorestest.Pod{a, b} {
		wg.Go(func() {
			_, err := p.Delta.Update(t.Context(), func(tx *filestore.Tx) error {
				return verifymanager.NewDeltaDB(p.Tables).UpdateAll(p.Ctx(), tx)
			})
			assert.NoError(t, err)
		})
	}
	wg.Wait()
	assert.Len(t, a.Messages(), 3)
}

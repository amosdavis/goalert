package compatmanager

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/target/goalert/deltastores"
	"github.com/target/goalert/engine/enginetables"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/limit"
	"github.com/target/goalert/notification/slack"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/user"
	"github.com/target/goalert/user/contactmethod"
	"github.com/target/goalert/util/log"
)

// SlackLookup is what is asked of Slack. *slack.ChannelSender is one.
type SlackLookup interface {
	Team(ctx context.Context, id string) (*slack.Team, error)
	User(ctx context.Context, id string) (*slack.User, error)
}

// DeltaDB keeps Slack contact methods and the users' Slack accounts together on
// the Delta backend. What it has to ask Slack it does before the transaction
// (F3).
type DeltaDB struct {
	db    *filestore.DeltaDB
	t     *deltastores.Tables
	slack SlackLookup

	mu      sync.Mutex
	failed  map[string]time.Time // when a lookup that failed is tried again
	teams   map[string]string    // the names of teams
	members map[string]slack.User
}

// NewDeltaDB returns a compatibility manager on the Delta backend.
func NewDeltaDB(db *filestore.DeltaDB, t *deltastores.Tables, slack SlackLookup) *DeltaDB {
	return &DeltaDB{db: db, t: t, slack: slack, failed: make(map[string]time.Time)}
}

// Name returns the name of the module.
func (db *DeltaDB) Name() string { return "Engine.CompatManager" }

const (
	providerPrefix = "slack:"
	typeSlackDM    = string(gadb.EnumUserContactMethodTypeSLACKDM)

	// maxLookups is how many of each kind are handled in a cycle, and prepareTime
	// how long that may take, so that Slack can't hold up pages (F15).
	maxLookups  = 10
	prepareTime = 3 * time.Second
	// retryLookup is how long after it failed a lookup is tried again.
	retryLookup = time.Minute
)

// subjects are the Slack accounts that have no contact method.
func (db *DeltaDB) subjects(r filestore.Reader) (out []user.AuthSubject) {
	db.t.User.EachWithoutMethod(r, providerPrefix, func(a user.AuthSubject) bool {
		if !db.waiting("team:" + a.ProviderID) {
			out = append(out, a)
		}
		return len(out) < maxLookups
	})
	return out
}

// methods are the Slack contact methods that have no account.
func (db *DeltaDB) methods(r filestore.Reader) (out []contactmethod.Row) {
	db.t.ContactMethod.EachOfType(r, typeSlackDM, func(m contactmethod.Row) bool {
		if !m.Disabled && db.t.User.AuthSubjectsByContactMethod.Count(r, m.ID) == 0 && !db.waiting("user:"+m.Value) {
			out = append(out, m)
		}
		return len(out) < maxLookups
	})
	return out
}

// waiting reports whether a lookup failed a short time ago.
func (db *DeltaDB) waiting(key string) bool { return db.db.Now().Before(db.failed[key]) }

// Prepare asks Slack what UpdateAll needs. It does nothing if that is nothing.
func (db *DeltaDB) Prepare(ctx context.Context) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.teams, db.members = make(map[string]string), make(map[string]slack.User)
	r := db.db.View()
	subjects, methods := db.subjects(r), db.methods(r)
	if len(subjects)+len(methods) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, prepareTime)
	defer cancel()
	failed := func(key string, err error) {
		log.Log(ctx, err)
		db.failed[key] = db.db.Now().Add(retryLookup)
	}
	for _, a := range subjects {
		if _, ok := db.teams[a.ProviderID]; ok || db.waiting("team:"+a.ProviderID) {
			continue
		}
		team, err := db.slack.Team(ctx, strings.TrimPrefix(a.ProviderID, providerPrefix))
		if err != nil {
			failed("team:"+a.ProviderID, fmt.Errorf("update contact methods: lookup Slack team (%s): %w", a.ProviderID, err))
			continue
		}
		db.teams[a.ProviderID] = team.Name
	}
	for _, m := range methods {
		u, err := db.slack.User(ctx, m.Value)
		if err != nil {
			failed("user:"+m.Value, fmt.Errorf("update auth subjects: lookup Slack user (%s): %w", m.Value, err))
			continue
		}
		db.members[m.Value] = *u
	}
}

// UpdateAll adds what is missing, as far as Prepare has found what it takes. One
// that can't be handled is logged, and stays as it is.
func (db *DeltaDB) UpdateAll(ctx context.Context, tx *filestore.Tx) error {
	if err := permission.LimitCheckAny(ctx, permission.System); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if len(db.teams)+len(db.members) == 0 {
		return nil
	}
	err := enginetables.Each(tx, db.subjects(tx), func(a user.AuthSubject) error {
		name, ok := db.teams[a.ProviderID]
		if !ok {
			return nil
		}
		return errors.Wrap(db.addMethod(tx, a, name), "update contact methods")
	})
	if err != nil {
		log.Log(ctx, err)
	}
	err = enginetables.Each(tx, db.methods(tx), func(m contactmethod.Row) error {
		u, ok := db.members[m.Value]
		if !ok {
			return nil
		}
		a := user.AuthSubject{ProviderID: providerPrefix + u.TeamID, SubjectID: u.ID, UserID: m.UserID}
		return errors.Wrap(db.t.User.SetAuthSubjectContactMethod(tx, a, m.ID), "update auth subjects")
	})
	if err != nil {
		log.Log(ctx, err)
	}
	return nil
}

func (db *DeltaDB) addMethod(tx *filestore.Tx, a user.AuthSubject, name string) error {
	m, ok := db.t.ContactMethod.FindByTypeValue(tx, typeSlackDM, a.SubjectID)
	if !ok {
		err := db.t.ContactMethod.Insert(tx, contactmethod.Row{
			Name: name, Type: typeSlackDM, Value: a.SubjectID, UserID: a.UserID,
		}, db.t.Limit.Max(tx, limit.ContactMethodsPerUser))
		if err != nil {
			return err
		}
		if m, ok = db.t.ContactMethod.FindByTypeValue(tx, typeSlackDM, a.SubjectID); !ok {
			return fmt.Errorf("the contact method of Slack user %s is missing", a.SubjectID)
		}
	}
	return db.t.User.SetAuthSubjectContactMethod(tx, a, m.ID)
}

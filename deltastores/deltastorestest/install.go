// Package deltastorestest has an install of GoAlert on the Delta backend, in
// memory, for tests.
package deltastorestest

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"
	_ "time/tzdata" // some systems have none

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/alert"
	"github.com/target/goalert/assignment"
	"github.com/target/goalert/config"
	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/deltastores"
	"github.com/target/goalert/escalation"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/integrationkey"
	"github.com/target/goalert/keyring"
	"github.com/target/goalert/notification"
	"github.com/target/goalert/notification/nfydest"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/schedule"
	"github.com/target/goalert/schedule/rotation"
	"github.com/target/goalert/schedule/rule"
	"github.com/target/goalert/service"
	"github.com/target/goalert/storagebackend"
	"github.com/target/goalert/user"
	"github.com/target/goalert/user/contactmethod"
	"github.com/target/goalert/user/notificationrule"
	"github.com/target/goalert/util/log"
	"github.com/target/goalert/util/timeutil"
)

// Start is the time at which an install's clock starts, which stands still.
var Start = time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)

// Install is a table in memory that several pods share.
type Install struct {
	Store *objstore.Mem
	Clock *filestore.FakeClock

	// Options are the pods'. The clock is the install's.
	Options filestore.DeltaOptions
	// Wrap, if set, is what a pod sees of the store.
	Wrap func(objstore.Store) objstore.Store

	pods int
}

// New creates a table.
func New(t testing.TB) *Install {
	t.Helper()
	in := &Install{
		Store: objstore.NewMem(), Clock: filestore.NewFakeClock(Start),
		Options: filestore.DeltaOptions{
			PollInterval: -1, ConflictBackoff: time.Millisecond, CommitBackoff: time.Millisecond, Logf: func(string, ...any) {},
		},
	}
	in.Clock.SetSpeed(0)
	in.Clock.Set(Start)
	require.NoError(t, deltastores.Create(t.Context(), in.Backend("admin"), in.options()))
	return in
}

func (in *Install) options() filestore.DeltaOptions {
	opts := in.Options
	opts.Clock = in.Clock
	return opts
}

// Backend is the table's storage, as a pod sees it.
func (in *Install) Backend(writer string) *storagebackend.Backend {
	store := objstore.Store(in.Store)
	if in.Wrap != nil {
		store = in.Wrap(store)
	}
	return &storagebackend.Backend{Store: store, Log: deltalog.New(store, writer)}
}

// Pod is a pod's stores.
type Pod struct {
	*deltastores.Stores

	T        testing.TB
	Install  *Install
	Delta    *filestore.DeltaDB
	Registry *nfydest.Registry
}

// Pod opens the table on another pod, with a destination type to notify
// (DestType).
func (in *Install) Pod(t testing.TB) *Pod {
	t.Helper()
	in.pods++
	ctx := permission.SystemContext(context.Background(), "test")
	db, tables, err := deltastores.Open(ctx, in.Backend(fmt.Sprintf("pod-%d", in.pods)), in.options())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	reg := nfydest.NewRegistry()
	reg.RegisterProvider(ctx, dests{})
	s, err := deltastores.New(ctx, db, tables, deltastores.Options{
		Logger: log.NewLogger(), Registry: reg,
		Config: config.StoreConfig{Keys: keyring.Keys{[]byte("key")}, FallbackURL: "http://localhost"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	reg.RegisterProvider(ctx, s.Schedule)
	reg.RegisterProvider(ctx, s.Rotation)
	reg.RegisterProvider(ctx, s.User)
	reg.RegisterProvider(ctx, s.Alert)
	return &Pod{Stores: s, T: t, Install: in, Delta: db, Registry: reg}
}

// Ctx is the system's.
func (p *Pod) Ctx() context.Context {
	return p.Config.Config().Context(permission.SystemContext(context.Background(), "test"))
}

// Admin is an admin's context.
func (p *Pod) Admin() context.Context {
	return p.As(uuid.Nil.String(), permission.RoleAdmin)
}

// As is the context of a user who is logged in.
func (p *Pod) As(userID string, role permission.Role) context.Context {
	return permission.UserSourceContext(p.Config.Config().Context(context.Background()), userID, role, &permission.SourceInfo{
		Type: permission.SourceTypeAuthProvider, ID: "test",
	})
}

// Update runs a transaction.
func (p *Pod) Update(fn func(tx *filestore.Tx) error) {
	p.T.Helper()
	_, err := p.Delta.Update(context.Background(), fn)
	require.NoError(p.T, err)
}

// Sync catches up with the other pods.
func (p *Pod) Sync() {
	p.T.Helper()
	p.Update(func(*filestore.Tx) error { return nil })
}

// View is the newest state.
func (p *Pod) View() filestore.Reader {
	p.T.Helper()
	p.Sync()
	return p.Delta.View()
}

// AddUser adds a user, and returns the id.
func (p *Pod) AddUser(name string) string {
	p.T.Helper()
	u, err := p.User.Insert(p.Ctx(), &user.User{Name: name, Email: name + "@example.com", Role: permission.RoleUser})
	require.NoError(p.T, err)
	return u.ID
}

// AddMethod adds a contact method of the test destination type, and returns the
// id.
func (p *Pod) AddMethod(userID, addr string) string {
	p.T.Helper()
	cm, err := p.ContactMethod.Create(p.Admin(), nil, &contactmethod.ContactMethod{
		Name: addr, UserID: userID, Dest: Dest(addr),
	})
	require.NoError(p.T, err)
	return cm.ID.String()
}

// AddRule notifies a user on a contact method after a delay in minutes.
func (p *Pod) AddRule(userID, methodID string, delay int) {
	p.T.Helper()
	_, err := p.NotificationRule.CreateTx(p.Admin(), nil, &notificationrule.NotificationRule{
		UserID: userID, ContactMethodID: uuid.MustParse(methodID), DelayMinutes: delay,
	})
	require.NoError(p.T, err)
}

// AddNotifiedUser adds a user with a contact method and a rule that uses it at
// once.
func (p *Pod) AddNotifiedUser(name string) (userID, methodID string) {
	p.T.Helper()
	userID = p.AddUser(name)
	methodID = p.AddMethod(userID, name)
	p.AddRule(userID, methodID, 0)
	return userID, methodID
}

// AddPolicy adds an escalation policy that repeats, and returns the id.
func (p *Pod) AddPolicy(name string, repeat int) string {
	p.T.Helper()
	pol, err := p.Escalation.CreatePolicyTx(p.Admin(), nil, &escalation.Policy{Name: name, Repeat: repeat})
	require.NoError(p.T, err)
	return pol.ID
}

// AddStep adds a step to an escalation policy, with a delay in minutes, and
// returns the id.
func (p *Pod) AddStep(policyID string, delay int, dests ...gadb.DestV1) string {
	p.T.Helper()
	step, err := p.Escalation.CreateStepTx(p.Admin(), nil, &escalation.Step{PolicyID: policyID, DelayMinutes: delay})
	require.NoError(p.T, err)
	ctx, err := p.NotificationChannel.ResolveDests(p.Admin(), dests...)
	require.NoError(p.T, err)
	for _, d := range dests {
		require.NoError(p.T, p.Escalation.AddStepActionTx(ctx, nil, step.ID, d))
	}
	return step.ID.String()
}

// AddService adds a service, and returns the id.
func (p *Pod) AddService(name, policyID string) string {
	p.T.Helper()
	svc, err := p.Service.CreateServiceTx(p.Admin(), nil, &service.Service{Name: name, EscalationPolicyID: policyID})
	require.NoError(p.T, err)
	return svc.ID
}

// AddKey adds an integration key, and returns the id.
func (p *Pod) AddKey(serviceID string) string {
	p.T.Helper()
	key, err := p.IntegrationKey.Create(p.Admin(), nil, &integrationkey.IntegrationKey{
		Name: "key of " + serviceID, Type: integrationkey.TypeGeneric, ServiceID: serviceID,
	})
	require.NoError(p.T, err)
	return key.ID
}

// AddAlert adds an alert, and returns the id.
func (p *Pod) AddAlert(serviceID, summary string) int64 {
	p.T.Helper()
	a, err := p.Alert.CreateTx(p.Admin(), nil, &alert.Alert{Summary: summary, ServiceID: serviceID, Source: alert.SourceManual})
	require.NoError(p.T, err)
	return int64(a.ID)
}

// AddSchedule adds a schedule in UTC, and returns the id.
func (p *Pod) AddSchedule(name string) string {
	p.T.Helper()
	s, err := p.Schedule.CreateScheduleTx(p.Admin(), nil, &schedule.Schedule{Name: name, TimeZone: time.UTC})
	require.NoError(p.T, err)
	return s.ID
}

// AddScheduleRule puts a user or a rotation on a schedule, always.
func (p *Pod) AddScheduleRule(scheduleID string, tgt assignment.Target) string {
	p.T.Helper()
	r, err := p.Rule.CreateRuleTx(p.Admin(), nil, &rule.Rule{ScheduleID: scheduleID, WeekdayFilter: timeutil.EveryDay(), Target: tgt})
	require.NoError(p.T, err)
	return r.ID
}

// AddRotation adds a rotation of users, and returns the id.
func (p *Pod) AddRotation(name string, typ rotation.Type, length int, start time.Time, userIDs ...string) string {
	p.T.Helper()
	rot, err := p.Rotation.CreateRotationTx(p.Admin(), nil, &rotation.Rotation{Name: name, Type: typ, ShiftLength: length, Start: start})
	require.NoError(p.T, err)
	if len(userIDs) > 0 {
		require.NoError(p.T, p.Rotation.AddRotationUsersTx(p.Admin(), nil, rot.ID, userIDs))
	}
	return rot.ID
}

// Messages are the outgoing messages, oldest first.
func (p *Pod) Messages() []notification.MessageRow {
	p.T.Helper()
	var out []notification.MessageRow
	p.Tables.Notification.Messages.Ascend(p.View(), func(m notification.MessageRow) bool {
		out = append(out, m)
		return true
	})
	slices.SortStableFunc(out, func(a, b notification.MessageRow) int { return a.CreatedAt.Compare(b.CreatedAt) })
	return out
}

// DestType is a destination type that only tests have.
const DestType = "test-dest"

// Dest is a destination of that type.
func Dest(addr string) gadb.DestV1 { return gadb.NewDestV1(DestType, "addr", addr) }

// ScheduleDest, RotationDest and UserDest are destinations of escalation
// policies.
func ScheduleDest(id string) gadb.DestV1 { return schedule.DestFromID(id) }
func RotationDest(id string) gadb.DestV1 { return rotation.DestFromID(id) }
func UserDest(id string) gadb.DestV1     { return user.DestFromID(id) }

// dests is the provider of DestType.
type dests struct{}

func (dests) ID() string { return DestType }

func (dests) TypeInfo(context.Context) (*nfydest.TypeInfo, error) {
	return &nfydest.TypeInfo{
		Type: DestType, Name: "Test", Enabled: true,
		SupportsAlertNotifications: true, SupportsStatusUpdates: true, SupportsUserVerification: true,
		SupportsOnCallNotify: true, SupportsSignals: true,
		RequiredFields: []nfydest.FieldConfig{{FieldID: "addr", Label: "Address"}},
	}, nil
}

func (dests) ValidateField(context.Context, string, string) error { return nil }

func (dests) DisplayInfo(_ context.Context, args map[string]string) (*nfydest.DisplayInfo, error) {
	return &nfydest.DisplayInfo{Text: args["addr"]}, nil
}

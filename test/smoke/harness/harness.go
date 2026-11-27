package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	stdlog "log"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"text/template"
	"time"
	_ "time/tzdata" // like cmd/goalert; not every machine has zone data

	"github.com/google/uuid"
	"github.com/pkg/errors"
	sloglogrus "github.com/samber/slog-logrus/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/alert"
	"github.com/target/goalert/app"
	"github.com/target/goalert/config"
	"github.com/target/goalert/devtools/mocktwilio"
	"github.com/target/goalert/expflag"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/user"
	"github.com/target/goalert/user/notificationrule"
	"github.com/target/goalert/util/log"
	"github.com/target/goalert/util/timeutil"
)

// parseExpFlags parses a comma-separated list of experimental flag names (as
// used by `goalert --experimental`), ignoring empty entries and surrounding
// whitespace.
func parseExpFlags(s string) expflag.FlagSet {
	var fs expflag.FlagSet
	for _, f := range strings.Split(s, ",") {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		fs = append(fs, expflag.Flag(f))
	}
	return fs
}

// mergeExpFlags returns the union of the given flag sets, preserving order and
// dropping duplicates.
func mergeExpFlags(sets ...expflag.FlagSet) expflag.FlagSet {
	var result expflag.FlagSet
	for _, fs := range sets {
		for _, f := range fs {
			if result.Has(f) {
				continue
			}
			result = append(result, f)
		}
	}
	return result
}

// envExpFlags returns the experimental flags requested via the SMOKE_EXP_FLAGS
// environment variable (comma-separated). These are merged into the flags of
// every harness so the whole suite can be run against an experimental code
// path without editing tests.
func envExpFlags() expflag.FlagSet {
	return parseExpFlags(os.Getenv("SMOKE_EXP_FLAGS"))
}

// Harness is a helper for smoketests. It deals with assertions, database management, and backend monitoring during tests.
type Harness struct {
	phoneCCG, uuidG, emailG *DataGen
	t                       *testing.T
	closing                 bool

	storage storage

	msgSvcID string
	expFlags expflag.FlagSet

	tw  *twilioAssertionAPI
	twS *httptest.Server

	cfg config.Config

	appCfg app.Config
	url    string

	email *emailServer

	ignoreErrors []string

	backend     *app.App
	backendLogs io.Closer
	logsDone    chan struct{}
	deltaPods   *deltaPodSet

	mx sync.Mutex

	userGeneratedIndex int

	gqlSessions map[string]string
}

func (h *Harness) Config() config.Config {
	return h.cfg
}

// NewHarness will create a new database, perform `migrateSteps` migrations, inject `initSQL` and return a new Harness bound to
// the result. It starts a backend process pre-configured to a mock twilio server for monitoring notifications as well.
func NewHarness(t *testing.T, initSQL, migrationName string) *Harness {
	t.Helper()
	return NewHarnessWithFlags(t, initSQL, migrationName, nil)
}

// NewHarnessWithFlags is the same as NewHarness, but allows passing in a set of experimental flags to be used for the test.
func NewHarnessWithFlags(t *testing.T, initSQL, migrationName string, fs expflag.FlagSet) *Harness {
	stdlog.SetOutput(io.Discard)
	t.Helper()
	h := NewStoppedHarnessWithFlags(t, initSQL, nil, migrationName, fs)
	h.Start()
	return h
}

func (h *Harness) App() *app.App { return h.backend }

func NewHarnessWithData(t *testing.T, initSQL string, sqlData interface{}, migrationName string) *Harness {
	t.Helper()
	h := NewStoppedHarness(t, initSQL, sqlData, migrationName)
	h.Start()
	return h
}

// NewHarnessDebugDB works like NewHarness, but fails the test immediately after
// the fixtures have been loaded. It is used to debug data & queries from a smoketest.
//
// Note that the clock will be frozen at the init timestamp for inspection.
func NewHarnessDebugDB(t *testing.T, initSQL, migrationName string) *Harness {
	t.Helper()
	_ = NewStoppedHarness(t, initSQL, nil, migrationName)
	t.Fatal("DEBUG DB :: fixtures loaded")
	return nil
}

const (
	twilioAuthToken  = "11111111111111111111111111111111"
	twilioAccountSID = "AC00000000000000000000000000000000"
)

// NewStoppedHarness will create a NewHarness, but will not call Start.
func NewStoppedHarness(t *testing.T, initSQL string, sqlData interface{}, migrationName string) *Harness {
	t.Helper()
	return NewStoppedHarnessWithFlags(t, initSQL, sqlData, migrationName, nil)
}

// NewStoppedHarnessWithFlags is the same as NewStoppedHarness, but allows
// passing in a set of experimental flags to be used for the test.
//
// Fixtures load at the latest schema; migrationName only records the schema a
// fixture was first written for.
func NewStoppedHarnessWithFlags(t *testing.T, initSQL string, sqlData interface{}, migrationName string, expFlags expflag.FlagSet) *Harness {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping Harness tests for short mode")
	}

	if env := envExpFlags(); len(env) > 0 {
		t.Logf("Using experimental flags from SMOKE_EXP_FLAGS: %v", env)
		expFlags = mergeExpFlags(expFlags, env)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	st, err := newStorage(ctx, t)
	if err != nil {
		t.Fatal(err)
	}

	twCfg := mocktwilio.Config{
		AuthToken:    twilioAuthToken,
		AccountSID:   twilioAccountSID,
		MinQueueTime: 100 * time.Millisecond, // until we have a stateless backend for answering calls
	}

	h := &Harness{
		uuidG:    NewDataGen(t, "UUID", DataGenFunc(GenUUID)),
		phoneCCG: NewDataGen(t, "Phone", DataGenArgFunc(GenPhoneCC)),
		emailG:   NewDataGen(t, "Email", DataGenFunc(func() string { return GenUUID() + "@example.com" })),
		storage:  st,

		gqlSessions: make(map[string]string),

		expFlags: expFlags,

		t: t,
	}
	h.email = newEmailServer(h)

	h.tw = newTwilioAssertionAPI(func() {
		h.FastForward(time.Minute)
		h.Trigger()
	}, func(num string) string {
		id, ok := h.phoneCCG.names[num]
		if !ok {
			return num
		}

		return fmt.Sprintf("%s/Phone(%s)", num, id)
	}, mocktwilio.NewServer(twCfg), h.phoneCCG.Get("twilio"))

	h.twS = httptest.NewServer(h.tw)

	h.execQuery(initSQL, sqlData)

	return h
}

func (h *Harness) Start() {
	h.t.Helper()
	h.StartWithAppCfgHook(nil)
}

func (h *Harness) StartWithAppCfgHook(fn func(*app.Config)) {
	h.t.Helper()

	var cfg config.Config
	cfg.General.DisableMessageBundles = true
	cfg.Twilio.Enable = true
	cfg.Twilio.AccountSID = twilioAccountSID
	cfg.Twilio.AuthToken = twilioAuthToken
	cfg.Twilio.FromNumber = h.phoneCCG.Get("twilio")

	cfg.SMTP.Enable = true
	cfg.SMTP.Address = h.email.Addr()
	cfg.SMTP.DisableTLS = true
	cfg.SMTP.From = "goalert-test@localhost"

	cfg.Webhook.Enable = true
	h.cfg = cfg

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := h.storage.SetClockSpeed(ctx, 1)
	if err != nil {
		h.t.Fatalf("resume flow of time: %v", err)
	}

	appCfg := app.Defaults()
	appCfg.ExpFlags = h.expFlags
	appCfg.LegacyLogger = log.NewLogger()
	appCfg.Logger = slog.New(sloglogrus.Option{
		Logger: appCfg.LegacyLogger.Logrus(),
	}.NewLogrusHandler())
	appCfg.ListenAddr = "localhost:0"
	appCfg.Verbose = true
	appCfg.JSON = true
	appCfg.TwilioBaseURL = h.twS.URL
	appCfg.InitialConfig = &h.cfg
	h.appCfg = appCfg

	r, w := io.Pipe()
	h.backendLogs = w
	h.logsDone = make(chan struct{})

	appCfg.LegacyLogger.EnableJSON()
	appCfg.LegacyLogger.SetOutput(w)

	go h.watchBackendLogs(r)

	if fn != nil {
		fn(&appCfg)
	}

	h.backend, err = h.storage.NewApp(ctx, appCfg)
	if err != nil {
		h.t.Fatalf("failed to start backend: %v", err)
	}
	h.TwilioNumber("") // register default number

	h.runBackend(h.backend)
	err = h.backend.WaitForStartup(ctx)
	if err != nil {
		h.t.Fatalf("failed to start backend: %v", err)
	}
}

// RestartGoAlertWithConfig will restart the backend with the provided config.
func (h *Harness) RestartGoAlertWithConfig(cfg config.Config) {
	h.t.Helper()
	h.RestartGoAlertWithAppCfgHook(func(c *app.Config) {
		c.InitialConfig = &cfg
	})
}

// RestartGoAlertWithAppCfgHook will restart the backend with the provided config hook.
func (h *Harness) RestartGoAlertWithAppCfgHook(reconfig func(*app.Config)) {
	h.t.Helper()

	h.t.Logf("Stopping backend for restart")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := h.shutdownBackend(ctx, h.backend)
	if err != nil {
		h.t.Error("failed to shutdown backend cleanly:", err)
	}

	h.t.Logf("Restarting backend")
	reconfig(&h.appCfg)
	h.backend, err = h.storage.NewApp(ctx, h.appCfg)
	if err != nil {
		h.t.Fatalf("failed to start backend: %v", err)
	}

	h.runBackend(h.backend)
	err = h.backend.WaitForStartup(ctx)
	if err != nil {
		h.t.Fatalf("failed to start backend: %v", err)
	}

	h.t.Logf("Backend restarted")
}

// SetURL routes subsequent harness HTTP requests through url instead of the
// current backend. Passing an empty string restores direct backend requests.
func (h *Harness) SetURL(url string) { h.url = url }

// URL returns the URL harness HTTP requests use.
func (h *Harness) URL() string {
	if h.url != "" {
		return h.url
	}
	return h.backend.URL()
}

// IgnoreErrorsWith will cause the Harness to ignore backend errors containing the specified substring.
func (h *Harness) IgnoreErrorsWith(substr string) {
	h.mx.Lock()
	defer h.mx.Unlock()
	h.ignoreErrors = append(h.ignoreErrors, substr)
}

// Now returns the current time, as observed by the DB.
func (h *Harness) Now() time.Time {
	h.t.Helper()

	now, err := h.storage.Now(context.Background())
	require.NoError(h.t, err, "get now()")

	return now
}

// FastForwardToTime will fast-forward the database time to the next occurrence of the provided clock time.
func (h *Harness) FastForwardToTime(t timeutil.Clock, zoneName string) {
	h.t.Helper()

	zone, err := time.LoadLocation(zoneName)
	require.NoError(h.t, err, "load location")

	now := h.Now()

	y, m, d := now.In(zone).Date()
	dst := time.Date(y, m, d, t.Hour(), t.Minute(), 0, 0, zone)
	if !dst.After(now) {
		dst = dst.AddDate(0, 0, 1)
	}

	h.FastForward(dst.Sub(now))
}

func (h *Harness) FastForward(d time.Duration) {
	h.t.Helper()
	h.t.Logf("Fast-forward %s", d.String())
	err := h.storage.AdvanceClock(context.Background(), d)
	if err != nil {
		h.t.Fatalf("failed to fast-forward time: %v", err)
	}
}

func (h *Harness) execQuery(sql string, data interface{}) {
	h.t.Helper()
	t := template.New("sql")
	t.Funcs(template.FuncMap{
		"uuidJSON": func(id string) string { return fmt.Sprintf(`"%s"`, h.uuidG.Get(id)) },
		"uuid":     func(id string) string { return fmt.Sprintf("'%s'", h.uuidG.Get(id)) },
		"phone":    func(id string) string { return fmt.Sprintf("'%s'", h.phoneCCG.Get(id)) },
		"email":    func(id string) string { return fmt.Sprintf("'%s'", h.emailG.Get(id)) },
		"phoneCC":  func(cc, id string) string { return fmt.Sprintf("'%s'", h.phoneCCG.GetWithArg(cc, id)) },

		// autoDedupKey returns the dedup key GoAlert gives an open alert
		// with no explicit dedup ID. Fixtures that insert open alerts need
		// it: the database no longer fills it in.
		"autoDedupKey": func(summary string, details ...string) (string, error) {
			a := alert.Alert{Summary: summary}
			if len(details) > 0 {
				a.Details = details[0]
			}
			v, err := a.DedupKey().Value()
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("'%s'", v), nil
		},
	})
	_, err := t.Parse(sql)
	if err != nil {
		h.t.Fatalf("failed to parse query template: %v", err)
	}

	b := new(bytes.Buffer)
	err = t.Execute(b, data)
	if err != nil {
		h.t.Fatalf("failed to render query template: %v", err)
	}

	err = h.storage.Exec(context.Background(), b.String())
	if err != nil {
		h.t.Fatalf("failed to exec query: %v\n%s", err, b.String())
	}
}

// Exec runs SQL against the test's storage, with the same template functions
// as fixtures (such as {{uuid "name"}}). Use only the statements fixtures
// use: INSERT ... VALUES, UPDATE ... SET ... WHERE column = value, and DELETE.
func (h *Harness) Exec(sql string) {
	h.t.Helper()
	h.execQuery(sql, nil)
}

// CreateAlerts will create one or more unacknowledged alerts for a service.
func (h *Harness) CreateAlert(serviceID string, summary string) TestAlert {
	h.t.Helper()

	return h.CreateAlertWithDetails(serviceID, summary, "")
}

type TestAlert interface {
	ID() int
	Ack()
	Escalate()
	Close()
}
type testAlert struct {
	h *Harness
	a alert.Alert
}

func (t testAlert) setStatus(stat alert.Status) {
	t.h.t.Helper()
	permission.SudoContext(context.Background(), func(ctx context.Context) {
		t.h.t.Helper()
		t.a.Status = stat

		result, isNew, err := t.h.backend.AlertStore.CreateOrUpdate(ctx, &t.a)
		require.NoErrorf(t.h.t, err, "set alert to %s", stat)
		require.False(t.h.t, isNew, "not be new")
		require.NotNil(t.h.t, result)
	})
}

func (t testAlert) ID() int { return t.a.ID }
func (t testAlert) Close()  { t.setStatus(alert.StatusClosed) }
func (t testAlert) Ack()    { t.setStatus(alert.StatusActive) }
func (t testAlert) Escalate() {
	t.h.t.Helper()
	permission.SudoContext(context.Background(), func(ctx context.Context) {
		t.h.t.Helper()

		err := t.h.backend.AlertStore.Escalate(ctx, t.a.ID, 0)
		require.NoErrorf(t.h.t, err, "escalate alert %d", t.a.ID)
	})
}

// CreateAlertWithDetails will create a single alert with summary and detailss.
func (h *Harness) CreateAlertWithDetails(serviceID, summary, details string) TestAlert {
	h.t.Helper()

	var newAlert alert.Alert
	permission.SudoContext(context.Background(), func(ctx context.Context) {
		h.t.Helper()
		a := &alert.Alert{
			ServiceID: serviceID,
			Summary:   summary,
			Details:   details,
		}

		h.t.Logf("insert alert: %v", a)
		_newAlert, isNew, err := h.backend.AlertStore.CreateOrUpdate(ctx, a)
		if err != nil {
			h.t.Fatalf("failed to insert alert: %v", err)
		}
		if !isNew {
			h.t.Fatal("could not create duplicate alert with summary: " + summary)
		}
		newAlert = *_newAlert
	})

	return testAlert{h: h, a: newAlert}
}

// AddNotificationRule will add a notification rule to the database.
func (h *Harness) AddNotificationRule(userID, cmID string, delayMinutes int) {
	h.t.Helper()
	nr := &notificationrule.NotificationRule{
		DelayMinutes:    delayMinutes,
		UserID:          userID,
		ContactMethodID: uuid.MustParse(cmID),
	}
	h.t.Logf("insert notification rule: %v", nr)
	permission.SudoContext(context.Background(), func(ctx context.Context) {
		h.t.Helper()
		_, err := h.backend.NotificationRuleStore.Insert(ctx, nr)
		if err != nil {
			h.t.Fatalf("failed to insert notification rule: %v", err)
		}
	})
}

// runEngineCycle runs, and waits for, one engine cycle on each app. With no
// apps it uses the current backend.
func (h *Harness) runEngineCycle(apps ...*app.App) {
	h.t.Helper()
	if len(apps) == 0 {
		apps = []*app.App{h.backend}
	}
	for _, a := range apps {
		id := a.Engine.NextCycleID()
		go a.Engine.Trigger()
		require.NoError(h.t, a.Engine.WaitCycleID(context.Background(), id))
	}
}

// Trigger will trigger, and wait for, an engine cycle. Every running pod gets
// a cycle, so that the one holding the engine's lease does the work.
func (h *Harness) Trigger() {
	h.t.Helper()

	apps := []*app.App{h.backend}
	if h.deltaPods != nil {
		apps = h.deltaPods.active()
	}
	h.runEngineCycle(apps...)
}

// Escalate will escalate an alert in the database, when 'level' matches.
func (h *Harness) Escalate(alertID, level int) {
	h.t.Helper()
	err := h.EscalateAlertErr(alertID)
	require.NoError(h.t, err, "escalate alert")
}

func (h *Harness) EscalateAlertErr(alertID int) (err error) {
	h.t.Helper()
	h.t.Logf("escalate alert #%d", alertID)
	permission.SudoContext(context.Background(), func(ctx context.Context) {
		h.t.Helper()
		err = h.backend.AlertStore.Escalate(ctx, alertID, -1)
	})
	return err
}

// Phone will return the generated phone number for the id provided.
func (h *Harness) Phone(id string) string { return h.phoneCCG.Get(id) }

// PhoneCC will return the generated phone number for the id provided.
func (h *Harness) PhoneCC(cc, id string) string { return h.phoneCCG.GetWithArg(cc, id) }

// UUID will return the generated UUID for the id provided.
func (h *Harness) UUID(id string) string { return h.uuidG.Get(id) }

func (h *Harness) isClosing() bool {
	h.mx.Lock()
	defer h.mx.Unlock()
	return h.closing
}

func (h *Harness) dumpDB() {
	testName := reflect.ValueOf(h.t).Elem().FieldByName("name").String()
	file := filepath.Join("smoketest_db_dump", testName+".sql")
	file, err := filepath.Abs(file)
	if err != nil {
		h.t.Fatalf("failed to get abs dump path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
		h.t.Fatalf("failed to create abs dump path: %v", err)
	}

	fd, err := os.Create(file)
	if err != nil {
		h.t.Fatalf("failed to open dump file: %v", err)
	}
	defer fd.Close()

	// A failed dump fails the test but doesn't stop the rest of Close.
	if err := h.storage.Dump(context.Background(), fd); err != nil {
		h.t.Errorf("dump %s: %v", h.storage, err)
	}
}

// Close terminates any background processes, and drops the testing database.
// It should be called at the end of all tests (usually with `defer h.Close()`).
func (h *Harness) Close() {
	h.t.Helper()
	if recErr := recover(); recErr != nil {
		defer panic(recErr)
	}

	// Deferred so it still runs if a later WaitAndAssert or Shutdown call
	// triggers Goexit (e.g. via t.Fatalf). Closes the log pipe writer and
	// blocks until watchBackendLogs has drained, preventing it from calling
	// t.Logf after the test has been marked complete.
	defer func() {
		h.mx.Lock()
		h.closing = true
		h.mx.Unlock()
		if h.backendLogs != nil {
			h.backendLogs.Close()
		}
		if h.logsDone != nil {
			<-h.logsDone
		}
	}()

	h.dumpDB() // early as possible

	// If backend startup never completed, Engine is nil and Trigger would panic;
	// skip assertions that depend on driving engine cycles.
	if h.backend != nil && h.backend.Engine != nil {
		h.tw.WaitAndAssert(h.t)
		h.email.WaitAndAssert()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := h.backend.Shutdown(ctx)
	if err != nil {
		h.t.Error("failed to shutdown backend cleanly:", err)
	}

	h.twS.Close()

	h.tw.Close()

	if err := h.storage.Close(ctx); err != nil {
		h.t.Error(err)
	}
}

// TwilioNumber will return a registered (or register if missing) Twilio number for the given ID.
// The default FromNumber will always be the empty ID.
func (h *Harness) TwilioNumber(id string) string {
	if id != "" {
		id = ":" + id
	}
	num := h.phoneCCG.Get("twilio" + id)

	err := h.tw.RegisterSMSCallback(num, h.URL()+"/api/v2/twilio/message")
	if err != nil {
		h.t.Fatalf("failed to init twilio (SMS callback): %v", err)
	}
	err = h.tw.RegisterVoiceCallback(num, h.URL()+"/api/v2/twilio/call")
	if err != nil {
		h.t.Fatalf("failed to init twilio (voice callback): %v", err)
	}

	return num
}

// TwilioMessagingService will return the id and phone numbers for the mock messaging service.
func (h *Harness) TwilioMessagingService() string {
	h.mx.Lock()
	if h.msgSvcID != "" {
		h.mx.Unlock()
		return h.msgSvcID
	}
	defer h.mx.Unlock()

	nums := []string{h.phoneCCG.Get("twilio:sid1"), h.phoneCCG.Get("twilio:sid2"), h.phoneCCG.Get("twilio:sid3")}
	newID, err := h.tw.NewMessagingService(h.URL()+"/api/v2/twilio/message", nums...)
	if err != nil {
		panic(err)
	}

	h.msgSvcID = newID
	return newID
}

// CreateUser generates a random user.
func (h *Harness) CreateUser() (u *user.User) {
	h.t.Helper()
	var err error
	permission.SudoContext(context.Background(), func(ctx context.Context) {
		u, err = h.backend.UserStore.Insert(ctx, &user.User{
			Name:  fmt.Sprintf("Generated%d", h.userGeneratedIndex),
			ID:    uuid.New().String(),
			Role:  permission.RoleUser,
			Email: fmt.Sprintf("generated%d@example.com", h.userGeneratedIndex),
		})
	})
	if err != nil {
		h.t.Fatal(errors.Wrap(err, "generate random user"))
	}
	h.userGeneratedIndex++
	return u
}

// WaitAndAssertOnCallUsers will ensure the correct set of users as on-call for the given serviceID.
func (h *Harness) WaitAndAssertOnCallUsers(serviceID string, userIDs ...string) {
	h.t.Helper()
	doQL := func(query string, res interface{}) {
		g := h.GraphQLQuery2(query)
		for _, err := range g.Errors {
			h.t.Error("GraphQL Error:", err.Message)
		}
		if len(g.Errors) > 0 {
			h.t.Fatal("errors returned from GraphQL")
		}
		if res == nil {
			return
		}
		err := json.Unmarshal(g.Data, &res)
		if err != nil {
			h.t.Fatal("failed to parse response:", err)
		}
	}

	getUsers := func() []string {
		var result struct {
			Service struct {
				OnCallUsers []struct {
					UserID   string
					UserName string
				}
			}
		}

		doQL(fmt.Sprintf(`
			query{
				service(id: "%s"){
					onCallUsers{
						userID
						userName
					}
				}
			}
		`, serviceID), &result)

		var ids []string
		for _, oc := range result.Service.OnCallUsers {
			ids = append(ids, oc.UserID)
		}
		if len(ids) == 0 {
			return nil
		}
		sort.Strings(ids)
		uniq := ids[:1]
		last := ids[0]
		for _, id := range ids[1:] {
			if id == last {
				continue
			}
			uniq = append(uniq, id)
			last = id
		}
		return uniq
	}
	sort.Strings(userIDs)
	check := func(t *assert.CollectT) {
		// Run an engine cycle before each check (not just once up front) so
		// on-call state produced by River jobs that finish between polls is
		// picked up (see Trigger).
		h.Trigger()

		ids := getUsers()
		require.Lenf(t, ids, len(userIDs), "number of on-call users")
		require.ElementsMatch(t, userIDs, ids, "on-call users")
	}

	assert.EventuallyWithT(h.t, check, 30*time.Second, time.Second)
}

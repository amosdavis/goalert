package deltastores

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/target/goalert/alert"
	"github.com/target/goalert/alert/alertlog"
	"github.com/target/goalert/alert/alertmetrics"
	"github.com/target/goalert/apikey"
	"github.com/target/goalert/assignment"
	"github.com/target/goalert/auth"
	"github.com/target/goalert/auth/authlink"
	"github.com/target/goalert/auth/basic"
	"github.com/target/goalert/auth/nonce"
	"github.com/target/goalert/calsub"
	"github.com/target/goalert/config"
	"github.com/target/goalert/escalation"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/heartbeat"
	"github.com/target/goalert/integrationkey"
	"github.com/target/goalert/keyring"
	"github.com/target/goalert/label"
	"github.com/target/goalert/limit"
	"github.com/target/goalert/notice"
	"github.com/target/goalert/notification"
	"github.com/target/goalert/notification/nfydest"
	"github.com/target/goalert/notification/twilio"
	"github.com/target/goalert/notificationchannel"
	"github.com/target/goalert/oncall"
	"github.com/target/goalert/override"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/schedule"
	"github.com/target/goalert/schedule/rotation"
	"github.com/target/goalert/schedule/rule"
	"github.com/target/goalert/service"
	"github.com/target/goalert/timezone"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/user"
	"github.com/target/goalert/user/contactmethod"
	"github.com/target/goalert/user/favorite"
	"github.com/target/goalert/user/notificationrule"
	"github.com/target/goalert/util/log"
)

// Stores are GoAlert's stores on the Delta backend.
type Stores struct {
	DB     txn.DB
	Tables *Tables

	Config *config.Store

	OAuthKeyring, SessionKeyring, APIKeyring, AuthLinkKeyring keyring.Keyring

	Alert               *alert.Store
	AlertLog            *alertlog.Store
	AlertMetrics        *alertmetrics.Store
	APIKey              *apikey.Store
	AuthBasic           *basic.Store
	AuthLink            *authlink.Store
	CalSub              *calsub.Store
	ContactMethod       *contactmethod.Store
	Escalation          *escalation.Store
	Favorite            *favorite.Store
	Heartbeat           *heartbeat.Store
	IntegrationKey      *integrationkey.Store
	Label               *label.Store
	Limit               *limit.Store
	Nonce               *nonce.Store
	Notice              *notice.Store
	Notification        *notification.Store
	NotificationChannel *notificationchannel.Store
	NotificationRule    *notificationrule.Store
	OnCall              *oncall.Store
	Override            *override.Store
	Rotation            *rotation.Store
	Rule                *rule.Store
	Schedule            *schedule.Store
	Service             *service.Store
	TimeZone            *timezone.Store
	User                *user.Store
}

// Options are what the stores need besides the table.
type Options struct {
	Logger   *log.Logger
	Registry *nfydest.Registry

	// Config is the config store's configuration, without a database. Its keys
	// also encrypt the keyrings.
	Config config.StoreConfig

	// NoSecrets leaves out the config store and the keyrings, which would be
	// created with the wrong keys, for a program that only works with the rest
	// of the data. What signs or checks tokens does not work without them.
	NoSecrets bool
}

// New builds GoAlert's stores on a Delta table whose schema has these tables.
// Close stops what some of them run in the background.
func New(ctx context.Context, delta *filestore.DeltaDB, t *Tables, opts Options) (_ *Stores, err error) {
	db := deltatxn.DB{DB: delta}
	s := &Stores{DB: db, Tables: t}
	defer func() {
		if err != nil {
			_ = s.Close(context.WithoutCancel(ctx))
		}
	}()

	if !opts.NoSecrets {
		if err := s.initSecrets(ctx, delta, opts); err != nil {
			return nil, err
		}
	}

	limits := t.Limit.MaxFunc
	s.Limit = limit.NewDeltaStore(db, t.Limit)
	s.Nonce = nonce.NewDeltaStore(opts.Logger, db, t.Nonce)
	s.TimeZone = timezone.NewStore(ctx, nil)
	s.Favorite = favorite.NewDeltaStore(db, t.Favorite)
	s.Label = label.NewDeltaStore(db, t.Label)
	s.NotificationChannel = notificationchannel.NewDeltaStore(db, t.NotificationChannel, opts.Registry)
	s.ContactMethod = contactmethod.NewDeltaStore(db, t.ContactMethod, opts.Registry, limits(limit.ContactMethodsPerUser))
	s.NotificationRule = notificationrule.NewDeltaStore(db, t.NotificationRule, t.ContactMethod.Owner, limits(limit.NotificationRulesPerUser))
	s.Heartbeat = heartbeat.NewDeltaStore(db, t.Heartbeat, limits(limit.HeartbeatMonitorsPerService))
	s.Override = override.NewDeltaStore(db, t.Override, limits(limit.UserOverridesPerSchedule))
	s.Rule = rule.NewDeltaStore(db, t.Rule, rule.DeltaLimits{
		RulesPerSchedule:   limits(limit.RulesPerSchedule),
		TargetsPerSchedule: limits(limit.TargetsPerSchedule),
	})
	// Nothing has to follow a change of a rotation: the engine looks at them
	// all (F52). Nor of an alert's status, or a new signal, below.
	s.Rotation = rotation.NewDeltaStore(db, t.Rotation, t.Favorite.IsRotationFavorite, limits(limit.ParticipantsPerRotation), nil)
	s.Schedule = schedule.NewDeltaStore(db, t.Schedule, schedule.DeltaLookups{
		IsFavorite:      t.Favorite.IsScheduleFavorite,
		UserExists:      t.userExists,
		SchedulesOfUser: t.schedulesOfUser,
	})
	s.User = user.NewDeltaStore(db, t.User, t.Favorite.IsUserFavorite,
		user.WithContactMethods(t.usersByDest),
		user.WithRotations(s.Rotation.RemoveUserDelta),
		user.WithOnCall(user.OnCallFuncs{Steps: t.OnCall.UserSteps, Step: t.onCallStep, Services: t.onCallServices}),
	)
	s.OnCall = oncall.NewDeltaStore(db, t.OnCall, s.Rule, s.Schedule, oncall.DeltaLookups{
		ScheduleTimeZone: t.Schedule.TimeZone,
		UserName:         t.userName,
		ServiceSteps:     t.serviceSteps,
		Rotation:         t.rotation,
		Overrides:        t.overrides,
	})
	s.Service = service.NewDeltaStore(db, t.Service, service.DeltaLookups{
		EscalationPolicyName:       t.Escalation.EscalationPolicyName,
		FavoriteServices:           t.Favorite.FavoriteServices,
		IntegrationKeyService:      t.IntegrationKey.ServiceID,
		ServicesWithLabel:          t.Label.ServicesWithLabel,
		ClearEscalationPolicyState: t.Escalation.ClearEscalationPolicyState,
	})
	s.IntegrationKey = integrationkey.NewDeltaStore(db, t.IntegrationKey, s.APIKeyring, opts.Registry, s.NotificationChannel, integrationkey.DeltaLookups{
		KeysPerService:           limits(limit.IntegrationKeysPerService),
		SignalsPerService:        limits(limit.PendingSignalsPerService),
		SignalsPerDestPerService: limits(limit.PendingSignalsPerDestPerService),
	})

	s.AlertMetrics = alertmetrics.NewDeltaStore(db, t.AlertMetrics)
	s.AlertLog = alertlog.NewDeltaStore(db, t.AlertLog, opts.Registry, alertlog.DeltaLookups{
		UserName:             t.userName,
		IntegrationKeyName:   t.integrationKeyName,
		HeartbeatMonitorName: t.heartbeatMonitorName,
		ChannelName:          t.channelName,
		ChannelDest:          t.NotificationChannel.Dest,
		ContactMethodDest:    t.ContactMethod.Dest,
		MessageDest:          t.messageDest,
		IntegrationKeyType:   t.integrationKeyType,
		HeartbeatInterval:    t.heartbeatInterval,
		PolicyServices:       t.Service.PolicyServices,
		AlertIDs:             t.Alert.AlertIDs,
	})
	s.Alert = alert.NewDeltaStore(db, t.Alert, s.AlertLog, t.AlertLog, t.AlertMetrics, alert.DeltaLookups{
		Service:           t.alertService,
		ServiceNames:      t.Service.EachName,
		MaxUnacked:        limits(limit.UnackedAlertsPerService),
		PolicyHasSteps:    t.Escalation.PolicyHasSteps,
		InsertEPState:     t.Escalation.InsertAlertState,
		ClearEPState:      t.Escalation.DeleteAlertState,
		RequestEscalation: t.Escalation.ForceEscalationByTime,
		EPState:           t.epState,
		MultiAck:          t.Escalation.AlertMultiAck,
		PendingMessages:   t.pendingMessages,
		UserName:          t.userName,
		ChannelName:       t.channelName,
		ChannelDest:       t.NotificationChannel.Dest,
		ContactMethodDest: t.ContactMethod.Dest,
	})
	s.Escalation = escalation.NewDeltaStore(db, t.Escalation, opts.Registry, escalation.DeltaLookups{
		StepLimit:      limits(limit.EPStepsPerPolicy),
		ActionLimit:    limits(limit.EPActionsPerStep),
		IsFavorite:     t.Favorite.IsEscalationPolicyFavorite,
		OnCallSteps:    t.OnCall.UserSteps,
		ChannelDest:    t.NotificationChannel.Dest,
		ServicePolicy:  t.servicePolicy,
		PolicyServices: t.Service.PolicyServices,
		AlertService:   t.alertServiceID,
		OpenAlerts:     t.openAlerts,
		TriggerAlert: func(tx *filestore.Tx, alertID int64) error {
			return s.Alert.DeltaSetStatus(tx, alertID, alert.StatusTriggered)
		},
		MapDestToID:  s.NotificationChannel.MapDestToID,
		LookupDestID: s.NotificationChannel.LookupDestID,
		LogEP:        s.AlertLog.LogEPTx,
	})
	s.Notification = notification.NewDeltaStore(db, t.Notification, notification.DeltaLookups{
		MethodOwner:             t.ContactMethod.Owner,
		MethodDest:              t.ContactMethod.Dest,
		MethodDisabled:          t.methodDisabled,
		MethodTypeValue:         t.methodTypeValue,
		SetMethodDisabled:       t.ContactMethod.SetDisabled,
		SetMethodLastTestVerify: t.ContactMethod.SetLastTestVerify,
		ChannelDest:             t.NotificationChannel.Dest,
		ChannelNameType:         t.channelNameType,
		UserName:                t.userName,
		ServiceName:             t.serviceName,
	})
	s.Notice = notice.NewDeltaStore(db, notice.DeltaLookups{
		PolicyServices:    t.policyServiceCount,
		PolicyMinutes:     t.Escalation.PolicyMinutes,
		UnackedAlerts:     t.Alert.UnackedCount,
		UnackedAlertLimit: limits(limit.UnackedAlertsPerService),
	})

	s.AuthBasic = basic.NewDeltaStore(db, t.AuthBasic, t.User.SetAuthSubject)
	s.AuthLink = authlink.NewDeltaStore(db, t.AuthLink, s.AuthLinkKeyring, t.User.InsertAuthSubject)
	s.APIKey = apikey.NewDeltaStore(db, t.APIKey, s.APIKeyring)
	s.CalSub = calsub.NewDeltaStore(db, t.CalSub, s.APIKeyring, s.OnCall, calsub.DeltaLookups{
		ScheduleName: t.scheduleName,
		UserName:     t.userName,
		MaxPerUser:   limits(limit.CalendarSubscriptionsPerUser),
	})
	return s, nil
}

func (s *Stores) initSecrets(ctx context.Context, delta *filestore.DeltaDB, opts Options) (err error) {
	s.Config, err = config.NewDeltaStore(ctx, opts.Config, s.DB, s.Tables.Config, delta.Changes("config"))
	if err != nil {
		return fmt.Errorf("init config store: %w", err)
	}
	for _, k := range []struct {
		ring *keyring.Keyring
		keyring.Config
	}{
		{&s.OAuthKeyring, keyring.Config{Name: "oauth-state", RotationDays: 1, MaxOldKeys: 1}},
		{&s.AuthLinkKeyring, keyring.Config{Name: "auth-link", RotationDays: 1, MaxOldKeys: 1}},
		{&s.SessionKeyring, keyring.Config{Name: "browser-sessions", RotationDays: 1, MaxOldKeys: 30}},
		{&s.APIKeyring, keyring.Config{Name: "api-keys", MaxOldKeys: 100}},
	} {
		k.Keys = opts.Config.Keys
		ring, err := keyring.NewDeltaDB(ctx, opts.Logger, s.DB, s.Tables.Keyring, &k.Config)
		if err != nil {
			return fmt.Errorf("init %s keyring: %w", k.Name, err)
		}
		*k.ring = ring
	}
	return nil
}

// ReEncryptAll encrypts the keyrings and the configuration with the first of
// keys. The others can decrypt them too. If any can't be, nothing is changed.
func (t *Tables) ReEncryptAll(ctx context.Context, db txn.DB, keys keyring.Keys) error {
	if err := permission.LimitCheckAny(ctx, permission.Admin); err != nil {
		return err
	}
	if len(keys) == 0 {
		return errors.New("no keys provided")
	}
	return deltatxn.Write(ctx, db, nil, func(s deltatxn.Scope) error {
		if err := t.Keyring.ReEncrypt(s.Tx, keys); err != nil {
			return err
		}
		return t.Config.ReEncrypt(s.Tx, keys)
	})
}

// NewAuthHandler returns the auth handler on the Delta backend.
func (s *Stores) NewAuthHandler(cfg auth.HandlerConfig) *auth.Handler {
	return auth.NewDeltaHandler(s.DB, s.Tables.Auth, cfg, auth.DeltaUsers{
		AuthSubjectUserID: s.Tables.User.AuthSubjectUserID,
		InsertAuthSubject: s.Tables.User.InsertAuthSubject,
		UpdateProfile:     s.Tables.User.UpdateProfile,
	})
}

// NewTwilioSMS returns the sender of SMS messages on the Delta backend.
func (s *Stores) NewTwilioSMS(cfg *twilio.Config) *twilio.SMS {
	return twilio.NewDeltaSMS(s.DB, s.Tables.Twilio, cfg, s.Tables.alertOpen, s.Tables.serviceName)
}

// HandlerConfig is the part of the auth handler's configuration that is stores.
func (s *Stores) HandlerConfig() auth.HandlerConfig {
	return auth.HandlerConfig{
		UserStore: s.User, SessionKeyring: s.SessionKeyring, IntKeyStore: s.IntegrationKey,
		CalSubStore: s.CalSub, APIKeyring: s.APIKeyring, APIKeyStore: s.APIKey,
	}
}

// Close stops what the stores run in the background, and writes what they have
// not written yet. The table stays open.
func (s *Stores) Close(ctx context.Context) error {
	var errs []error
	stop := func(name string, shutdown func(context.Context) error) {
		if err := shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("shut down %s: %w", name, err))
		}
	}
	if s.APIKey != nil {
		stop("API key store", s.APIKey.Shutdown)
	}
	if s.CalSub != nil {
		stop("calendar subscription store", s.CalSub.Shutdown)
	}
	for _, k := range []keyring.Keyring{s.SessionKeyring, s.OAuthKeyring, s.APIKeyring, s.AuthLinkKeyring} {
		if k != nil {
			stop("keyring", k.Shutdown)
		}
	}
	if s.Nonce != nil {
		stop("nonce store", s.Nonce.Shutdown)
	}
	if s.Config != nil {
		stop("config store", s.Config.Shutdown)
	}
	return errors.Join(errs...)
}

// What stores need of each other's tables.

func (t *Tables) userName(r filestore.Reader, id string) (string, bool) {
	row, ok := t.User.Users.Get(r, id)
	return row.Name, ok
}

func (t *Tables) userExists(r filestore.Reader, id string) bool {
	_, ok := t.User.Users.Get(r, id)
	return ok
}

func (t *Tables) usersByDest(r filestore.Reader, destType string, args map[string]string) []string {
	rows, err := t.ContactMethod.FindByDestArgs(r, destType, args)
	if err != nil {
		return nil // a destination that can't be read matches none
	}
	ids := make([]string, len(rows))
	for i, row := range rows {
		ids[i] = row.UserID
	}
	return ids
}

func (t *Tables) schedulesOfUser(r filestore.Reader, userID string) []string {
	targets := []assignment.Target{assignment.UserTarget(userID)}
	for _, id := range t.Rotation.UserRotationIDs(r, userID) {
		targets = append(targets, assignment.RotationTarget(id))
	}
	return t.Rule.ScheduleIDsByTarget(r, targets...)
}

func (t *Tables) scheduleName(r filestore.Reader, id string) (string, bool) {
	row, ok := t.Schedule.Schedules.Get(r, id)
	return row.Name, ok
}

func (t *Tables) onCallStep(r filestore.Reader, stepID string) (user.OnCallStep, bool) {
	st, ok := t.Escalation.Step(r, stepID)
	return user.OnCallStep{PolicyID: st.PolicyID, PolicyName: st.PolicyName, StepNumber: st.StepNumber}, ok
}

func (t *Tables) onCallServices(r filestore.Reader, policyID string) []user.OnCallService {
	var out []user.OnCallService
	for _, id := range t.Service.PolicyServices(r, policyID) {
		name, _ := t.serviceName(r, id)
		out = append(out, user.OnCallService{ID: id, Name: name})
	}
	return out
}

func (t *Tables) serviceName(r filestore.Reader, id string) (string, bool) {
	row, ok := t.Service.Services.Get(r, id)
	return row.Name, ok
}

func (t *Tables) servicePolicy(r filestore.Reader, id string) (string, bool) {
	row, ok := t.Service.Services.Get(r, id)
	return row.EscalationPolicyID, ok
}

func (t *Tables) policyServiceCount(r filestore.Reader, policyID string) int {
	return len(t.Service.PolicyServices(r, policyID))
}

func (t *Tables) serviceSteps(r filestore.Reader, serviceID string) []oncall.DeltaStep {
	policyID, ok := t.servicePolicy(r, serviceID)
	if !ok {
		return nil
	}
	var out []oncall.DeltaStep
	for _, st := range t.Escalation.PolicySteps(r, policyID) {
		out = append(out, oncall.DeltaStep{ID: st.ID, Number: int(st.StepNumber)})
	}
	return out
}

func (t *Tables) rotation(r filestore.Reader, id string) (oncall.DeltaRotation, bool) {
	rot, ok := t.Rotation.Rotations.Get(r, id)
	if !ok {
		return oncall.DeltaRotation{}, false
	}
	st, ok := t.Rotation.State.Get(r, id)
	if !ok {
		return oncall.DeltaRotation{}, false
	}
	out := oncall.DeltaRotation{
		Type: rotation.Type(rot.Type), Start: rot.Start, ShiftLength: int(rot.ShiftLength), TimeZone: rot.TimeZone,
		Position: int(st.Position), ShiftStart: st.ShiftStart,
	}
	for _, p := range t.Rotation.OrderedParticipants(r, id) {
		out.UserIDs = append(out.UserIDs, p.UserID)
	}
	return out, true
}

// overrides are all of a schedule's, whenever they are.
func (t *Tables) overrides(r filestore.Reader, scheduleID string) []override.UserOverride {
	return t.Override.FindOverlapping(r, scheduleID, time.Time{}, time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC))
}

func (t *Tables) alertService(r filestore.Reader, id string) (alert.DeltaService, bool) {
	row, ok := t.Service.Services.Get(r, id)
	return alert.DeltaService{
		Name: row.Name, EscalationPolicyID: row.EscalationPolicyID, InMaintenance: row.MaintenanceExpiresAt != nil,
	}, ok
}

func (t *Tables) alertServiceID(r filestore.Reader, alertID int64) (string, bool) {
	row, ok := t.Alert.Alerts.Get(r, filestore.IntKey(alertID))
	if !ok || row.ServiceID == nil {
		return "", false
	}
	return *row.ServiceID, true
}

func (t *Tables) alertOpen(r filestore.Reader, alertID int64) bool {
	status, ok := t.Alert.Status(r, alertID)
	return ok && status != alert.StatusClosed
}

func (t *Tables) openAlerts(r filestore.Reader, serviceID string) []int64 {
	return t.Alert.AlertIDs(r, serviceID, string(alert.StatusTriggered), string(alert.StatusActive))
}

func (t *Tables) epState(r filestore.Reader, alertID int64) (alert.State, bool) {
	st, ok := t.Escalation.AlertState(r, alertID)
	if !ok {
		return alert.State{}, false
	}
	out := alert.State{ID: int(alertID), StepNumber: int(st.StepNumber), RepeatCount: int(st.LoopCount)}
	if st.LastEscalation != nil {
		out.LastEscalation = *st.LastEscalation
	}
	return out, true
}

func (t *Tables) pendingMessages(r filestore.Reader, alertID int64, serviceID string, before time.Time) []alert.PendingMessage {
	str := func(p *string) string {
		if p == nil {
			return ""
		}
		return *p
	}
	var out []alert.PendingMessage
	for _, m := range t.Notification.PendingForAlert(r, alertID, serviceID, before) {
		out = append(out, alert.PendingMessage{UserID: str(m.UserID), ContactMethodID: str(m.ContactMethodID), ChannelID: str(m.ChannelID)})
	}
	return out
}

// messageDest is a message's destination, or an empty one if it has neither a
// contact method nor a channel.
func (t *Tables) messageDest(r filestore.Reader, id string) (gadb.DestV1, error) {
	m, ok := t.Notification.Messages.Get(r, id)
	switch {
	case !ok:
		return gadb.DestV1{}, sql.ErrNoRows
	case m.ContactMethodID != nil:
		return t.ContactMethod.Dest(r, *m.ContactMethodID)
	case m.ChannelID != nil:
		return t.NotificationChannel.Dest(r, *m.ChannelID)
	}
	return gadb.DestV1{}, nil
}

func (t *Tables) methodDisabled(r filestore.Reader, id string) (disabled, ok bool) {
	row, ok := t.ContactMethod.Methods.Get(r, id)
	return row.Disabled, ok
}

func (t *Tables) methodTypeValue(r filestore.Reader, id string) (typ, value string, ok bool) {
	row, ok := t.ContactMethod.Methods.Get(r, id)
	return row.Type, row.Value, ok
}

func (t *Tables) channelName(r filestore.Reader, id string) (string, bool) {
	row, ok := t.NotificationChannel.Channels.Get(r, id)
	return row.Name, ok
}

func (t *Tables) channelNameType(r filestore.Reader, id string) (name, typ string, ok bool) {
	row, ok := t.NotificationChannel.Channels.Get(r, id)
	return row.Name, row.Type, ok
}

func (t *Tables) integrationKeyName(r filestore.Reader, id string) (string, bool) {
	row, ok := t.IntegrationKey.Keys.Get(r, id)
	return row.Name, ok
}

func (t *Tables) integrationKeyType(r filestore.Reader, id string) (integrationkey.Type, bool) {
	row, ok := t.IntegrationKey.Keys.Get(r, id)
	return integrationkey.Type(row.Type), ok
}

func (t *Tables) heartbeatMonitorName(r filestore.Reader, id string) (string, bool) {
	row, ok := t.Heartbeat.Monitors.Get(r, id)
	return row.Name, ok
}

func (t *Tables) heartbeatInterval(r filestore.Reader, id string) (time.Duration, bool) {
	row, ok := t.Heartbeat.Monitors.Get(r, id)
	return time.Duration(row.HeartbeatInterval) * time.Microsecond, ok
}

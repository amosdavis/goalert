// Package deltastores assembles GoAlert on the Delta backend: one schema with
// every table and foreign key, and every store with what it needs of the others
// (docs/filestore-failure-modes.md, F51).
package deltastores

import (
	"github.com/target/goalert/alert"
	"github.com/target/goalert/alert/alertlog"
	"github.com/target/goalert/alert/alertmetrics"
	"github.com/target/goalert/apikey"
	"github.com/target/goalert/auth"
	"github.com/target/goalert/auth/authlink"
	"github.com/target/goalert/auth/basic"
	"github.com/target/goalert/auth/nonce"
	"github.com/target/goalert/calsub"
	"github.com/target/goalert/config"
	"github.com/target/goalert/engine/enginetables"
	"github.com/target/goalert/escalation"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/heartbeat"
	"github.com/target/goalert/integrationkey"
	"github.com/target/goalert/keyring"
	"github.com/target/goalert/label"
	"github.com/target/goalert/limit"
	"github.com/target/goalert/notification"
	"github.com/target/goalert/notification/slack"
	"github.com/target/goalert/notification/twilio"
	"github.com/target/goalert/notificationchannel"
	"github.com/target/goalert/oncall"
	"github.com/target/goalert/override"
	"github.com/target/goalert/schedule"
	"github.com/target/goalert/schedule/rotation"
	"github.com/target/goalert/schedule/rule"
	"github.com/target/goalert/service"
	"github.com/target/goalert/user"
	"github.com/target/goalert/user/contactmethod"
	"github.com/target/goalert/user/favorite"
	"github.com/target/goalert/user/notificationrule"
)

// Tables are all of GoAlert's tables on the Delta backend, by package.
type Tables struct {
	Alert               *alert.DeltaTables
	AlertLog            *alertlog.DeltaTables
	AlertMetrics        *alertmetrics.DeltaTables
	APIKey              *apikey.DeltaTables
	Auth                *auth.DeltaTables
	AuthBasic           *basic.DeltaTables
	AuthLink            *authlink.DeltaTables
	CalSub              *calsub.DeltaTables
	Config              *config.DeltaTables
	ContactMethod       *contactmethod.DeltaTables
	Engine              *enginetables.DeltaTables
	Escalation          *escalation.DeltaTables
	Favorite            *favorite.DeltaTables
	Heartbeat           *heartbeat.DeltaTables
	IntegrationKey      *integrationkey.DeltaTables
	Keyring             *keyring.DeltaTables
	Label               *label.DeltaTables
	Limit               *limit.DeltaTables
	Nonce               *nonce.DeltaTables
	Notification        *notification.DeltaTables
	NotificationChannel *notificationchannel.DeltaTables
	NotificationRule    *notificationrule.DeltaTables
	OnCall              *oncall.DeltaTables
	Override            *override.DeltaTables
	Rotation            *rotation.DeltaTables
	Rule                *rule.DeltaTables
	Schedule            *schedule.DeltaTables
	Service             *service.DeltaTables
	Slack               *slack.DeltaTables
	Twilio              *twilio.DeltaTables
	User                *user.DeltaTables
}

// NewSchema returns the schema of GoAlert's Delta table, with every table and
// foreign key in it. A process needs one, whether it creates the table or opens
// it. They can't be added to later without a migration (F19).
func NewSchema() (*filestore.Schema, *Tables) {
	s := filestore.NewSchema()
	t := &Tables{
		Alert:               alert.DefineDeltaTables(s),
		AlertLog:            alertlog.DefineDeltaTables(s),
		AlertMetrics:        alertmetrics.DefineDeltaTables(s),
		APIKey:              apikey.DefineDeltaTables(s),
		Auth:                auth.DefineDeltaTables(s),
		AuthBasic:           basic.DefineDeltaTables(s),
		AuthLink:            authlink.DefineDeltaTables(s),
		CalSub:              calsub.DefineDeltaTables(s),
		Config:              config.DefineDeltaTables(s),
		ContactMethod:       contactmethod.DefineDeltaTables(s),
		Engine:              enginetables.DefineDeltaTables(s),
		Escalation:          escalation.DefineDeltaTables(s),
		Heartbeat:           heartbeat.DefineDeltaTables(s),
		IntegrationKey:      integrationkey.DefineDeltaTables(s),
		Keyring:             keyring.DefineDeltaTables(s),
		Label:               label.DefineDeltaTables(s),
		Limit:               limit.DefineDeltaTables(s),
		Nonce:               nonce.DefineDeltaTables(s),
		Notification:        notification.DefineDeltaTables(s),
		NotificationChannel: notificationchannel.DefineDeltaTables(s),
		NotificationRule:    notificationrule.DefineDeltaTables(s),
		OnCall:              oncall.DefineDeltaTables(s),
		Override:            override.DefineDeltaTables(s),
		Rotation:            rotation.DefineDeltaTables(s),
		Rule:                rule.DefineDeltaTables(s),
		Schedule:            schedule.DefineDeltaTables(s),
		Service:             service.DefineDeltaTables(s),
		Slack:               slack.DefineDeltaTables(s),
		Twilio:              twilio.DefineDeltaTables(s),
		User:                user.DefineDeltaTables(s),
	}
	t.Favorite = favorite.DefineDeltaTables(s, t.User)
	t.declareForeignKeys()
	return s, t
}

func (t *Tables) declareForeignKeys() {
	var (
		alerts         = t.Alert.Alerts
		alertLogs      = t.AlertLog.Logs
		channels       = t.NotificationChannel.Channels
		contactMethods = t.ContactMethod.Methods
		messages       = t.Notification.Messages
		policies       = t.Escalation.Policies
		rotations      = t.Rotation.Rotations
		schedules      = t.Schedule.Schedules
		services       = t.Service.Services
		steps          = t.Escalation.Steps
		users          = t.User.Users
	)
	alert.DeclareDeltaForeignKeys(t.Alert, services, channels, contactMethods)
	alertmetrics.DeclareDeltaForeignKeys(t.AlertMetrics, alerts)
	apikey.DeclareDeltaForeignKeys(t.APIKey, users)
	auth.DeclareDeltaForeignKeys(t.Auth, users)
	basic.DeclareDeltaForeignKeys(t.AuthBasic, users)
	calsub.DeclareDeltaForeignKeys(t.CalSub, users, schedules)
	contactmethod.DeclareDeltaForeignKeys(t.ContactMethod, users)
	enginetables.DeclareDeltaForeignKeys(t.Engine, alerts, users)
	escalation.DeclareDeltaForeignKeys(t.Escalation, alerts, services, users, schedules, rotations, channels)
	favorite.DeclareDeltaForeignKeys(t.Favorite, services, schedules, rotations, policies)
	heartbeat.DeclareDeltaForeignKeys(t.Heartbeat, services)
	integrationkey.DeclareDeltaForeignKeys(t.IntegrationKey, services, channels, messages)
	label.DeclareDeltaForeignKeys(t.Label, services, schedules, rotations, policies)
	notification.DeclareDeltaForeignKeys(t.Notification, alerts, alertLogs, channels, contactMethods, policies, schedules, services, users)
	notification.DeclareDeltaCycleForeignKey(t.Notification, t.Engine.Cycles)
	notificationrule.DeclareDeltaForeignKeys(t.NotificationRule, contactMethods, users)
	oncall.DeclareDeltaForeignKeys(t.OnCall, schedules, steps, users)
	override.DeclareDeltaForeignKeys(t.Override, users, schedules)
	rotation.DeclareDeltaForeignKeys(t.Rotation, users)
	rule.DeclareDeltaForeignKeys(t.Rule, schedules, rotations, users)
	service.DeclareDeltaForeignKeys(t.Service, policies)
	slack.DeclareDeltaForeignKeys(t.Slack, users)
	twilio.DeclareDeltaForeignKeys(t.Twilio, alerts, services)
	user.DeclareDeltaForeignKeys(t.User, contactMethods, t.ContactMethod.Owner)
}

package app

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"net/http"

	"github.com/pkg/errors"
	"github.com/target/goalert/alert"
	"github.com/target/goalert/alert/alertlog"
	"github.com/target/goalert/apikey"
	"github.com/target/goalert/app/lifecycle"
	"github.com/target/goalert/auth"
	"github.com/target/goalert/auth/basic"
	"github.com/target/goalert/auth/nonce"
	"github.com/target/goalert/config"
	"github.com/target/goalert/deltastores"
	"github.com/target/goalert/engine"
	"github.com/target/goalert/escalation"
	"github.com/target/goalert/graphql2/graphqlapp"
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
	"github.com/target/goalert/schedule"
	"github.com/target/goalert/schedule/rotation"
	"github.com/target/goalert/schedule/rule"
	"github.com/target/goalert/service"
	"github.com/target/goalert/timezone"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/user"
	"github.com/target/goalert/user/contactmethod"
	"github.com/target/goalert/user/favorite"
	"github.com/target/goalert/user/notificationrule"
)

// App represents an instance of the GoAlert application.
type App struct {
	cfg Config

	Logger *slog.Logger

	mgr *lifecycle.Manager

	// storage is where the data is, and stores are the stores on it.
	storage *Storage
	stores  *deltastores.Stores

	l net.Listener

	httpClient *http.Client

	doneCh chan struct{}

	srv        *http.Server
	startupErr error

	notificationManager *notification.Manager
	Engine              *engine.Engine
	graphql2            *graphqlapp.App
	AuthHandler         *auth.Handler

	twilioSMS    *twilio.SMS
	twilioVoice  *twilio.Voice
	twilioConfig *twilio.Config

	ConfigStore *config.Store

	AlertStore    *alert.Store
	AlertLogStore *alertlog.Store

	AuthBasicStore        *basic.Store
	UserStore             *user.Store
	ContactMethodStore    *contactmethod.Store
	NotificationRuleStore *notificationrule.Store
	FavoriteStore         *favorite.Store

	ServiceStore        *service.Store
	EscalationStore     *escalation.Store
	IntegrationKeyStore *integrationkey.Store
	ScheduleRuleStore   *rule.Store
	NotificationStore   *notification.Store
	ScheduleStore       *schedule.Store
	RotationStore       *rotation.Store
	DestRegistry        *nfydest.Registry

	OverrideStore *override.Store
	LimitStore    *limit.Store

	OAuthKeyring   keyring.Keyring
	SessionKeyring keyring.Keyring
	APIKeyring     keyring.Keyring

	NonceStore    *nonce.Store
	LabelStore    *label.Store
	OnCallStore   *oncall.Store
	NCStore       *notificationchannel.Store
	TimeZoneStore *timezone.Store
	NoticeStore   *notice.Store
	APIKeyStore   *apikey.Store
}

// newApp returns an App that has no data yet, and binds the listening socket.
func newApp(c Config) (*App, error) {
	if c.Logger == nil {
		return nil, errors.New("Logger is required")
	}

	httpClient, err := newHTTPClient()
	if err != nil {
		return nil, errors.Wrap(err, "init http client")
	}

	l, err := net.Listen("tcp", c.ListenAddr)
	if err != nil {
		return nil, errors.Wrapf(err, "bind address %s", c.ListenAddr)
	}

	if c.TLSListenAddr != "" {
		l2, err := tls.Listen("tcp", c.TLSListenAddr, c.TLSConfig)
		if err != nil {
			return nil, errors.Wrapf(err, "listen %s", c.TLSListenAddr)
		}
		l = newMultiListener(l, l2)
	}

	return &App{
		l:          l,
		cfg:        c,
		doneCh:     make(chan struct{}),
		Logger:     c.Logger,
		httpClient: httpClient,
	}, nil
}

// start prepares an App that has its data to be run.
func (app *App) start() error {
	if app.cfg.StatusAddr != "" {
		err := listenStatus(app.cfg.StatusAddr, app.doneCh)
		if err != nil {
			return errors.Wrap(err, "start status listener")
		}
	}

	app.Logger.Debug("starting app")
	app.mgr = lifecycle.NewManager(app._Run, app._Shutdown)
	return app.mgr.SetStartupFunc(app.startup)
}

// WaitForStartup will wait until the startup sequence is completed or the context is expired.
func (a *App) WaitForStartup(ctx context.Context) error {
	return a.mgr.WaitForStartup(a.Context(ctx))
}

// Txn runs transactions on GoAlert's data.
func (app *App) Txn() txn.DB { return app.stores.DB }

// URL returns the non-TLS listener URL of the application.
func (a *App) URL() string {
	return "http://" + a.l.Addr().String()
}

package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/target/goalert/config"
	"github.com/target/goalert/deltastores"
	"github.com/target/goalert/engine"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/notification"
	"github.com/target/goalert/notification/nfydest"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/storagebackend"
	"github.com/target/goalert/tablelog"
)

// Storage is where GoAlert keeps its data: a Delta table that every pod has in
// memory.
type Storage struct {
	DB     *filestore.DeltaDB
	Tables *deltastores.Tables

	// PodName names this pod in the commits it writes, and to people.
	PodName string

	// CloseFunc, if set, closes what DB is on.
	CloseFunc func() error
}

// Close closes the storage.
func (st *Storage) Close() error {
	err := st.DB.Close()
	if st.CloseFunc != nil {
		err = errors.Wrap(firstErr(err, st.CloseFunc()), "close storage")
	}
	return err
}

func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// podName is a name for this pod, if it was not given one. It need not be the
// same again, and no two pods may have the same.
func podName(name string) string {
	if name == "" {
		name, _ = os.Hostname()
	}
	if name == "" {
		name = "goalert"
	}
	return name + "-" + uuid.NewString()[:8]
}

func (cfg Config) openBackend(ctx context.Context, name string) (*storagebackend.Backend, error) {
	b, err := storagebackend.New(ctx, storagebackend.Config{
		Objects: cfg.StorageObjects, Log: cfg.StorageLog,
		Warn: func(err error) { cfg.Logger.WarnContext(ctx, "Storage.", slog.Any("error", err)) },
	}, name)
	return b, errors.Wrap(err, "open storage")
}

func (cfg Config) storageOptions(name string) filestore.DeltaOptions {
	return filestore.DeltaOptions{
		WriterID: name,
		Logf:     func(format string, args ...any) { cfg.Logger.Warn(fmt.Sprintf(format, args...)) },
	}
}

// checkStorage reports settings of the storage that weaken the table, and
// returns an error for those that break it (F34).
func (cfg Config) checkStorage(ctx context.Context, b *storagebackend.Backend) error {
	problems, err := b.CheckSettings(ctx)
	if errors.Is(err, objstore.ErrDenied) {
		cfg.Logger.WarnContext(ctx, "The storage's settings could not be checked.", slog.Any("error", err))
	} else if err != nil {
		return errors.Wrap(err, "check the storage's settings")
	}
	var broken []string
	for _, p := range problems {
		if p.Breaks {
			broken = append(broken, p.String())
			continue
		}
		cfg.Logger.WarnContext(ctx, "A setting of the storage should be changed.", slog.String("setting", p.Setting), slog.String("problem", p.Problem))
	}
	if len(broken) > 0 {
		return fmt.Errorf("settings of the storage would break GoAlert: %s", strings.Join(broken, "; "))
	}
	return nil
}

// OpenStorage opens the storage that cfg names. It must have been created.
func OpenStorage(ctx context.Context, cfg Config) (*Storage, error) {
	name := podName(cfg.PodName)
	b, err := cfg.openBackend(ctx, name)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*Storage, error) { return nil, firstErr(err, b.Close()) }
	if err := cfg.checkStorage(ctx, b); err != nil {
		return fail(err)
	}
	db, tables, err := deltastores.Open(ctx, b, cfg.storageOptions(name))
	if errors.Is(err, tablelog.ErrNoTable) {
		err = errors.Wrap(err, "there is no GoAlert in this storage; create it with 'goalert create-storage', or check --storage-objects")
	}
	if err != nil {
		return fail(errors.Wrap(err, "open storage"))
	}
	return &Storage{DB: db, Tables: tables, PodName: name, CloseFunc: b.Close}, nil
}

// CreateStorage creates the storage that cfg names, for a new GoAlert. Nothing
// else does, so that a pod pointed at the wrong place does not start a second one
// (F27).
func CreateStorage(ctx context.Context, cfg Config) error {
	name := podName(cfg.PodName)
	b, err := cfg.openBackend(ctx, name)
	if err != nil {
		return err
	}
	defer b.Close()
	if err := cfg.checkStorage(ctx, b); err != nil {
		return err
	}
	return errors.Wrap(deltastores.Create(ctx, b, cfg.storageOptions(name)), "create storage")
}

// NewStorageApp is NewApp for a GoAlert whose data is in a Delta table. The app
// closes it when it shuts down.
func NewStorageApp(c Config, st *Storage) (*App, error) {
	app, err := newApp(c)
	if err != nil {
		return nil, err
	}
	app.storage = st
	return app, app.start()
}

func (app *App) initStorageStores(ctx context.Context) error {
	app.DestRegistry = nfydest.NewRegistry()
	var fallback url.URL
	fallback.Scheme = "http"
	fallback.Host = app.l.Addr().String()
	fallback.Path = app.cfg.HTTPPrefix
	s, err := deltastores.New(ctx, app.storage.DB, app.storage.Tables, deltastores.Options{
		Logger: app.cfg.LegacyLogger, Registry: app.DestRegistry,
		Config: config.StoreConfig{
			Keys:               app.cfg.EncryptionKeys,
			FallbackURL:        fallback.String(),
			ExplicitURL:        app.cfg.PublicURL,
			IngressEmailDomain: app.cfg.EmailIntegrationDomain,
		},
	})
	if err != nil {
		return err
	}
	app.stores = s
	app.ConfigStore = s.Config
	app.OAuthKeyring, app.SessionKeyring = s.OAuthKeyring, s.SessionKeyring
	app.APIKeyring, app.AuthLinkKeyring = s.APIKeyring, s.AuthLinkKeyring
	app.AlertStore, app.AlertLogStore, app.AlertMetricsStore = s.Alert, s.AlertLog, s.AlertMetrics
	app.AuthBasicStore, app.AuthLinkStore, app.APIKeyStore = s.AuthBasic, s.AuthLink, s.APIKey
	app.UserStore, app.ContactMethodStore, app.NotificationRuleStore = s.User, s.ContactMethod, s.NotificationRule
	app.FavoriteStore, app.LabelStore, app.LimitStore = s.Favorite, s.Label, s.Limit
	app.ServiceStore, app.EscalationStore = s.Service, s.Escalation
	app.IntegrationKeyStore, app.HeartbeatStore = s.IntegrationKey, s.Heartbeat
	app.ScheduleStore, app.ScheduleRuleStore, app.OverrideStore = s.Schedule, s.Rule, s.Override
	app.RotationStore, app.OnCallStore, app.CalSubStore = s.Rotation, s.OnCall, s.CalSub
	app.NotificationStore, app.NCStore, app.NoticeStore = s.Notification, s.NotificationChannel, s.Notice
	app.NonceStore, app.TimeZoneStore = s.Nonce, s.TimeZone
	return nil
}

func (app *App) initStorageEngine(ctx context.Context) (err error) {
	app.notificationManager = notification.NewManager(app.DestRegistry)
	app.Engine, err = engine.NewDeltaEngine(ctx, engine.DeltaConfig{
		DB: app.storage.DB, Tables: app.storage.Tables, Name: app.storage.PodName,
	}, app.engineConfig())
	return errors.Wrap(err, "init engine")
}

// Health returns why GoAlert does not work, if it does not: the storage refuses
// writes or can't be reached (F21, F31, F33), or the engine does not work (F55).
func (app *App) Health() error {
	if app.storage != nil {
		if err := app.storage.DB.Health(); err != nil {
			return err
		}
	}
	return app.Engine.Health()
}

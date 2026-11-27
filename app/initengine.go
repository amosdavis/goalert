package app

import (
	"context"

	"github.com/target/goalert/engine"
	"github.com/target/goalert/notification"

	"github.com/pkg/errors"
)

func (app *App) initEngine(ctx context.Context) (err error) {
	app.notificationManager = notification.NewManager(app.DestRegistry)
	app.Engine, err = engine.NewDeltaEngine(ctx, engine.DeltaConfig{
		DB: app.storage.DB, Tables: app.storage.Tables, Name: app.storage.PodName,
	}, app.engineConfig())
	return errors.Wrap(err, "init engine")
}

func (app *App) engineConfig() *engine.Config {
	return &engine.Config{
		AlertStore:          app.AlertStore,
		AlertLogStore:       app.AlertLogStore,
		ContactMethodStore:  app.ContactMethodStore,
		NotificationManager: app.notificationManager,
		UserStore:           app.UserStore,
		NotificationStore:   app.NotificationStore,
		NCStore:             app.NCStore,
		OnCallStore:         app.OnCallStore,
		ScheduleStore:       app.ScheduleStore,
		DestRegistry:        app.DestRegistry,

		ConfigSource: app.ConfigStore,

		CycleTime: app.cfg.EngineCycleTime,

		MaxMessages: 50,

		DisableCycle: app.cfg.APIOnly,
		LogCycles:    app.cfg.LogEngine,
		Logger:       app.Logger,
	}
}

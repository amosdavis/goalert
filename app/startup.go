package app

import (
	"context"
	"log/slog"

	"github.com/target/goalert/expflag"
	"github.com/target/goalert/notification/email"
	"github.com/target/goalert/notification/webhook"
	"github.com/target/goalert/permission"

	"github.com/pkg/errors"
)

func (app *App) initStartup(ctx context.Context, label string, fn func(context.Context) error) {
	if app.startupErr != nil {
		return
	}

	err := fn(ctx)
	if err != nil {
		app.startupErr = errors.Wrap(err, label)
	}
}

func (app *App) startup(ctx context.Context) error {
	for _, f := range app.cfg.ExpFlags {
		if expflag.Description(f) == "" {
			app.Logger.WarnContext(ctx, "Unknown experimental flag.", slog.String("flag", string(f)))
		} else {
			app.Logger.InfoContext(ctx, "Experimental flag enabled.",
				slog.String("flag", string(f)),
				slog.String("description", expflag.Description(f)),
			)
		}
	}

	app.initStartup(ctx, "Startup.Stores", app.initStorageStores)
	if app.startupErr != nil {
		return app.startupErr // ConfigStore will panic if not initialized
	}
	if app.cfg.InitialConfig != nil {
		app.initStartup(ctx, "Startup.InitialConfig", func(ctx context.Context) (err error) {
			permission.SudoContext(ctx, func(ctx context.Context) {
				err = app.ConfigStore.SetConfig(ctx, *app.cfg.InitialConfig)
			})
			return err
		})
	}
	ctx = app.ConfigStore.Config().Context(ctx)

	return app.startupAll(ctx)
}

// startupAll starts everything else.
func (app *App) startupAll(ctx context.Context) error {
	// init twilio before engine
	app.initStartup(
		ctx, "Startup.Twilio", app.initTwilio)

	app.initStartup(ctx, "Startup.Engine", app.initEngine)
	app.initStartup(ctx, "Startup.Auth", app.initAuth)
	app.initStartup(ctx, "Startup.GraphQL", app.initGraphQL)

	app.initStartup(ctx, "Startup.HTTPServer", app.initHTTP)

	if app.startupErr != nil {
		return app.startupErr
	}

	app.DestRegistry.RegisterProvider(ctx, app.twilioSMS)
	app.DestRegistry.RegisterProvider(ctx, app.twilioVoice)
	app.DestRegistry.RegisterProvider(ctx, email.NewSender(ctx))
	app.DestRegistry.RegisterProvider(ctx, app.ScheduleStore)
	app.DestRegistry.RegisterProvider(ctx, app.UserStore)
	app.DestRegistry.RegisterProvider(ctx, app.RotationStore)
	app.DestRegistry.RegisterProvider(ctx, webhook.NewSender(ctx, app.httpClient))
	if app.cfg.StubNotifiers {
		app.DestRegistry.StubNotifiers()
	}

	err := app.notificationManager.SetResultReceiver(ctx, app.Engine)
	if err != nil {
		return err
	}

	return app.mgr.SetPauseResumer(app.Engine)
}

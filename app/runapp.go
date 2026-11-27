package app

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/target/goalert/app/lifecycle"

	"github.com/pkg/errors"
)

var triggerSignals []os.Signal

// Run will start the application and start serving traffic.
func (app *App) Run(ctx context.Context) error {
	return app.mgr.Run(app.Context(ctx))
}

func (app *App) _Run(ctx context.Context) error {
	go func() {
		err := app.Engine.Run(ctx)
		// ErrShutdown / context.Canceled mean the engine was shut down before
		// this goroutine got scheduled (race between _Run and _Shutdown);
		// it's expected during fast restarts, not a failure.
		if err != nil && !errors.Is(err, lifecycle.ErrShutdown) && !errors.Is(err, context.Canceled) {
			app.Logger.ErrorContext(ctx, "Failed to run engine.", slog.Any("error", err))
		}
	}()

	app.Logger.InfoContext(ctx, "Listening.",
		slog.String("address", app.l.Addr().String()),
		slog.String("url", app.ConfigStore.Config().PublicURL()),
	)
	err := app.srv.Serve(app.l)
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return errors.Wrap(err, "serve HTTP")
	}

	<-ctx.Done()
	return nil
}

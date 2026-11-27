package app

import (
	"context"
	"os"
	"reflect"
	"time"

	"github.com/pkg/errors"
)

// Shutdown will cause the App to begin a graceful shutdown, using
// the provided context for any cleanup operations.
func (app *App) Shutdown(ctx context.Context) error {
	return app.mgr.Shutdown(app.Context(ctx))
}

func (app *App) _Shutdown(ctx context.Context) error {
	defer close(app.doneCh)
	var errs []error
	type shutdownable interface{ Shutdown(context.Context) error }

	shut := func(sh shutdownable, msg string) {
		if sh == nil {
			return
		}
		t := reflect.TypeOf(sh)
		if t.Kind() == reflect.Pointer && reflect.ValueOf(sh) == reflect.Zero(t) {
			// check for nil pointer
			return
		}
		err := sh.Shutdown(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			errs = append(errs, errors.Wrap(err, msg))
		}
	}

	// It's important to shutdown the HTTP server first
	// so things like message responses are handled before
	// shutting down things like the engine or notification manager
	// that would still need to process them.

	shut(app.srv, "HTTP server")
	shut(app.Engine, "engine")
	// Both write what they have not written yet.
	shut(app.AuthHandler, "auth handler")
	if app.stores != nil {
		shut(shutdownFunc(app.stores.Close), "stores")
	}
	shut(shutdownFunc(func(context.Context) error { return app.storage.Close() }), "storage")
	return shutdownErr(errs)
}

type shutdownFunc func(context.Context) error

func (fn shutdownFunc) Shutdown(ctx context.Context) error { return fn(ctx) }

func shutdownErr(errs []error) error {
	if len(errs) == 1 {
		return errs[0]
	}
	if len(errs) > 1 {
		return errors.Errorf("multiple shutdown errors: %+v", errs)
	}

	return nil
}

var shutdownSignals = []os.Signal{os.Interrupt}

const shutdownTimeout = time.Minute * 2

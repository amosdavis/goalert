package harness

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/target/goalert/app"
)

// storage is where a test's GoAlert keeps its data. Everything the harness
// does to that data, other than through GoAlert itself, goes through this
// interface (docs/filestore-plan.md, phase 0).
type storage interface {
	// Exec runs fixture SQL: INSERT ... VALUES, UPDATE ... SET ... WHERE
	// column = value, and DELETE.
	Exec(ctx context.Context, sql string) error

	// Now reads the storage's clock. SetClockSpeed sets how fast it runs
	// (0 freezes it, 1 is real time) and AdvanceClock moves it forward.
	Now(ctx context.Context) (time.Time, error)
	SetClockSpeed(ctx context.Context, speed float64) error
	AdvanceClock(ctx context.Context, d time.Duration) error

	// NewApp starts GoAlert on the storage; it can be called again to
	// restart.
	NewApp(ctx context.Context, cfg app.Config) (*app.App, error)

	// Dump writes all stored data to w, for debugging a failed test.
	Dump(ctx context.Context, w io.Writer) error

	// Close releases the storage and deletes its data.
	Close(ctx context.Context) error

	// String describes where the data is, for log messages.
	String() string
}

// newStorage creates the storage that SMOKE_STORAGE names.
func newStorage(ctx context.Context, t *testing.T) (storage, error) {
	t.Helper()
	switch name := os.Getenv("SMOKE_STORAGE"); name {
	case "", "delta":
		return newDeltaStorage(ctx, t)
	default:
		return nil, fmt.Errorf("unknown SMOKE_STORAGE %q", name)
	}
}

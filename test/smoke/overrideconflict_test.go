package smoke

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/target/goalert/assignment"
	"github.com/target/goalert/override"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/test/smoke/harness"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/sqltxn"
)

// TestOverrideConflict checks that two transactions creating the same
// override can't both commit, even when the second checks for conflicts
// while the first is still open.
func TestOverrideConflict(t *testing.T) {
	t.Parallel()

	sql := `
	insert into users (id, name, email)
	values
		({{uuid "u1"}}, 'bob', 'bob@example.com');

	insert into schedules (id, name, time_zone)
	values
		({{uuid "sid"}}, 'schedule', 'UTC');
	`

	h := harness.NewHarness(t, sql, "sched-module-v3")
	defer h.Close()

	db := sqltxn.DB{DB: h.App().DB()}

	ctx := permission.SystemContext(context.Background(), "Smoketest")
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	start := time.Now().Add(-time.Hour)
	end := start.Add(8 * time.Hour)
	create := func(tx txn.Tx) error {
		_, err := h.App().OverrideStore.CreateUserOverrideTx(ctx, tx, &override.UserOverride{
			AddUserID: h.UUID("u1"),
			Target:    assignment.ScheduleTarget(h.UUID("sid")),
			Start:     start,
			End:       end,
		})
		return err
	}

	// The first transaction creates the override and stays open for a
	// while. A transaction can run more than once on some backends, so it
	// signals only once.
	var once sync.Once
	created := make(chan struct{})
	first := make(chan error, 1)
	go func() {
		first <- db.Run(ctx, nil, func(tx txn.Tx) error {
			if err := create(tx); err != nil {
				return err
			}
			once.Do(func() { close(created) })
			time.Sleep(3 * time.Second)
			return nil
		})
	}()
	select {
	case <-created:
	case err := <-first:
		t.Fatalf("first transaction ended before creating the override: %v", err)
	}

	// The second checks for conflicts while the first is open.
	second := db.Run(ctx, nil, create)
	require.NoError(t, <-first)
	require.Error(t, second, "the second override conflicts with the first")
}

package enginetables

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/target/goalert/config"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/util/log"
)

// Task is work that the pod with the lease does every so often, beside the
// engine's cycles. Each of its transactions is a Runner's Do.
type Task struct {
	Name  string
	Every time.Duration
	Run   func(ctx context.Context) error
}

// Maintenance runs tasks when they are due. When each was last done is in the
// table, so it does not matter which pod does it, or how often they start.
type Maintenance struct {
	t      *DeltaTables
	db     *filestore.DeltaDB
	runner *Runner
	tasks  []Task

	failed map[string]time.Time // when a task that failed is tried again
}

// retryTask is how long after it failed a task is tried again, unless it is due
// sooner.
const retryTask = 10 * time.Minute

// NewMaintenance returns the tasks of a pod.
func (t *DeltaTables) NewMaintenance(db *filestore.DeltaDB, runner *Runner, tasks ...Task) *Maintenance {
	return &Maintenance{t: t, db: db, runner: runner, tasks: tasks, failed: make(map[string]time.Time)}
}

const maintenanceState = "maintenance"

type taskState struct {
	LastRun map[string]time.Time
	Config  string
}

// RunDue runs the tasks that are due, one after the other, until ctx is done. A
// task that fails is logged, and tried again later. A change of maintenance
// configuration makes work due immediately, not only after its next interval.
func (m *Maintenance) RunDue(ctx context.Context) error {
	signature := fmt.Sprintf("%#v", config.FromContext(ctx).Maintenance)
	for _, task := range m.tasks {
		var state taskState
		if err := m.t.LoadState(m.db.View(), maintenanceState, &state); err != nil {
			return err
		}
		now := m.db.Now()
		changed := state.Config != signature
		if !changed && (now.Sub(state.LastRun[task.Name]) < task.Every || now.Before(m.failed[task.Name])) {
			continue
		}
		err := task.Run(ctx)
		if err == nil {
			err = m.runner.Do(ctx, Step{Name: "maintenance", Run: func(_ context.Context, tx *filestore.Tx) error {
				var state taskState
				if err := m.t.LoadState(tx, maintenanceState, &state); err != nil {
					return err
				}
				if state.LastRun == nil {
					state.LastRun = make(map[string]time.Time)
				}
				state.LastRun[task.Name], state.Config = now, signature
				return m.t.SaveState(tx, maintenanceState, state)
			}})
		}
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case errors.Is(err, ErrNotHolder):
			return err
		case err != nil:
			log.Log(ctx, fmt.Errorf("maintenance: %s: %w", task.Name, err))
			m.failed[task.Name] = now.Add(min(task.Every, retryTask))
		default:
			delete(m.failed, task.Name)
		}
	}
	return nil
}

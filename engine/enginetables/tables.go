// Package enginetables has the engine's own tables on the Delta backend: the
// lease of the pod that runs it, what its modules remember, and the
// notification cycles.
package enginetables

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/util/jsonutil"
)

// DeltaTables are the engine's tables on the Delta backend.
type DeltaTables struct {
	// Cycles is notification_policy_cycles, by id. Other packages declare their
	// foreign keys to it. Add rows with InsertCycle.
	Cycles *filestore.Table[CycleRow]
	// CyclesByAlert finds cycles by filestore.IntKey of their alert_id.
	CyclesByAlert *filestore.Index[CycleRow]

	lease  *filestore.Table[leaseRow]
	states *filestore.Table[stateRow]
}

// CycleRow is a notification_policy_cycles row on the Delta backend: a user is
// notified of an alert by their notification rules. The columns checked and
// repeat_count, which nothing used any more, are gone.
type CycleRow struct {
	ID        string     `parquet:"id"`
	AlertID   int64      `parquet:"alert_id"`
	UserID    string     `parquet:"user_id"`
	StartedAt time.Time  `parquet:"started_at,timestamp(microsecond:utc)"`
	LastTick  *time.Time `parquet:"last_tick,optional,timestamp(microsecond:utc)"`
	MultiAck  bool       `parquet:"multi_ack"`
}

// leaseRow says which pod runs the engine.
type leaseRow struct {
	Name       string    `parquet:"name"`
	Holder     string    `parquet:"holder"`
	HolderName string    `parquet:"holder_name"` // for people
	TakenAt    time.Time `parquet:"taken_at,timestamp(microsecond:utc)"`
	ExpiresAt  time.Time `parquet:"expires_at,timestamp(microsecond:utc)"`
}

// stateRow is what a module remembers (engine_processing_versions.state).
type stateRow struct {
	Module string `parquet:"module"`
	State  []byte `parquet:"state"`
}

// DefineDeltaTables adds the engine's tables to the Delta backend's schema:
// notification_policy_cycles, keyed by id (notification_policy_cycles_pkey),
// and engine_lease and engine_state, which are new.
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	t := &DeltaTables{
		Cycles: filestore.NewTable(s, "notification_policy_cycles", func(c CycleRow) string { return c.ID }),
		lease:  filestore.NewTable(s, "engine_lease", func(l leaseRow) string { return l.Name }).Buckets(1),
		states: filestore.NewTable(s, "engine_state", func(r stateRow) string { return r.Module }).Buckets(1),
	}
	t.CyclesByAlert = t.Cycles.Index("idx_np_cycle_alert_id", func(c CycleRow) []string {
		return []string{filestore.IntKey(c.AlertID)}
	})
	return t
}

// DeclareDeltaForeignKeys declares this package's foreign keys to other
// packages' tables. The keys of alerts are filestore.IntKey of their ids.
func DeclareDeltaForeignKeys[A, U any](t *DeltaTables, alerts *filestore.Table[A], users *filestore.Table[U]) {
	filestore.References(t.Cycles, "notification_policy_cycles_alert_id_fkey", alerts,
		func(c CycleRow) (string, bool) { return filestore.IntKey(c.AlertID), true }, filestore.Cascade, nil)
	filestore.References(t.Cycles, "notification_policy_cycles_user_id_fkey", users,
		func(c CycleRow) (string, bool) { return c.UserID, true }, filestore.Cascade, nil)
}

// InsertCycle starts to notify a user of an alert, now.
func (t *DeltaTables) InsertCycle(tx *filestore.Tx, alertID int64, userID string, multiAck bool) error {
	return t.Cycles.Insert(tx, CycleRow{
		ID: uuid.NewString(), AlertID: alertID, UserID: userID, MultiAck: multiAck, StartedAt: tx.Now(),
	}, "notification_policy_cycles_pkey")
}

// LoadState reads what a module remembers into v. For a module that remembers
// nothing yet, v stays as it is.
func (t *DeltaTables) LoadState(r filestore.Reader, module string, v any) error {
	row, ok := t.states.Get(r, module)
	if !ok {
		return nil
	}
	if err := json.Unmarshal(row.State, v); err != nil {
		return fmt.Errorf("enginetables: state of %s: %w", module, err)
	}
	return nil
}

// SaveState replaces what a module remembers by v. Fields that v does not have,
// which another version wrote, stay.
func (t *DeltaTables) SaveState(tx *filestore.Tx, module string, v any) error {
	row, ok := t.states.Get(tx, module)
	if !ok {
		row.State = []byte("{}")
	}
	data, err := jsonutil.Apply(row.State, v)
	if err != nil {
		return err
	}
	return t.states.Put(tx, stateRow{Module: module, State: data})
}

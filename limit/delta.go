package limit

import (
	"context"
	"fmt"
	"math"

	"github.com/target/goalert/filestore"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
)

// limitRow is a config_limits row on the Delta backend.
type limitRow struct {
	ID  string `parquet:"id"`
	Max int32  `parquet:"max"`
}

// DeltaTables are the limit store's tables on the Delta backend.
type DeltaTables struct {
	Limits *filestore.Table[limitRow]
}

// DefineDeltaTables adds the limit store's tables to the Delta backend's
// schema: config_limits, keyed by id (config_limits_pkey).
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	return &DeltaTables{
		Limits: filestore.NewTable(s, "config_limits", func(r limitRow) string { return r.ID }),
	}
}

// deltaDefaults are the limits the Postgres migrations set on a new install.
var deltaDefaults = []limitRow{
	{string(NotificationRulesPerUser), 15},
	{string(ContactMethodsPerUser), 10},
	{string(EPStepsPerPolicy), 10},
	{string(EPActionsPerStep), 20},
	{string(ParticipantsPerRotation), 50},
	{string(RulesPerSchedule), 30},
	{string(IntegrationKeysPerService), 30},
	{string(UnackedAlertsPerService), 200},
	{string(TargetsPerSchedule), 10},
	{string(HeartbeatMonitorsPerService), 30},
	{string(UserOverridesPerSchedule), 35},
	{string(CalendarSubscriptionsPerUser), 15},
	{string(PendingSignalsPerService), 50},
	{string(PendingSignalsPerDestPerService), 5},
}

// InsertDefaults sets the limits a new install starts with, leaving any that
// are set alone (ON CONFLICT DO NOTHING). Like the migrations it stands for,
// it runs once, when the Delta table is created: without it a new install
// has no limits (F8), and UpdateLimitsTx has no rows to update. Running it on
// every start would bring back limits that ResetAll removed.
func (t *DeltaTables) InsertDefaults(tx *filestore.Tx) error {
	for _, row := range deltaDefaults {
		if _, ok := t.Limits.Get(tx, row.ID); ok {
			continue
		}
		if err := t.Limits.Insert(tx, row, "config_limits_pkey"); err != nil {
			return err
		}
	}
	return nil
}

// Max returns, in r, the max value of a limit, or -1 if there is none. A
// limit without a row is no limit, as in the Postgres triggers.
func (t *DeltaTables) Max(r filestore.Reader, id ID) int {
	row, ok := t.Limits.Get(r, string(id))
	if !ok {
		return -1
	}
	return int(row.Max)
}

// MaxFunc returns Max for one limit, as the stores that enforce it take it.
func (t *DeltaTables) MaxFunc(id ID) func(filestore.Reader) int {
	return func(r filestore.Reader) int { return t.Max(r, id) }
}

// NewDeltaStore returns a limit store on the Delta backend.
func NewDeltaStore(db txn.DB, tables *DeltaTables) *Store {
	return &Store{delta: &deltaLimits{db: db, DeltaTables: tables}}
}

type deltaLimits struct {
	db txn.DB
	*DeltaTables
}

// newLimitRow refuses what the Postgres columns would: ids outside
// enum_limit_type, and values outside an integer.
func newLimitRow(id string, max int) (limitRow, error) {
	if err := ID(id).Valid(); err != nil {
		return limitRow{}, err
	}
	if max < math.MinInt32 || max > math.MaxInt32 {
		return limitRow{}, fmt.Errorf("limit %s: %d is out of range", id, max)
	}
	return limitRow{ID: id, Max: int32(max)}, nil
}

// update sets a limit that has a row. Like the Postgres UPDATE, it does
// nothing for one that has none.
func (d *deltaLimits) update(ctx context.Context, tx txn.Tx, id string, max int) error {
	row, err := newLimitRow(id, max)
	if err != nil {
		return err
	}
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		if _, ok := d.Limits.Get(s.Reader, id); !ok {
			return nil
		}
		return d.Limits.Put(s.Tx, row)
	})
}

// set sets a limit, whether it has a row or not.
func (d *deltaLimits) set(ctx context.Context, id ID, max int) error {
	row, err := newLimitRow(string(id), max)
	if err != nil {
		return err
	}
	return deltatxn.Write(ctx, d.db, nil, func(s deltatxn.Scope) error {
		return d.Limits.Put(s.Tx, row)
	})
}

// resetAll removes every limit.
func (d *deltaLimits) resetAll(ctx context.Context) error {
	return deltatxn.Write(ctx, d.db, nil, func(s deltatxn.Scope) error {
		var ids []string
		d.Limits.Ascend(s.Reader, func(r limitRow) bool {
			ids = append(ids, r.ID)
			return true
		})
		for _, id := range ids {
			if _, err := d.Limits.Delete(s.Tx, id); err != nil {
				return err
			}
		}
		return nil
	})
}

func (d *deltaLimits) max(ctx context.Context, id ID) (int, error) {
	var max int
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		max = d.Max(s.Reader, id)
		return nil
	})
	return max, err
}

func (d *deltaLimits) all(ctx context.Context) (Limits, error) {
	l := make(Limits, 8)
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		clear(l)
		d.Limits.Ascend(s.Reader, func(r limitRow) bool {
			l[ID(r.ID)] = int(r.Max)
			return true
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return l, nil
}

package heartbeat

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
)

// monitorRow is a heartbeat_monitors row on the Delta backend. The interval
// is in microseconds.
type monitorRow struct {
	ID                string     `parquet:"id"`
	Name              string     `parquet:"name"`
	ServiceID         string     `parquet:"service_id"`
	HeartbeatInterval int64      `parquet:"heartbeat_interval"`
	LastState         string     `parquet:"last_state"`
	LastHeartbeat     *time.Time `parquet:"last_heartbeat,optional,timestamp(microsecond:utc)"`
	AdditionalDetails *string    `parquet:"additional_details,optional"`
	Muted             *string    `parquet:"muted,optional"`
}

func (r monitorRow) monitor() Monitor {
	m := Monitor{
		ID:        r.ID,
		Name:      r.Name,
		ServiceID: r.ServiceID,
		Timeout:   time.Duration(r.HeartbeatInterval) * time.Microsecond,
		lastState: State(r.LastState),
	}
	if r.AdditionalDetails != nil {
		m.AdditionalDetails = *r.AdditionalDetails
	}
	if r.Muted != nil {
		m.Muted = *r.Muted
	}
	if r.LastHeartbeat != nil {
		m.lastHeartbeat = *r.LastHeartbeat
	}
	return m
}

// set copies to the row what UpdateTx can change.
func (r *monitorRow) set(m *Monitor) {
	r.Name = m.Name
	r.HeartbeatInterval = int64(m.Timeout / time.Microsecond)
	r.AdditionalDetails = optText(m.AdditionalDetails)
	r.Muted = optText(m.Muted)
}

func optText(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// DeltaTables are the heartbeat store's tables on the Delta backend. Other
// packages declare their foreign keys to Monitors. The engine changes
// LastState, and finds the monitors it may have to change with ByState.
type DeltaTables struct {
	Monitors *filestore.Table[monitorRow]
	// ByState has the monitors that ever had a heartbeat, by last_state.
	ByState   *filestore.Index[monitorRow]
	byService *filestore.Index[monitorRow]
}

// DefineDeltaTables adds the heartbeat store's tables to the Delta backend's
// schema: heartbeat_monitors, keyed by id (heartbeat_monitors_pkey).
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	t := &DeltaTables{Monitors: filestore.NewTable(s, "heartbeat_monitors", func(r monitorRow) string { return r.ID })}
	t.ByState = t.Monitors.Index("heartbeat_monitors_last_state", func(r monitorRow) []string {
		if r.LastHeartbeat == nil {
			return nil
		}
		return []string{r.LastState}
	})
	t.byService = t.Monitors.Index("idx_heartbeat_monitor_service", func(r monitorRow) []string { return []string{r.ServiceID} })
	t.Monitors.Unique("heartbeat_monitor_name_service_id", func(r monitorRow) (string, bool) {
		return r.ServiceID + "/" + strings.ToLower(r.Name), true
	})
	return t
}

// DeclareDeltaForeignKeys declares this package's foreign keys to other
// packages' tables.
func DeclareDeltaForeignKeys[Svc any](t *DeltaTables, services *filestore.Table[Svc]) {
	filestore.References(t.Monitors, "heartbeat_monitors_service_id_fkey", services,
		func(r monitorRow) (string, bool) { return r.ServiceID, true }, filestore.Cascade, nil)
}

// NewDeltaStore returns a heartbeat store on the Delta backend.
// maxPerService is the heartbeat_monitors_per_service limit, or -1 for none.
func NewDeltaStore(db txn.DB, tables *DeltaTables, maxPerService func(r filestore.Reader) int) *Store {
	if maxPerService == nil {
		panic("heartbeat: NewDeltaStore needs the limit on monitors per service") // or it would not be enforced (F8)
	}
	return &Store{delta: &deltaMonitors{db: db, DeltaTables: tables, maxPerService: maxPerService}}
}

type deltaMonitors struct {
	db txn.DB
	*DeltaTables
	maxPerService func(r filestore.Reader) int
}

func (d *deltaMonitors) insert(ctx context.Context, tx txn.Tx, id uuid.UUID, m *Monitor) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row := monitorRow{ID: id.String(), ServiceID: canonical(m.ServiceID), LastState: string(StateInactive)}
		row.set(m)
		if err := d.Monitors.Insert(s.Tx, row, "heartbeat_monitors_pkey"); err != nil {
			return err
		}
		return d.checkLimit(s.Tx, row.ServiceID)
	})
}

// checkLimit is trg_enforce_heartbeat_monitor_limit, which counts the new
// row too.
func (d *deltaMonitors) checkLimit(r filestore.Reader, serviceID string) error {
	limit := d.maxPerService(r)
	if limit == -1 || d.byService.Count(r, serviceID) <= limit {
		return nil
	}
	return &filestore.CheckError{
		Table: "heartbeat_monitors", Constraint: "heartbeat_monitors_per_service_limit",
		Message: "limit exceeded", HintText: "max=" + strconv.Itoa(limit),
	}
}

// record sets a monitor's last heartbeat to now. Each heartbeat is one write
// of one row, and heartbeats that arrive while another commit is being made
// share the next one (F15, F37).
func (d *deltaMonitors) record(ctx context.Context, id uuid.UUID) error {
	return deltatxn.Write(ctx, d.db, nil, func(s deltatxn.Scope) error {
		row, ok := d.Monitors.Get(s.Reader, id.String())
		if !ok {
			return nil // an UPDATE of no rows
		}
		row.LastHeartbeat = &s.Now
		return d.Monitors.Put(s.Tx, row)
	})
}

func (d *deltaMonitors) delete(ctx context.Context, tx txn.Tx, ids []uuid.UUID) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		for _, id := range ids {
			if _, err := d.Monitors.Delete(s.Tx, id.String()); err != nil {
				return err
			}
		}
		return nil
	})
}

func (d *deltaMonitors) update(ctx context.Context, tx txn.Tx, id uuid.UUID, m *Monitor) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.Monitors.Get(s.Reader, id.String())
		if !ok {
			return nil // an UPDATE of no rows
		}
		row.set(m)
		return d.Monitors.Put(s.Tx, row)
	})
}

// findOne returns sql.ErrNoRows for a missing monitor. In a transaction, no
// other change can come between this and the transaction's own, as with the
// Postgres query's FOR UPDATE (F4).
func (d *deltaMonitors) findOne(ctx context.Context, tx txn.Tx, id uuid.UUID) (*Monitor, error) {
	var m Monitor
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.Monitors.Get(s.Reader, id.String())
		if !ok {
			return sql.ErrNoRows
		}
		m = row.monitor()
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func (d *deltaMonitors) findMany(ctx context.Context, ids []uuid.UUID) ([]Monitor, error) {
	var res []Monitor
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		res = make([]Monitor, 0, len(ids)) // not nil, as the Postgres store returns
		seen := make(map[uuid.UUID]bool, len(ids))
		for _, id := range ids {
			if seen[id] {
				continue // like `id = ANY(...)`
			}
			seen[id] = true
			if row, ok := d.Monitors.Get(s.Reader, id.String()); ok {
				res = append(res, row.monitor())
			}
		}
		return nil
	})
	return res, err
}

func (d *deltaMonitors) findAllByService(ctx context.Context, serviceID uuid.UUID) ([]Monitor, error) {
	var res []Monitor
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		res = []Monitor{} // not nil, as the Postgres store returns
		d.byService.Ascend(s.Reader, serviceID.String(), func(row monitorRow) bool {
			res = append(res, row.monitor())
			return true
		})
		return nil
	})
	return res, err
}

// canonical returns a validated UUID as the tables store it, in the form
// Postgres gives UUIDs.
func canonical(id string) string { return uuid.MustParse(id).String() }

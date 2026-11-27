package engine

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/target/goalert/alert"
	"github.com/target/goalert/alert/alertlog"
	"github.com/target/goalert/app/lifecycle"
	"github.com/target/goalert/deltastores"
	"github.com/target/goalert/engine/cleanupmanager"
	"github.com/target/goalert/engine/enginetables"
	"github.com/target/goalert/engine/escalationmanager"
	"github.com/target/goalert/engine/message"
	"github.com/target/goalert/engine/npcyclemanager"
	"github.com/target/goalert/engine/rotationmanager"
	"github.com/target/goalert/engine/schedulemanager"
	"github.com/target/goalert/engine/statusmgr"
	"github.com/target/goalert/engine/verifymanager"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/notification"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util/log"
)

// DeltaConfig is what an engine on the Delta backend needs besides its Config.
type DeltaConfig struct {
	DB     *filestore.DeltaDB
	Tables *deltastores.Tables

	// Name is the pod's, for people.
	Name string

	// LeaseTime is how long after the pod that runs the engine stops without
	// saying so another takes over. The default is 30 seconds.
	LeaseTime time.Duration
}

const defaultLeaseTime = 30 * time.Second

// deltaEngine is the part of an engine that depends on the Delta backend.
type deltaEngine struct {
	db      *filestore.DeltaDB
	t       *deltastores.Tables
	runner  *enginetables.Runner
	modules []deltaModule
	msg     *message.DeltaDB
	maint   *enginetables.Maintenance

	leaseTime time.Duration

	// The maintenance that runs beside the cycles, if it does.
	maintMx   sync.Mutex
	maintStop context.CancelFunc
	maintDone chan struct{}

	healthMx sync.Mutex
	// failing is since when no cycle succeeded, or zero.
	failing time.Time
	lastErr error
}

// deltaModule is a module on the Delta backend. UpdateAll runs in a transaction,
// which can run more than once, so it does no I/O (F3, F4).
type deltaModule interface {
	Name() string
	UpdateAll(ctx context.Context, tx *filestore.Tx) error
}

// NewDeltaEngine will create a new Engine on the Delta backend, whose stores are
// the Delta backend's too. Of several pods one runs it at a time (F28).
func NewDeltaEngine(ctx context.Context, dc DeltaConfig, c *Config) (*Engine, error) {
	if dc.LeaseTime <= 0 {
		dc.LeaseTime = defaultLeaseTime
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	d := &deltaEngine{
		db: dc.DB, t: dc.Tables, leaseTime: dc.LeaseTime,
		runner: enginetables.NewRunner(dc.DB, dc.Tables.Engine.NewLease(dc.Name, dc.LeaseTime)),
		msg:    message.NewDeltaDB(dc.DB, dc.Tables, c.AlertLogStore),
	}
	// Each sees what those before it did in the same cycle: an alert that opens
	// escalates, and the messages about it are claimed, in one commit.
	d.modules = []deltaModule{
		rotationmanager.NewDeltaDB(d.t),
		schedulemanager.NewDeltaDB(d.t),
		escalationmanager.NewDeltaDB(d.t, c.AlertLogStore),
		npcyclemanager.NewDeltaDB(d.t, c.AlertLogStore),
		statusmgr.NewDeltaDB(d.t, c.DestRegistry),
		verifymanager.NewDeltaDB(d.t),
	}
	cleanup := cleanupmanager.NewDeltaDB(d.db, d.t, c.AlertStore, d.runner, c.Logger.With(slog.String("module", "cleanup")))
	d.maint = d.t.Engine.NewMaintenance(d.db, d.runner, cleanup.Tasks()...)

	p := newEngine(c)
	p.delta = d
	p.mgr = lifecycle.NewManager(p._run, p._shutdown)
	return p, p.mgr.SetPauseResumer(lifecycle.PauseResumerFunc(p._pause, p._resume))
}

// deltaCycle runs an engine cycle, if this pod has the lease or can take it.
func (p *Engine) deltaCycle(ctx context.Context) {
	d := p.delta
	var batch message.Batch
	var steps []enginetables.Step
	for _, m := range d.modules {
		steps = append(steps, timed(m.Name(), m.UpdateAll))
	}
	steps = append(steps, timed(d.msg.Name(), func(ctx context.Context, tx *filestore.Tx) error {
		return d.msg.UpdateAll(ctx, tx, &batch)
	}))
	errs, err := d.runner.Run(ctx, steps...)
	if errors.Is(err, enginetables.ErrNotHolder) {
		metricLease.Set(0)
		d.setHealth(ctx, nil)
		return
	}
	if err != nil {
		if errors.Is(err, objstore.ErrUnknown) {
			d.msg.Unclear(&batch)
		}
		metricCycleErrors.Inc()
		log.Log(ctx, errors.Wrap(err, "engine cycle"))
		d.setHealth(ctx, err)
		return
	}
	metricLease.Set(1)
	d.setHealth(ctx, nil)
	for i, err := range errs {
		if err != nil {
			metricCycleErrors.Inc()
			log.Log(ctx, errors.Wrap(err, steps[i].Name))
		}
	}
	if err := errs[len(errs)-1]; err != nil {
		if errors.Is(err, objstore.ErrUnknown) {
			d.msg.Unclear(&batch)
		}
		return // what it claimed, if it did, is left to expire
	}
	d.msg.Done(&batch)
	metricRetries.Add(float64(batch.Retried))
	start := time.Now()
	d.msg.Send(ctx, &batch, p.deltaSend, p.cfg.NotificationManager.MessageStatus)
	metricModuleDuration.WithLabelValues("Engine.Message.Send").Observe(time.Since(start).Seconds())
}

// timed is a module's step, whose time is measured.
func timed(name string, fn func(ctx context.Context, tx *filestore.Tx) error) enginetables.Step {
	return enginetables.Step{Name: name, Run: func(ctx context.Context, tx *filestore.Tx) error {
		start := time.Now()
		defer func() { metricModuleDuration.WithLabelValues(name).Observe(time.Since(start).Seconds()) }()
		return fn(ctx, tx)
	}}
}

// deltaSend sends a message. Its log entry, and the subscription to its alert's
// status, are in the commit that records what came of it.
func (p *Engine) deltaSend(ctx context.Context, msg *message.Message) (*notification.SendResult, message.FollowUp, error) {
	ctx = p.messageContext(ctx, msg)
	m, first, res, err := p.buildMessage(ctx, msg)
	if err != nil || res != nil {
		return res, nil, err
	}
	res, err = p.cfg.NotificationManager.SendMessage(ctx, m)
	if err != nil {
		return nil, nil, err
	}
	return res, func(_ context.Context, tx *filestore.Tx) error {
		return errors.Wrap(p.afterSend(ctx, tx, msg, first && res.State.IsOK()), "append alert log")
	}, nil
}

func (p *Engine) afterSend(ctx context.Context, tx *filestore.Tx, msg *message.Message, subscribe bool) error {
	meta := alertlog.NotificationMetaData{MessageID: msg.ID}
	var err error
	switch msg.Type {
	case notification.MessageTypeAlert:
		err = p.cfg.AlertLogStore.LogTx(ctx, deltatxn.Of(tx), msg.AlertID, alertlog.TypeNotificationSent, meta)
	case notification.MessageTypeAlertBundle:
		err = p.cfg.AlertLogStore.LogServiceTx(ctx, deltatxn.Of(tx), msg.ServiceID, alertlog.TypeNotificationSent, meta)
	}
	if err != nil || !subscribe {
		return err
	}
	id := func(v uuid.NullUUID) *string {
		if !v.Valid {
			return nil
		}
		s := v.UUID.String()
		return &s
	}
	return p.delta.t.Alert.Subscribe(tx, alert.DeltaSubscription{
		AlertID: int64(msg.AlertID), LastAlertStatus: string(alert.StatusTriggered),
		ChannelID: id(msg.DestID.NCID), ContactMethodID: id(msg.DestID.CMID),
	})
}

// maintain starts the maintenance that is due, unless it runs already. It stops
// with the engine, and never holds up a cycle.
func (d *deltaEngine) maintain(ctx context.Context) {
	if !d.runner.Holds() {
		return
	}
	d.maintMx.Lock()
	defer d.maintMx.Unlock()
	if d.maintDone != nil {
		select {
		case <-d.maintDone:
		default:
			return
		}
	}
	ctx, d.maintStop = context.WithCancel(ctx)
	done := make(chan struct{})
	d.maintDone = done
	go func() {
		defer close(done)
		defer recoverPanic(ctx, "maintenance")
		err := d.maint.RunDue(ctx)
		if err != nil && ctx.Err() == nil && !errors.Is(err, enginetables.ErrNotHolder) {
			log.Log(ctx, errors.Wrap(err, "maintenance"))
		}
	}()
}

// stop ends what runs beside the cycles, and gives the lease back, so that
// another pod goes on at once (F55).
func (d *deltaEngine) stop(ctx context.Context) error {
	d.maintMx.Lock()
	done := d.maintDone
	if d.maintStop != nil {
		d.maintStop()
	}
	d.maintMx.Unlock()
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	metricLease.Set(0)
	return errors.Wrap(d.runner.Release(ctx), "release the engine's lease")
}

// setHealth records how a cycle went.
func (d *deltaEngine) setHealth(ctx context.Context, err error) {
	st := d.runner.Status()
	if err == nil && !st.Mine && st.Free && !st.ExpiresAt.IsZero() && d.db.Now().Sub(st.ExpiresAt) > d.leaseTime {
		err = fmt.Errorf("the pod that has the engine's lease (%s) stopped renewing it, and this one can't take it", st.HolderName)
	}
	d.healthMx.Lock()
	defer d.healthMx.Unlock()
	d.lastErr = err
	switch {
	case err == nil:
		d.failing = time.Time{}
	case d.failing.IsZero():
		d.failing = time.Now()
	}
}

// healthGrace is how long cycles may fail before the engine is not healthy.
const healthGrace = time.Minute

// Health returns an error if the engine has not worked for a while, here or, as
// far as this pod can tell, anywhere (F55).
func (p *Engine) Health() error {
	if p == nil || p.cfg.DisableCycle {
		return nil
	}
	d := p.delta
	d.healthMx.Lock()
	defer d.healthMx.Unlock()
	if d.failing.IsZero() || time.Since(d.failing) < healthGrace {
		return nil
	}
	return fmt.Errorf("the engine has not worked since %s: %w", d.failing.Format(time.RFC3339), d.lastErr)
}

// current runs fn on what this pod knows, and if it does not find what it looks
// for, on the newest state there is: another pod may just have made it (F29).
func (d *deltaEngine) current(ctx context.Context, fn func(r filestore.Reader) bool) (bool, error) {
	if fn(d.db.View()) {
		return true, nil
	}
	found := false
	_, err := d.db.Update(ctx, func(tx *filestore.Tx) error {
		found = fn(tx)
		return nil
	})
	return found, err
}

func (d *deltaEngine) findCallback(ctx context.Context, id string) (*callback, error) {
	var m notification.MessageRow
	ok, err := d.current(ctx, func(r filestore.Reader) (ok bool) {
		m, ok = d.t.Notification.Messages.Get(r, id)
		return ok
	})
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrNoRows
	}
	c := &callback{ID: m.ID, CreatedAt: m.CreatedAt}
	if m.AlertID != nil {
		c.AlertID = int(*m.AlertID)
	}
	if m.ServiceID != nil {
		c.ServiceID = *m.ServiceID
	}
	if m.ContactMethodID != nil {
		if c.ContactMethodID, err = uuid.Parse(*m.ContactMethodID); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (d *deltaEngine) isKnownDest(ctx context.Context, dest gadb.DestV1) (bool, error) {
	return d.current(ctx, func(r filestore.Reader) bool {
		if cm, ok := d.t.ContactMethod.FindByDest(r, dest); ok && !cm.Disabled {
			return true
		}
		_, ok := d.t.NotificationChannel.FindByDest(r, dest)
		return ok
	})
}

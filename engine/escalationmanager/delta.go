package escalationmanager

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/pkg/errors"
	"github.com/target/goalert/alert"
	"github.com/target/goalert/alert/alertlog"
	"github.com/target/goalert/deltastores"
	"github.com/target/goalert/engine/enginetables"
	"github.com/target/goalert/escalation"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/notification"
	"github.com/target/goalert/oncall"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util/log"
)

// DeltaDB escalates alerts on the Delta backend.
type DeltaDB struct {
	t   *deltastores.Tables
	log *alertlog.Store
}

// NewDeltaDB returns an escalation manager on the Delta backend. log is the
// Delta backend's too.
func NewDeltaDB(t *deltastores.Tables, log *alertlog.Store) *DeltaDB {
	return &DeltaDB{t: t, log: log}
}

// Name returns the name of the module.
func (db *DeltaDB) Name() string { return "Engine.EscalationManager" }

// maxEscalations is how many alerts are escalated in a cycle, so that a cycle
// stays short (F15). The rest follow in the next.
const maxEscalations = 1000

// UpdateAll records who is on call for escalation policy steps, and escalates
// the alerts that are due. One that can't be escalated is logged, and stays as
// it is.
func (db *DeltaDB) UpdateAll(ctx context.Context, tx *filestore.Tx) error {
	if err := permission.LimitCheckAny(ctx, permission.System); err != nil {
		return err
	}
	if err := db.updateOnCall(tx); err != nil {
		return errors.Wrap(err, "update ep step on-call")
	}
	if err := db.t.Service.EndMaintenance(tx); err != nil {
		return errors.Wrap(err, "end maintenance")
	}

	var due []escalationDue
	var noSteps []int64
	db.t.Escalation.States.Ascend(tx, func(st escalation.StateRow) bool {
		d, ok, over := db.due(tx, st)
		if ok {
			due = append(due, d)
		}
		if over {
			noSteps = append(noSteps, st.AlertID)
		}
		return true
	})
	for _, id := range noSteps {
		if err := db.t.Escalation.DeleteAlertState(tx, id); err != nil {
			return errors.Wrap(err, "end policies with no steps")
		}
	}
	// New alerts first, then those whose step was deleted, then the one that is
	// longest due.
	slices.SortStableFunc(due, func(a, b escalationDue) int {
		return cmp.Or(cmp.Compare(a.kind, b.kind), a.since.Compare(b.since), cmp.Compare(a.state.AlertID, b.state.AlertID))
	})
	if len(due) > maxEscalations {
		due = due[:maxEscalations]
	}
	err := enginetables.Each(tx, due, func(d escalationDue) error {
		return errors.Wrapf(db.escalate(ctx, tx, d), "escalate alert %d", d.state.AlertID)
	})
	if err != nil {
		log.Log(ctx, err)
	}
	return nil
}

// updateOnCall records who goes on and off call for escalation policy steps:
// the users of their actions, and those on call for the schedules and rotations
// of their actions.
func (db *DeltaDB) updateOnCall(tx *filestore.Tx) error {
	type shift struct{ stepID, userID string }
	onCall := make(map[shift]bool) // and whether they already are
	scheds := make(map[string][]string)
	db.t.Escalation.Actions.Ascend(tx, func(a escalation.ActionRow) bool {
		switch {
		case a.UserID != nil:
			onCall[shift{a.StepID, *a.UserID}] = false
		case a.RotationID != nil:
			if userID, ok := db.t.Rotation.ActiveUserID(tx, *a.RotationID); ok {
				onCall[shift{a.StepID, userID}] = false
			}
		case a.ScheduleID != nil:
			users, ok := scheds[*a.ScheduleID]
			if !ok {
				users = db.t.OnCall.ScheduleUsers(tx, *a.ScheduleID)
				scheds[*a.ScheduleID] = users
			}
			for _, userID := range users {
				onCall[shift{a.StepID, userID}] = false
			}
		}
		return true
	})
	var ended []shift
	db.t.OnCall.EachOpenStep(tx, func(row oncall.StepOnCallRow) bool {
		s := shift{row.StepID, row.UserID}
		if _, ok := onCall[s]; ok {
			onCall[s] = true
		} else {
			ended = append(ended, s)
		}
		return true
	})
	for _, s := range ended {
		if err := db.t.OnCall.EndStepOnCall(tx, s.stepID, s.userID); err != nil {
			return err
		}
	}
	var started []shift
	for s, already := range onCall {
		if !already {
			started = append(started, s)
		}
	}
	slices.SortFunc(started, func(a, b shift) int {
		return cmp.Or(cmp.Compare(a.stepID, b.stepID), cmp.Compare(a.userID, b.userID))
	})
	for _, s := range started {
		if _, err := db.t.OnCall.StartStepOnCall(tx, s.stepID, s.userID); err != nil {
			return err
		}
	}
	return nil
}

// Why an alert is due.
const (
	dueNew     = iota // it was never escalated
	dueDeleted        // the step it was on was deleted
	dueNext           // it is time for the next step, or somebody asked for it
)

type escalationDue struct {
	kind  int
	since time.Time
	state escalation.StateRow
	step  escalation.StepRow
	meta  alertlog.EscalationMetaData
}

// due reports whether an alert has to escalate, and how, or if it never will,
// because its policy has no steps.
func (db *DeltaDB) due(tx *filestore.Tx, st escalation.StateRow) (d escalationDue, ok, over bool) {
	policy, hasPolicy := db.t.Escalation.Policies.Get(tx, st.PolicyID)
	if !hasPolicy {
		return d, false, false
	}
	if policy.StepCount == 0 && st.StepID == nil {
		return d, false, true
	}
	if status, found := db.t.Alert.Status(tx, st.AlertID); !found || status != alert.StatusTriggered && !st.ForceEscalation {
		return d, false, false
	}
	if svc, found := db.t.Service.Services.Get(tx, st.ServiceID); !found || svc.MaintenanceExpiresAt != nil {
		return d, false, false
	}

	d.state = st
	var number int32
	switch {
	case st.LastEscalation == nil:
		d.kind = dueNew
	case st.StepID == nil:
		d.kind, d.since = dueDeleted, *st.LastEscalation
		number = st.StepNumber
		if number >= policy.StepCount {
			number, d.meta.Repeat = 0, true
		}
		d.meta.NewStepIndex = int(number)
	default:
		if !st.ForceEscalation && (st.NextEscalation == nil || !st.NextEscalation.Before(tx.Now())) {
			return d, false, false
		}
		old, found := db.t.Escalation.Steps.Get(tx, *st.StepID)
		if !found {
			return d, false, false
		}
		d.kind = dueNext
		if st.NextEscalation != nil {
			d.since = *st.NextEscalation
		}
		number = old.StepNumber + 1
		d.meta.OldDelayMinutes, d.meta.Forced = int(old.Delay), st.ForceEscalation
		if number >= policy.StepCount {
			d.meta.Repeat = true
			if !st.ForceEscalation && policy.Repeat != -1 && st.LoopCount >= policy.Repeat {
				return d, false, false // it has repeated often enough
			}
			number = 0
		}
		d.meta.NewStepIndex = int(number)
	}
	d.step, ok = db.t.Escalation.PolicyStep(tx, st.PolicyID, int(number))
	return d, ok, false
}

// escalate puts an alert on its next step, and starts to notify those on call
// for it.
func (db *DeltaDB) escalate(ctx context.Context, tx *filestore.Tx, d escalationDue) error {
	notified := false
	for _, userID := range db.t.OnCall.StepUsers(tx, d.step.ID) {
		if err := db.t.Engine.InsertCycle(tx, d.state.AlertID, userID, d.step.MultiAck); err != nil {
			return fmt.Errorf("start notification cycle: %w", err)
		}
		notified = true
	}
	var channels []string
	db.t.Escalation.ActionsByStep.Ascend(tx, d.step.ID, func(a escalation.ActionRow) bool {
		if a.ChannelID != nil {
			channels = append(channels, *a.ChannelID)
		}
		return true
	})
	for _, channelID := range channels {
		_, err := db.t.Notification.InsertMessage(tx, notification.MessageRow{
			MessageType: string(gadb.EnumOutgoingMessagesTypeAlertNotification),
			AlertID:     &d.state.AlertID, ServiceID: &d.state.ServiceID, EscalationPolicyID: &d.step.PolicyID,
			ChannelID: &channelID,
		})
		if err != nil {
			return fmt.Errorf("notify channel: %w", err)
		}
		notified = true
	}

	now := tx.Now()
	next := now.Add(time.Duration(d.step.Delay) * time.Minute)
	_, err := db.t.Escalation.UpdateState(tx, d.state.AlertID, func(st *escalation.StateRow) {
		st.LastEscalation, st.NextEscalation = &now, &next
		st.StepID, st.StepNumber = &d.step.ID, d.step.StepNumber
		st.ForceEscalation = false
		if d.kind == dueNext && d.meta.Repeat {
			st.LoopCount++
		}
	})
	if err != nil {
		return fmt.Errorf("update state: %w", err)
	}
	d.meta.NoOneOnCall = !notified
	err = db.log.LogTx(ctx, deltatxn.Of(tx), int(d.state.AlertID), alertlog.TypeEscalated, d.meta)
	return errors.Wrap(err, "log escalation")
}

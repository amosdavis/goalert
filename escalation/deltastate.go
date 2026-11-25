package escalation

import (
	"time"

	"github.com/target/goalert/filestore"
)

// StateRow is an escalation_policy_state row on the Delta backend: how far an
// open alert has escalated.
type StateRow struct {
	AlertID         int64      `parquet:"alert_id"`
	ID              int64      `parquet:"id"`
	ServiceID       string     `parquet:"service_id"`
	PolicyID        string     `parquet:"escalation_policy_id"`
	StepID          *string    `parquet:"escalation_policy_step_id,optional"`
	StepNumber      int32      `parquet:"escalation_policy_step_number"`
	LoopCount       int32      `parquet:"loop_count"`
	ForceEscalation bool       `parquet:"force_escalation"`
	LastEscalation  *time.Time `parquet:"last_escalation,optional,timestamp(microsecond:utc)"`
	NextEscalation  *time.Time `parquet:"next_escalation,optional,timestamp(microsecond:utc)"`
}

// AlertState returns, in r, an alert's state and whether it has one.
func (t *DeltaTables) AlertState(r filestore.Reader, alertID int64) (StateRow, bool) {
	return t.States.Get(r, filestore.IntKey(alertID))
}

// AlertMultiAck reports whether, in r, the step an alert is on has multi-ack
// enabled.
func (t *DeltaTables) AlertMultiAck(r filestore.Reader, alertID int64) bool {
	st, ok := t.AlertState(r, alertID)
	if !ok || st.StepID == nil {
		return false
	}
	step, ok := t.Steps.Get(r, *st.StepID)
	return ok && step.MultiAck
}

// InsertState adds an alert's state. Without an id it gets one, and without a
// service the alert's (trg_10_set_ep_state_svc_id_on_insert).
func (t *DeltaTables) InsertState(tx *filestore.Tx, st StateRow) error {
	if st.ServiceID == "" {
		if t.lookups.AlertService == nil {
			return missing("AlertService")
		}
		st.ServiceID, _ = t.lookups.AlertService(tx, st.AlertID)
	}
	if err := t.checkServicePolicy(tx, st); err != nil {
		return err
	}
	return t.insertState(tx, st)
}

func (t *DeltaTables) insertState(tx *filestore.Tx, st StateRow) error {
	if st.ID == 0 {
		var err error
		st.ID, err = tx.NextID("escalation_policy_state_id_seq")
		if err != nil {
			return err
		}
	}
	return t.States.Insert(tx, st, "escalation_policy_state_pkey")
}

// checkServicePolicy is svc_ep_fkey: a state's policy is its service's.
func (t *DeltaTables) checkServicePolicy(tx *filestore.Tx, st StateRow) error {
	if t.lookups.ServicePolicy == nil {
		return missing("ServicePolicy")
	}
	policyID, ok := t.lookups.ServicePolicy(tx, st.ServiceID)
	if !ok || policyID != st.PolicyID {
		return &filestore.ForeignKeyError{
			Table: "escalation_policy_state", Key: filestore.IntKey(st.AlertID),
			ForeignKey: "svc_ep_fkey", Missing: true,
		}
	}
	return nil
}

// UpdateState changes an alert's state, and reports whether it has one. change
// replaces what it changes: it must not write through the row's pointers.
//
// An alert that is made to escalate (force_escalation) becomes triggered, in
// the same transaction (trg_30_trig_alert_on_force_escalation). There is
// nothing left to do for trg_20_lock_svc_on_force_escalation, which locks the
// service: a transaction is made on top of every earlier one (F4).
func (t *DeltaTables) UpdateState(tx *filestore.Tx, alertID int64, change func(st *StateRow)) (bool, error) {
	old, ok := t.AlertState(tx, alertID)
	if !ok {
		return false, nil
	}
	st := old
	change(&st)
	if st.ServiceID != old.ServiceID || st.PolicyID != old.PolicyID {
		if err := t.checkServicePolicy(tx, st); err != nil {
			return false, err
		}
	}
	if err := t.States.Put(tx, st); err != nil {
		return false, err
	}
	if !st.ForceEscalation || old.ForceEscalation {
		return true, nil
	}
	if t.lookups.TriggerAlert == nil {
		return false, missing("TriggerAlert")
	}
	return true, t.lookups.TriggerAlert(tx, alertID)
}

// InsertAlertState is trg_10_insert_ep_state_on_alert_insert, for an alert
// that was just added and is not closed: it starts to escalate, unless its
// service's policy has no steps.
func (t *DeltaTables) InsertAlertState(tx *filestore.Tx, alertID int64, serviceID string) error {
	if t.lookups.ServicePolicy == nil {
		return missing("ServicePolicy")
	}
	policyID, ok := t.lookups.ServicePolicy(tx, serviceID)
	if !ok {
		return nil // an INSERT of no rows
	}
	if p, ok := t.Policies.Get(tx, policyID); !ok || p.StepCount <= 0 {
		return nil
	}
	return t.insertState(tx, StateRow{AlertID: alertID, ServiceID: serviceID, PolicyID: policyID})
}

// DeleteAlertState is trg_10_clear_ep_state_on_alert_close, for an alert that
// was just closed.
func (t *DeltaTables) DeleteAlertState(tx *filestore.Tx, alertID int64) error {
	_, err := t.States.Delete(tx, filestore.IntKey(alertID))
	return err
}

// ClearNextEscalation is trg_20_clear_next_esc_on_alert_ack, for an alert
// whose status was just changed from active to another.
func (t *DeltaTables) ClearNextEscalation(tx *filestore.Tx, alertID int64) error {
	_, err := t.UpdateState(tx, alertID, func(st *StateRow) { st.NextEscalation = nil })
	return err
}

// ForceEscalation makes an alert escalate, and reports whether it does now and
// did not before. It leaves it to the caller to check that the alert's service
// is not in maintenance.
func (t *DeltaTables) ForceEscalation(tx *filestore.Tx, alertID int64) (bool, error) {
	if st, ok := t.AlertState(tx, alertID); !ok || st.ForceEscalation {
		return false, nil
	}
	return t.UpdateState(tx, alertID, func(st *StateRow) { st.ForceEscalation = true })
}

// ForceEscalationByTime makes an alert escalate if it has not since a time,
// and reports whether it has not.
func (t *DeltaTables) ForceEscalationByTime(tx *filestore.Tx, alertID int64, since time.Time) (bool, error) {
	st, ok := t.AlertState(tx, alertID)
	if !ok || st.LastEscalation != nil && st.LastEscalation.After(since) {
		return false, nil
	}
	return t.UpdateState(tx, alertID, func(st *StateRow) { st.ForceEscalation = true })
}

// ClearEscalationPolicyState is trg_10_clear_ep_state_on_svc_ep_change, and
// the ON UPDATE CASCADE of svc_ep_fkey, for a service whose policy was just
// changed: its alerts start over with the new policy.
//
// That includes the open alerts that have no state, because the old policy had
// no steps. Without one, they would never escalate (F2).
func (t *DeltaTables) ClearEscalationPolicyState(tx *filestore.Tx, serviceID, newPolicyID string) error {
	var states []StateRow
	t.StatesByService.Ascend(tx, serviceID, func(st StateRow) bool {
		states = append(states, st)
		return true
	})
	for _, st := range states {
		if err := t.States.Put(tx, StateRow{
			AlertID: st.AlertID, ID: st.ID, ServiceID: st.ServiceID, PolicyID: newPolicyID,
		}); err != nil {
			return err
		}
	}
	if p, ok := t.Policies.Get(tx, newPolicyID); !ok || p.StepCount <= 0 {
		return nil
	}
	return t.startAlerts(tx, serviceID, newPolicyID)
}

// startAlerts adds the states of the service's open alerts that have none.
func (t *DeltaTables) startAlerts(tx *filestore.Tx, serviceID, policyID string) error {
	if t.lookups.OpenAlerts == nil {
		return missing("OpenAlerts")
	}
	for _, id := range t.lookups.OpenAlerts(tx, serviceID) {
		if _, ok := t.AlertState(tx, id); ok {
			continue
		}
		if err := t.insertState(tx, StateRow{AlertID: id, ServiceID: serviceID, PolicyID: policyID}); err != nil {
			return err
		}
	}
	return nil
}

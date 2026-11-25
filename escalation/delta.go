package escalation

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/txn/deltatxn"
)

// PolicyRow is an escalation_policies row on the Delta backend.
type PolicyRow struct {
	ID          string `parquet:"id"`
	Name        string `parquet:"name"`
	Description string `parquet:"description"`
	Repeat      int32  `parquet:"repeat"`
	StepCount   int32  `parquet:"step_count"` // kept by InsertStep and DeleteStep
}

// StepRow is an escalation_policy_steps row on the Delta backend.
type StepRow struct {
	ID         string `parquet:"id"`
	PolicyID   string `parquet:"escalation_policy_id"`
	Delay      int32  `parquet:"delay"` // minutes
	StepNumber int32  `parquet:"step_number"`
	MultiAck   bool   `parquet:"multi_ack"`
}

// ActionRow is an escalation_policy_actions row on the Delta backend; exactly
// one target is set.
type ActionRow struct {
	ID         string  `parquet:"id"`
	StepID     string  `parquet:"escalation_policy_step_id"`
	UserID     *string `parquet:"user_id,optional"`
	ScheduleID *string `parquet:"schedule_id,optional"`
	RotationID *string `parquet:"rotation_id,optional"`
	ChannelID  *string `parquet:"channel_id,optional"`
}

// DeltaTables are the escalation policy tables on the Delta backend. Other
// packages declare their foreign keys to Policies and Steps, and read all
// four.
//
// Steps, actions and states are written only through the functions here: they
// do what the Postgres triggers and checks did, and a write to the tables
// themselves does not. Deleting a row of Policies or States needs no more
// than the table's Delete.
type DeltaTables struct {
	Policies *filestore.Table[PolicyRow] // keyed by id
	Steps    *filestore.Table[StepRow]   // keyed by id
	Actions  *filestore.Table[ActionRow] // keyed by id
	States   *filestore.Table[StateRow]  // keyed by the alert's filestore.IntKey

	StepsByPolicy   *filestore.Index[StepRow]   // in no particular order, see PolicySteps
	ActionsByStep   *filestore.Index[ActionRow] // in the order they were added
	StatesByService *filestore.Index[StateRow]
	StatesByPolicy  *filestore.Index[StateRow]

	stepByNumber      *filestore.Unique[StepRow] // by stepNumberKey
	actionsBySchedule *filestore.Index[ActionRow]

	// by actionKey
	actionByUser, actionBySchedule, actionByRotation, actionByChannel *filestore.Unique[ActionRow]

	lookups DeltaLookups
}

func stepNumberKey(policyID string, number int32) string {
	return policyID + "/" + strconv.Itoa(int(number))
}

func actionKey(stepID, targetID string) string { return stepID + "/" + targetID }

// DefineDeltaTables adds escalation_policies, escalation_policy_steps,
// escalation_policy_actions and escalation_policy_state to the Delta backend's
// schema, with the foreign keys between them.
//
// Postgres checks escalation_policy_steps_escalation_policy_id_step_number_key
// and trg_ep_step_number_no_gaps when it commits (DEFERRABLE INITIALLY
// DEFERRED). Here both hold after every change, see setStepNumber.
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	t := &DeltaTables{
		Policies: filestore.NewTable(s, "escalation_policies", func(p PolicyRow) string { return p.ID }),
		Steps:    filestore.NewTable(s, "escalation_policy_steps", func(st StepRow) string { return st.ID }),
		Actions:  filestore.NewTable(s, "escalation_policy_actions", func(a ActionRow) string { return a.ID }),
		States:   filestore.NewTable(s, "escalation_policy_state", func(st StateRow) string { return filestore.IntKey(st.AlertID) }),
	}

	// The exact name first: Postgres checks its older index first.
	t.Policies.Unique("escalation_policies_name_key", func(p PolicyRow) (string, bool) { return p.Name, true })
	t.Policies.Unique("escalation_policies_name", func(p PolicyRow) (string, bool) { return strings.ToLower(p.Name), true })

	t.StepsByPolicy = t.Steps.Index("idx_ep_step_policies", func(st StepRow) []string { return []string{st.PolicyID} })
	t.stepByNumber = t.Steps.Unique("escalation_policy_steps_escalation_policy_id_step_number_key", func(st StepRow) (string, bool) {
		return stepNumberKey(st.PolicyID, st.StepNumber), true
	})

	t.ActionsByStep = t.Actions.Index("idx_ep_action_steps", func(a ActionRow) []string { return []string{a.StepID} })
	t.actionsBySchedule = t.Actions.Index("schedule_id", func(a ActionRow) []string {
		if a.ScheduleID == nil {
			return nil
		}
		return []string{*a.ScheduleID}
	})
	target := func(name string, of func(ActionRow) *string) *filestore.Unique[ActionRow] {
		return t.Actions.Unique(name, func(a ActionRow) (string, bool) {
			id := of(a)
			if id == nil {
				return "", false
			}
			return actionKey(a.StepID, *id), true
		})
	}
	t.actionByUser = target("epa_no_duplicate_users", func(a ActionRow) *string { return a.UserID })
	t.actionBySchedule = target("epa_no_duplicate_schedules", func(a ActionRow) *string { return a.ScheduleID })
	t.actionByRotation = target("epa_no_duplicate_rotations", func(a ActionRow) *string { return a.RotationID })
	t.actionByChannel = target("epa_no_duplicate_channels", func(a ActionRow) *string { return a.ChannelID })

	t.StatesByService = t.States.Index("service_id", func(st StateRow) []string { return []string{st.ServiceID} })
	t.StatesByPolicy = t.States.Index("idx_escalation_policy_state_policy_ids", func(st StateRow) []string { return []string{st.PolicyID} })
	t.States.Unique("escalation_policy_state_uniq_id", func(st StateRow) (string, bool) { return filestore.IntKey(st.ID), true })

	// The order matters when a policy is deleted: its states go before its
	// steps, since clearing a state's step would write a state whose policy
	// is already gone.
	filestore.References(t.States, "escalation_policy_state_escalation_policy_id_fkey", t.Policies,
		func(st StateRow) (string, bool) { return st.PolicyID, true }, filestore.Cascade, nil)
	filestore.References(t.Steps, "escalation_policy_steps_escalation_policy_id_fkey", t.Policies,
		func(st StepRow) (string, bool) { return st.PolicyID, true }, filestore.Cascade, nil)
	filestore.References(t.Actions, "escalation_policy_actions_escalation_policy_step_id_fkey", t.Steps,
		func(a ActionRow) (string, bool) { return a.StepID, true }, filestore.Cascade, nil)
	filestore.References(t.States, "escalation_policy_state_escalation_policy_step_id_fkey", t.Steps,
		func(st StateRow) (string, bool) { return filestore.Ref(st.StepID) }, filestore.SetNull,
		func(st StateRow) StateRow { st.StepID = nil; return st })
	return t
}

// DeclareDeltaForeignKeys declares this package's foreign keys to other
// packages' tables.
//
// svc_ep_fkey, of a state's service and policy to services, is not one
// filestore can declare. InsertState and UpdateState check it, and
// ClearEscalationPolicyState is its ON UPDATE CASCADE; its ON DELETE CASCADE
// is escalation_policy_state_service_id_fkey's.
func DeclareDeltaForeignKeys[Alert, Service, User, Schedule, Rotation, Channel any](
	t *DeltaTables,
	alerts *filestore.Table[Alert],
	services *filestore.Table[Service],
	users *filestore.Table[User],
	schedules *filestore.Table[Schedule],
	rotations *filestore.Table[Rotation],
	channels *filestore.Table[Channel],
) {
	filestore.References(t.States, "escalation_policy_state_alert_id_fkey", alerts,
		func(st StateRow) (string, bool) { return filestore.IntKey(st.AlertID), true }, filestore.Cascade, nil)
	filestore.References(t.States, "escalation_policy_state_service_id_fkey", services,
		func(st StateRow) (string, bool) { return st.ServiceID, true }, filestore.Cascade, nil)
	filestore.References(t.Actions, "escalation_policy_actions_user_id_fkey", users,
		func(a ActionRow) (string, bool) { return filestore.Ref(a.UserID) }, filestore.Cascade, nil)
	filestore.References(t.Actions, "escalation_policy_actions_schedule_id_fkey1", schedules,
		func(a ActionRow) (string, bool) { return filestore.Ref(a.ScheduleID) }, filestore.Cascade, nil)
	filestore.References(t.Actions, "escalation_policy_actions_rotation_id_fkey", rotations,
		func(a ActionRow) (string, bool) { return filestore.Ref(a.RotationID) }, filestore.Cascade, nil)
	filestore.References(t.Actions, "escalation_policy_actions_channel_id_fkey", channels,
		func(a ActionRow) (string, bool) { return filestore.Ref(a.ChannelID) }, filestore.Cascade, nil)
}

// SetLookups gives the tables' functions what they need from other packages'
// tables; NewDeltaStore does so too. Call it before the tables are in use.
func (t *DeltaTables) SetLookups(l DeltaLookups) { t.lookups = l }

// missing is the error of a function that needs a lookup that is not set.
func missing(lookup string) error {
	return fmt.Errorf("escalation: %s: %w", lookup, deltatxn.ErrNotPorted)
}

// checkLimit returns an error if count is more than a limit allows. A limit
// of -1 allows any number.
func checkLimit(table, constraint string, limit, count int) error {
	if limit == -1 || count <= limit {
		return nil
	}
	return &filestore.CheckError{
		Table: table, Constraint: constraint,
		Message: "limit exceeded", HintText: "max=" + strconv.Itoa(limit),
	}
}

// EscalationPolicyName returns a policy's name in r, and whether there is
// one.
func (t *DeltaTables) EscalationPolicyName(r filestore.Reader, id string) (string, bool) {
	p, ok := t.Policies.Get(r, id)
	return p.Name, ok
}

// StepInfo is where a step is.
type StepInfo struct {
	PolicyID, PolicyName string
	StepNumber           int
}

// Step returns, in r, the policy and number of a step, and whether there is
// one.
func (t *DeltaTables) Step(r filestore.Reader, stepID string) (StepInfo, bool) {
	st, ok := t.Steps.Get(r, stepID)
	if !ok {
		return StepInfo{}, false
	}
	p, ok := t.Policies.Get(r, st.PolicyID)
	return StepInfo{PolicyID: p.ID, PolicyName: p.Name, StepNumber: int(st.StepNumber)}, ok
}

// PolicySteps returns a policy's steps in r, in the order of their numbers.
func (t *DeltaTables) PolicySteps(r filestore.Reader, policyID string) []StepRow {
	var steps []StepRow
	t.StepsByPolicy.Ascend(r, policyID, func(st StepRow) bool {
		steps = append(steps, st)
		return true
	})
	slices.SortFunc(steps, func(a, b StepRow) int { return cmp.Compare(a.StepNumber, b.StepNumber) })
	return steps
}

// PolicyStep returns, in r, the step of a policy that has a number.
func (t *DeltaTables) PolicyStep(r filestore.Reader, policyID string, number int) (StepRow, bool) {
	return t.stepByNumber.Get(r, stepNumberKey(policyID, int32(number)))
}

// PolicyHasSteps reports whether a policy has a step in r.
func (t *DeltaTables) PolicyHasSteps(r filestore.Reader, policyID string) bool {
	found := false
	t.StepsByPolicy.Ascend(r, policyID, func(StepRow) bool { found = true; return false })
	return found
}

// PolicyMinutes returns how long, in r, a policy goes on for until it has
// repeated for the last time: 0 for one without steps, or that doesn't exist.
func (t *DeltaTables) PolicyMinutes(r filestore.Reader, policyID string) int {
	p, ok := t.Policies.Get(r, policyID)
	if !ok {
		return 0
	}
	minutes := 0
	t.StepsByPolicy.Ascend(r, policyID, func(st StepRow) bool {
		minutes += int(st.Delay) * (int(p.Repeat) + 1)
		return true
	})
	return minutes
}

// InsertStep adds a step to the end of its policy and returns it with its
// number; without an id, it gets one. In the same transaction (F2), it does the
// work of these triggers:
//
//   - trg_inc_ep_step_number_on_insert: the step's number is the number of
//     steps before it, whatever number it was given;
//   - trg_10_incr_ep_step_count_on_add: it counts in the policy's step_count;
//   - trg_enforce_ep_step_limit;
//   - trg_10_insert_ep_state_on_step_insert: with the policy's first step, the
//     open alerts of its services start to escalate.
//
// An alert can still have the state it had when the policy lost its last
// step. It keeps it, and escalates from there.
func (t *DeltaTables) InsertStep(tx *filestore.Tx, st StepRow) (StepRow, error) {
	l := t.lookups
	switch {
	case l.StepLimit == nil:
		return StepRow{}, missing("StepLimit")
	case l.PolicyServices == nil:
		return StepRow{}, missing("PolicyServices")
	case l.OpenAlerts == nil:
		return StepRow{}, missing("OpenAlerts")
	}

	if st.ID == "" {
		st.ID = uuid.NewString()
	}
	n := t.StepsByPolicy.Count(tx, st.PolicyID)
	st.StepNumber = int32(n)
	if err := t.Steps.Insert(tx, st, "escalation_policy_steps_pkey"); err != nil {
		return StepRow{}, err // including a policy that doesn't exist
	}
	if err := t.addStepCount(tx, st.PolicyID, 1); err != nil {
		return StepRow{}, err
	}
	if err := checkLimit("escalation_policy_steps", "ep_steps_per_policy_limit", l.StepLimit(tx), n+1); err != nil {
		return StepRow{}, err
	}
	if n > 0 {
		return st, nil
	}
	for _, svc := range l.PolicyServices(tx, st.PolicyID) {
		if err := t.startAlerts(tx, svc, st.PolicyID); err != nil {
			return StepRow{}, err
		}
	}
	return st, nil
}

func (t *DeltaTables) addStepCount(tx *filestore.Tx, policyID string, n int32) error {
	p, ok := t.Policies.Get(tx, policyID)
	if !ok {
		return nil // an UPDATE of no rows
	}
	p.StepCount += n
	return t.Policies.Put(tx, p)
}

// DeleteStep removes a step, and returns the row and whether there was one.
// In the same transaction, it does the work of trg_10_decr_ep_step_count_on_del
// and trg_decr_ep_step_number_on_delete: the policy's step_count goes down,
// and the numbers of the steps after it close the gap. The foreign keys remove
// its actions, and alerts that were on it are left on no step.
//
// Deleting a policy deletes its steps without this: there is no count or
// number left to keep.
func (t *DeltaTables) DeleteStep(tx *filestore.Tx, id string) (StepRow, bool, error) {
	st, ok := t.Steps.Get(tx, id)
	if !ok {
		return StepRow{}, false, nil
	}
	if _, err := t.Steps.Delete(tx, id); err != nil {
		return StepRow{}, false, err
	}
	if err := t.addStepCount(tx, st.PolicyID, -1); err != nil {
		return StepRow{}, false, err
	}
	// In ascending order, each number is free when a step moves to it.
	for _, next := range t.PolicySteps(tx, st.PolicyID) {
		if next.StepNumber < st.StepNumber {
			continue
		}
		next.StepNumber--
		if err := t.Steps.Put(tx, next); err != nil {
			return StepRow{}, false, err
		}
	}
	return st, true, nil
}

// updateStep changes a step; a step that doesn't exist stays so. change must
// not change the step's policy or number.
func (t *DeltaTables) updateStep(tx *filestore.Tx, id string, change func(st *StepRow)) error {
	st, ok := t.Steps.Get(tx, id)
	if !ok {
		return nil // an UPDATE of no rows
	}
	change(&st)
	return t.Steps.Put(tx, st)
}

// setStepNumber gives a step a number, and the step that has the number this
// one's. If the step doesn't exist, it does nothing.
//
// On Postgres, a transaction that reorders a policy sets one step's number at
// a time, and the numbers are checked when it commits: each must be used once
// (escalation_policy_steps_escalation_policy_id_step_number_key) and none
// skipped (trg_ep_step_number_no_gaps). A transaction here has no such moment,
// and a policy that was left with a gap would stop escalating (F2), so the
// numbers are right after every change. Swapping gives the same result as
// Postgres for every transaction that it would commit, as long as each step's
// number is set once: TestDeltaReorder tries them all. A number that would
// leave a gap is refused at once.
//
// When the change is a transaction of its own (alone), the number must be
// free: Postgres would refuse it when it commits.
func (t *DeltaTables) setStepNumber(tx *filestore.Tx, id string, number int, alone bool) error {
	st, ok := t.Steps.Get(tx, id)
	if !ok {
		return nil // an UPDATE of no rows
	}
	if number < 0 || number >= t.StepsByPolicy.Count(tx, st.PolicyID) {
		return &filestore.CheckError{
			Table: "escalation_policy_steps", Constraint: "trg_ep_step_number_no_gaps",
			Message: "must not have gap in step_numbers",
		}
	}
	old := st.StepNumber
	if number == int(old) {
		return nil
	}
	if other, ok := t.PolicyStep(tx, st.PolicyID, number); ok {
		if alone {
			return &filestore.UniqueError{
				Table: "escalation_policy_steps", Index: "escalation_policy_steps_escalation_policy_id_step_number_key",
				Key: stepNumberKey(st.PolicyID, int32(number)),
			}
		}
		st.StepNumber = -1 // until the other step is out of the way
		if err := t.Steps.Put(tx, st); err != nil {
			return err
		}
		other.StepNumber = old
		if err := t.Steps.Put(tx, other); err != nil {
			return err
		}
	}
	st.StepNumber = int32(number)
	return t.Steps.Put(tx, st)
}

// actionIDs is the version of the UUIDs that newActionID makes.
const actionIDs = 7

// newActionID returns an id for an action that is added at now to a step
// whose last id was last. Ids sort in the order that a step's actions were
// added, which is the order that they are listed in.
func newActionID(now time.Time, last uuid.UUID) uuid.UUID {
	id := uuid.New()
	// A version 7 UUID: milliseconds, then a counter, then random bits.
	prefix := uint64(now.UnixMilli())<<16 | actionIDs<<12
	if last.Version() == actionIDs {
		prefix = max(prefix, binary.BigEndian.Uint64(last[:8])+1)
	}
	binary.BigEndian.PutUint64(id[:8], prefix)
	return id
}

// InsertAction adds an action to a step; without an id, it gets one. It
// checks epa_there_can_only_be_one, and the limit that
// trg_enforce_ep_step_action_limit enforces.
func (t *DeltaTables) InsertAction(tx *filestore.Tx, a ActionRow) error {
	if t.lookups.ActionLimit == nil {
		return missing("ActionLimit")
	}
	targets := 0
	for _, id := range []*string{a.UserID, a.ScheduleID, a.RotationID, a.ChannelID} {
		if id != nil {
			targets++
		}
	}
	if targets != 1 {
		return &filestore.CheckError{Table: "escalation_policy_actions", Constraint: "epa_there_can_only_be_one"}
	}
	n := 0
	var last uuid.UUID
	t.ActionsByStep.Ascend(tx, a.StepID, func(other ActionRow) bool {
		n++
		if id, err := uuid.Parse(other.ID); err == nil && id.Version() == actionIDs {
			last = id
		}
		return true
	})
	if a.ID == "" {
		a.ID = newActionID(tx.Now(), last).String()
	}
	if err := t.Actions.Insert(tx, a, "escalation_policy_actions_pkey"); err != nil {
		return err
	}
	return checkLimit("escalation_policy_actions", "ep_actions_per_step_limit", t.lookups.ActionLimit(tx), n+1)
}

// deleteAction removes the action of a step that has a's target.
func (t *DeltaTables) deleteAction(tx *filestore.Tx, a ActionRow) error {
	for _, target := range []struct {
		id  *string
		key *filestore.Unique[ActionRow]
	}{
		{a.UserID, t.actionByUser}, {a.ScheduleID, t.actionBySchedule},
		{a.RotationID, t.actionByRotation}, {a.ChannelID, t.actionByChannel},
	} {
		if target.id == nil {
			continue
		}
		row, ok := target.key.Get(tx, actionKey(a.StepID, *target.id))
		if !ok {
			continue
		}
		if _, err := t.Actions.Delete(tx, row.ID); err != nil {
			return err
		}
	}
	return nil
}

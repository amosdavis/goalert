package escalation

import (
	"context"

	"github.com/google/uuid"
	"github.com/target/goalert/notification/nfydest"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/validation/validate"
)

// Store provides escalation policy operations on the Delta backend.
type Store struct {
	delta *deltaStore // deltastore.go
	reg   *nfydest.Registry
}

// FindManyPolicies returns escalation policies for the given IDs.
func (s *Store) FindManyPolicies(ctx context.Context, ids []string) ([]Policy, error) {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}

	err = validate.ManyUUID("EscalationPolicyID", ids, 200)
	if err != nil {
		return nil, err
	}

	return s.delta.findManyPolicies(ctx, ids)
}

// CreatePolicyTx creates a new escalation policy in the database.
func (s *Store) CreatePolicyTx(ctx context.Context, tx txn.Tx, p *Policy) (*Policy, error) {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return nil, err
	}

	n, err := p.Normalize()
	if err != nil {
		return nil, err
	}

	n.ID = uuid.New().String()

	if err := s.delta.createPolicy(ctx, tx, n); err != nil {
		return nil, err
	}
	return n, nil
}

// UpdatePolicyTx will update a single escalation policy.
func (s *Store) UpdatePolicyTx(ctx context.Context, tx txn.Tx, p *Policy) error {
	err := validate.UUID("EscalationPolicyID", p.ID)
	if err != nil {
		return err
	}
	n, err := p.Normalize()
	if err != nil {
		return err
	}

	err = permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return err
	}

	return s.delta.updatePolicy(ctx, tx, n)
}

// DeleteManyPoliciesTx deletes multiple policies in a single transaction.
func (s *Store) DeleteManyPoliciesTx(ctx context.Context, tx txn.Tx, ids []string) error {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return err
	}
	err = validate.ManyUUID("EscalationPolicyID", ids, 50)
	if err != nil {
		return err
	}

	return s.delta.deletePolicies(ctx, tx, ids)
}

// FindOnePolicyTx returns a policy by ID.
func (s *Store) FindOnePolicyTx(ctx context.Context, tx txn.Tx, id string) (*Policy, error) {
	err := validate.UUID("EscalationPolicyID", id)
	if err != nil {
		return nil, err
	}

	err = permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}

	return s.delta.findOnePolicy(ctx, tx, id, false)
}

// FindOnePolicyForUpdateTx returns a single policy locked to the tx for updating.
func (s *Store) FindOnePolicyForUpdateTx(ctx context.Context, tx txn.Tx, id string) (*Policy, error) {
	err := validate.UUID("EscalationPolicyID", id)
	if err != nil {
		return nil, err
	}

	err = permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}

	return s.delta.findOnePolicy(ctx, tx, id, true)
}

// FindAllPoliciesBySchedule will return all policies that have the given schedule assigned to them.
func (s *Store) FindAllPoliciesBySchedule(ctx context.Context, scheduleID string) ([]Policy, error) {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}
	err = validate.UUID("ScheduleID", scheduleID)
	if err != nil {
		return nil, err
	}
	return s.delta.findAllPoliciesBySchedule(ctx, scheduleID)
}

// FindOneStepForUpdateTx returns a step locked within the tx for update.
func (s *Store) FindOneStepForUpdateTx(ctx context.Context, tx txn.Tx, id string) (*Step, error) {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}

	err = validate.UUID("EscalationPolicyStepID ", id)
	if err != nil {
		return nil, err
	}

	return s.delta.findOneStep(ctx, tx, id)
}

func (s *Store) FindAllSteps(ctx context.Context, policyID string) ([]Step, error) {
	return s.FindAllStepsTx(ctx, nil, policyID)
}

// FindAllOnCallStepsForUserTx returns all steps a user is currently on-call for.
func (s *Store) FindAllOnCallStepsForUserTx(ctx context.Context, tx txn.Tx, userID string) ([]Step, error) {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}
	err = validate.UUID("UserID", userID)
	if err != nil {
		return nil, err
	}

	return s.delta.findAllOnCallSteps(ctx, tx, userID)
}

// FindAllStepsTx returns all steps for a policy.
func (s *Store) FindAllStepsTx(ctx context.Context, tx txn.Tx, policyID string) ([]Step, error) {
	err := validate.UUID("EscalationPolicyID", policyID)
	if err != nil {
		return nil, err
	}

	err = permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}

	return s.delta.findAllSteps(ctx, tx, policyID)
}

// CreateStepTx adds a step to an escalation policy.
func (s *Store) CreateStepTx(ctx context.Context, tx txn.Tx, st *Step) (*Step, error) {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return nil, err
	}

	n, err := st.Normalize()
	if err != nil {
		return nil, err
	}

	n.ID = uuid.New()

	if err := s.delta.createStep(ctx, tx, n); err != nil {
		return nil, err
	}
	return n, nil
}

// UpdateStepNumberTx updates the step number for a step.
func (s *Store) UpdateStepNumberTx(ctx context.Context, tx txn.Tx, stepID uuid.UUID, stepNumber int) error {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return err
	}

	return s.delta.updateStepNumber(ctx, tx, stepID, stepNumber)
}

// UpdateStepDelayTx updates the delay for a step.
func (s *Store) UpdateStepDelayTx(ctx context.Context, tx txn.Tx, stepID uuid.UUID, stepDelay int) error {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return err
	}

	err = validate.Range("DelayMinutes", stepDelay, 1, 9000)
	if err != nil {
		return err
	}

	return s.delta.updateStepDelay(ctx, tx, stepID, stepDelay)
}

// UpdateStepMultiAckTx updates the multi-ack setting for a step.
func (s *Store) UpdateStepMultiAckTx(ctx context.Context, tx txn.Tx, stepID uuid.UUID, multiAck bool) error {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return err
	}

	return s.delta.updateStepMultiAck(ctx, tx, stepID, multiAck)
}

// DeleteStepTx deletes a step from an escalation policy.
func (s *Store) DeleteStepTx(ctx context.Context, tx txn.Tx, id uuid.UUID) (string, error) {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return "", err
	}
	return s.delta.deleteStep(ctx, tx, id)
}

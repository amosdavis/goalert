package alert

import (
	"context"
	"database/sql"
	"time"

	"github.com/target/goalert/alert/alertlog"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util/log"
	"github.com/target/goalert/validation"
	"github.com/target/goalert/validation/validate"
)

const maxBatch = 500

// Store provides alert operations on the Delta backend.
type Store struct {
	delta *deltaAlerts
}

// A Trigger signals that an alert needs to be processed
type Trigger interface {
	TriggerAlert(int)
}

// EventAlertEscalated is sent when an alert is escalated.
type EventAlertEscalated struct {
	AlertID int64
}

// ServiceInfo will return the name of the given service ID as well as the current number
// of unacknowledged alerts.
func (s *Store) ServiceInfo(ctx context.Context, serviceID string) (string, int, error) {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return "", 0, err
	}

	err = validate.UUID("ServiceID", serviceID)
	if err != nil {
		return "", 0, err
	}

	return s.delta.serviceInfo(ctx, canonicalID(serviceID))
}

func (s *Store) EPID(ctx context.Context, alertID int) (string, error) {
	err := permission.LimitCheckAny(ctx, permission.System)
	if err != nil {
		return "", err
	}

	return s.delta.epID(ctx, alertID)
}

// EscalateAsOf will request escalation for the given alert ID as-of the given time.
//
// An error will be returned if the alert is already closed, if the service is
// in maintenance mode, there are no steps on the escalation policy, or if the
// alert has already been escalated since the given time.
func (s *Store) EscalateAsOf(ctx context.Context, id int, t time.Time) error {
	err := permission.LimitCheckAny(ctx, permission.System, permission.User)
	if err != nil {
		return err
	}

	return s.delta.escalateAsOf(ctx, id, t)
}

func (s *Store) Escalate(ctx context.Context, alertID int, currentLevel int) error {
	return s.EscalateAsOf(ctx, alertID, time.Time{})
}

func (s *Store) EscalateMany(ctx context.Context, alertIDs []int) ([]int, error) {
	err := permission.LimitCheckAny(ctx, permission.System, permission.User)
	if err != nil {
		return nil, err
	}

	if len(alertIDs) == 0 {
		return nil, nil
	}

	err = validate.Range("AlertIDs", len(alertIDs), 1, 1)
	if err != nil {
		return nil, err
	}
	err = s.EscalateAsOf(ctx, alertIDs[0], time.Time{})
	if err != nil {
		return nil, err
	}

	return alertIDs, nil
}

// EventAlertStatusUpdate is sent when an alert's status is updated.
//
// Note: It is not a guarantee that the status differs from the previous status in all cases.
type EventAlertStatusUpdate struct {
	AlertID int64
	Status  Status

	Created bool
}

func (s *Store) UpdateStatusByService(ctx context.Context, serviceID string, status Status) error {
	err := permission.LimitCheckAny(ctx, permission.System, permission.Admin, permission.User)
	if err != nil {
		return err
	}

	err = validate.UUID("ServiceID", serviceID)
	if err != nil {
		return err
	}

	err = validate.OneOf("Status", status, StatusActive, StatusClosed)
	if err != nil {
		return err
	}

	t := alertlog.TypeAcknowledged
	if status == StatusClosed {
		t = alertlog.TypeClosed
	}

	return s.delta.updateStatusByService(ctx, serviceID, status, t)
}

func (s *Store) UpdateManyAlertStatus(ctx context.Context, status Status, alertIDs []int, logMeta interface{}) ([]int, error) {
	err := permission.LimitCheckAny(ctx, permission.System, permission.User)
	if err != nil {
		return nil, err
	}

	if len(alertIDs) == 0 {
		return nil, nil
	}

	err = validate.Many(
		validate.Range("AlertIDs", len(alertIDs), 1, maxBatch),
		validate.OneOf("Status", status, StatusActive, StatusClosed),
	)
	if err != nil {
		return nil, err
	}

	t := alertlog.TypeAcknowledged
	if status == StatusClosed {
		t = alertlog.TypeClosed
	}

	return s.delta.updateMany(ctx, nil, status, alertIDs, t, logMeta)
}

func (s *Store) CreateTx(ctx context.Context, tx txn.Tx, a *Alert) (*Alert, error) {
	n, err := a.Normalize() // validation
	if err != nil {
		return nil, err
	}

	if n.Status == StatusClosed {
		return nil, validation.NewFieldError("Status", "Cannot create a closed alert.")
	}
	err = permission.LimitCheckAny(ctx,
		permission.System,
		permission.Admin,
		permission.User,
		permission.MatchService(a.ServiceID),
	)
	if err != nil {
		return nil, err
	}

	n, err = s.delta.create(ctx, tx, *n)
	if err != nil {
		return nil, err
	}
	logCreated(ctx, n)
	return n, nil
}

func logCreated(ctx context.Context, a *Alert) {
	ctx = log.WithFields(ctx, log.Fields{"AlertID": a.ID, "ServiceID": a.ServiceID})
	log.Logf(ctx, "Alert created.")
	metricCreatedTotal.WithLabelValues(a.ServiceID).Inc()
}

// CreateOrUpdateTx returns `isNew` to indicate if the returned alert was a new one.
// It is the caller's responsibility to log alert creation if the transaction is committed (and isNew is true).
func (s *Store) CreateOrUpdateTx(ctx context.Context, tx txn.Tx, a *Alert) (*Alert, bool, error) {
	err := permission.LimitCheckAny(ctx,
		permission.System,
		permission.Admin,
		permission.User,
		permission.MatchService(a.ServiceID),
	)
	if err != nil {
		return nil, false, err
	}
	/*
		- if new status is triggered, create or return existing

		- if new status is ack, old is trig, ack and return existing
		- if new status is ack, old is ack, return existing
		- if new status is ack, old is close, return nil

		- if new status is close, old is ack or trig, close, return existing
		- if new status is close, old is close, return nil
	*/

	n, err := a.Normalize() // validation
	if err != nil {
		return nil, false, err
	}

	return s.delta.createOrUpdate(ctx, tx, *n)
}

// CreateOrUpdate will create an alert or log a "duplicate suppressed message" if
// Status is Triggered. If Status is Closed, it will close and return the result.
//
// In the case that Status is closed but a matching alert is not present, nil is returned.
// Otherwise the current alert is returned.
func (s *Store) CreateOrUpdate(ctx context.Context, a *Alert) (*Alert, bool, error) {
	return s.createOrUpdate(ctx, a, nil)
}

// CreateOrUpdateWithMeta behaves the same as CreateOrUpdate, but also sets metadata on the alert if it is new.
func (s *Store) CreateOrUpdateWithMeta(ctx context.Context, a *Alert, meta map[string]string) (*Alert, bool, error) {
	return s.createOrUpdate(ctx, a, meta)
}

func (s *Store) createOrUpdate(ctx context.Context, a *Alert, meta map[string]string) (*Alert, bool, error) {
	err := permission.LimitCheckAny(ctx,
		permission.System,
		permission.Admin,
		permission.User,
		permission.MatchService(a.ServiceID),
	)
	if err != nil {
		return nil, false, err
	}

	// The function can run more than once when concurrent transactions conflict.
	var n *Alert
	var isNew bool
	err = s.runTx(ctx, func(tx txn.Tx) error {
		var err error
		n, isNew, err = s.CreateOrUpdateTx(ctx, tx, a)
		if err != nil {
			return err
		}

		// Set metadata only if meta is not nil and isNew is true
		if meta != nil && isNew {
			return s.SetMetadataTx(ctx, tx, n.ID, meta)
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}

	if n == nil {
		return nil, false, nil
	}
	if isNew {
		logCreated(ctx, n)
	}

	return n, isNew, nil
}

// runTx runs fn in a transaction.
func (s *Store) runTx(ctx context.Context, fn func(tx txn.Tx) error) error {
	s.delta.log.Prepare(ctx)
	return s.delta.db.Run(ctx, nil, fn)
}

func (s *Store) UpdateStatusTx(ctx context.Context, tx txn.Tx, id int, stat Status) error {
	return s.delta.updateStatus(ctx, tx, id, stat)
}

func (s *Store) UpdateStatus(ctx context.Context, id int, stat Status) error {
	err := validate.OneOf("Status", stat, StatusTriggered, StatusActive, StatusClosed)
	if err != nil {
		return err
	}
	return s.delta.run(ctx, nil, func(tx txn.Tx, sc deltatxn.Scope) error {
		err := s.delta.canTouch(ctx, sc.Reader, id)
		if err != nil {
			return err
		}
		return s.delta.updateStatus(ctx, tx, id, stat)
	})
}

func (s *Store) FindOne(ctx context.Context, id int) (*Alert, error) {
	alerts, err := s.FindMany(ctx, []int{id})
	if err != nil {
		return nil, err
	}
	// If alert is not found
	if len(alerts) == 0 {
		return nil, sql.ErrNoRows
	}
	return &alerts[0], nil
}

func (s *Store) FindMany(ctx context.Context, alertIDs []int) ([]Alert, error) {
	err := permission.LimitCheckAny(ctx, permission.System, permission.User)
	if err != nil {
		return nil, err
	}
	if len(alertIDs) == 0 {
		return nil, nil
	}

	err = validate.Range("AlertIDs", len(alertIDs), 1, maxBatch)
	if err != nil {
		return nil, err
	}

	return s.delta.findMany(ctx, alertIDs)
}

func (s *Store) State(ctx context.Context, alertIDs []int) ([]State, error) {
	err := permission.LimitCheckAny(ctx, permission.System, permission.User)
	if err != nil {
		return nil, err
	}

	err = validate.Range("AlertIDs", len(alertIDs), 1, maxBatch)
	if err != nil {
		return nil, err
	}

	return s.delta.state(ctx, alertIDs)
}

func (s *Store) Feedback(ctx context.Context, alertIDs []int) ([]Feedback, error) {
	err := permission.LimitCheckAny(ctx, permission.System, permission.User)
	if err != nil {
		return nil, err
	}
	err = validate.Range("AlertIDs", len(alertIDs), 1, maxBatch)
	if err != nil {
		return nil, err
	}

	return s.delta.findFeedback(ctx, alertIDs)
}

func (s Store) UpdateManyAlertFeedback(ctx context.Context, noiseReason string, alertIDs []int) ([]int, error) {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return nil, err
	}

	err = validate.Many(
		validate.Range("AlertIDs", len(alertIDs), 1, maxBatch),
		validate.Text("NoiseReason", noiseReason, 1, 255),
	)
	if err != nil {
		return nil, err
	}

	return s.delta.setFeedback(ctx, noiseReason, alertIDs...)
}

func (s Store) UpdateFeedback(ctx context.Context, feedback *Feedback) error {
	err := permission.LimitCheckAny(ctx, permission.System, permission.User)
	if err != nil {
		return err
	}

	err = validate.Text("NoiseReason", feedback.NoiseReason, 1, 255)
	if err != nil {
		return err
	}

	_, err = s.delta.setFeedback(ctx, feedback.NoiseReason, feedback.ID)
	return err
}

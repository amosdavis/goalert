package notice

import (
	"context"
	"strings"

	"github.com/target/goalert/filestore"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
)

// DeltaLookups are the reads of other packages' tables that notices come
// from. A method that needs one that is nil returns deltatxn.ErrNotPorted.
type DeltaLookups struct {
	// PolicyServices returns how many services use an escalation policy
	// (services).
	PolicyServices func(r filestore.Reader, policyID string) int

	// PolicyMinutes returns how long an escalation policy goes on for until
	// it has repeated for the last time (escalation_policy_steps and
	// escalation_policies).
	PolicyMinutes func(r filestore.Reader, policyID string) int

	// UnackedAlerts returns how many alerts of a service are triggered
	// (alerts).
	UnackedAlerts func(r filestore.Reader, serviceID string) int

	// UnackedAlertLimit is the unacked_alerts_per_service row of
	// config_limits, -1 for no limit.
	UnackedAlertLimit func(r filestore.Reader) int
}

// NewDeltaStore returns a notice store on the Delta backend. It has no tables
// of its own.
func NewDeltaStore(db txn.DB, lookups DeltaLookups) *Store {
	return &Store{delta: &deltaNotices{db: db, DeltaLookups: lookups}}
}

type deltaNotices struct {
	db txn.DB
	DeltaLookups
}

// policyStats returns how many services use a policy and how many minutes it
// goes on for, both at the same moment.
func (d *deltaNotices) policyStats(ctx context.Context, policyID string) (services, minutes int, err error) {
	if d.PolicyServices == nil || d.PolicyMinutes == nil {
		return 0, 0, deltatxn.ErrNotPorted
	}
	id := strings.ToLower(policyID) // as Postgres gives a valid UUID
	err = deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		services, minutes = d.PolicyServices(s.Reader, id), d.PolicyMinutes(s.Reader, id)
		return nil
	})
	return services, minutes, err
}

// unackedAlerts returns how many unacknowledged alerts a service has, and may
// have, both at the same moment.
func (d *deltaNotices) unackedAlerts(ctx context.Context, serviceID string) (count, limit int, err error) {
	if d.UnackedAlerts == nil || d.UnackedAlertLimit == nil {
		return 0, 0, deltatxn.ErrNotPorted
	}
	id := strings.ToLower(serviceID)
	err = deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		count, limit = d.UnackedAlerts(s.Reader, id), d.UnackedAlertLimit(s.Reader)
		return nil
	})
	return count, limit, err
}

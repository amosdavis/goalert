package notice

import (
	"context"
	"fmt"

	"github.com/target/goalert/config"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/validation/validate"
)

// Store allows identifying notices for various targets on the Delta backend.
type Store struct {
	delta *deltaNotices
}

// FindAllPolicyNotices sets a notice for a Policy if it is not assigned to any services.
func (s *Store) FindAllPolicyNotices(ctx context.Context, policyID string) ([]Notice, error) {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return nil, err
	}

	err = validate.UUID("EscalationPolicyStepID", policyID)
	if err != nil {
		return nil, err
	}

	numServices, mins, err := s.delta.policyStats(ctx, policyID)
	if err != nil {
		return nil, err
	}
	return policyNotices(ctx, numServices, mins), nil
}

// policyNotices returns the notices for a policy that numServices services use
// and that goes on for a number of minutes.
func policyNotices(ctx context.Context, numServices, mins int) []Notice {
	var notices []Notice
	if numServices == 0 {
		notices = append(notices, Notice{
			Message: "Not assigned to a service",
			Details: "To receive alerts for this configuration, assign this escalation policy to its relevant service.",
		})
	}

	cfg := config.FromContext(ctx)
	days := cfg.Maintenance.AlertAutoCloseDays

	if days > 0 && mins/(24*60) >= days {
		notices = append(notices, Notice{
			Message: "Auto-closure of unacknowledged alerts",
			Details: fmt.Sprintf("Alerts using this policy will be automatically closed after %d day(s).", days),
		})
	}

	return notices
}

// FindAllServiceNotices returns any relevant notices for the given service. Currently returns
// notices pertaining to the system limit for unacknowledged alerts.
func (s *Store) FindAllServiceNotices(ctx context.Context, serviceID string) ([]Notice, error) {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return nil, err
	}

	err = validate.UUID("serviceID", serviceID)
	if err != nil {
		return nil, err
	}

	count, limit, err := s.delta.unackedAlerts(ctx, serviceID)
	if err != nil {
		return nil, err
	}
	return serviceNotices(float32(count), float32(limit)), nil
}

// serviceNotices returns the notices for a service with count unacknowledged
// alerts.
func serviceNotices(count, limit float32) []Notice {
	if limit < 0 { // limit is disabled
		return nil
	}
	details := fmt.Sprintf("New alerts will be rejected while at or above the limit (%v), acknowledge or close alerts to resolve.", count)

	// hit system limit notice
	if count >= limit {
		return []Notice{
			{
				Type:    TypeError,
				Message: "Unacknowledged alert limit reached",
				Details: details,
			},
		}
	}

	// nearing system limit notice
	if count/limit > 0.75 {
		return []Notice{
			{
				Type:    TypeWarning,
				Message: "Near unacknowledged alert limit",
				Details: details,
			},
		}
	}

	return nil
}

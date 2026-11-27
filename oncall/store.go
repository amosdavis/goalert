package oncall

import (
	"context"
	"time"

	"github.com/target/goalert/assignment"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/schedule"
	"github.com/target/goalert/schedule/rule"
	"github.com/target/goalert/validation/validate"
)

// ScheduleOnCallUser represents a currently on-call user for a schedule.
type ScheduleOnCallUser struct {
	ID   string
	Name string
}

// ServiceOnCallUser represents a currently on-call user for a service.
type ServiceOnCallUser struct {
	StepNumber int    `json:"step_number"`
	UserID     string `json:"user_id"`
	UserName   string `json:"user_name"`
}

// A Shift represents a duration a user is on-call.
// If truncated is true, then the End time does not represent
// the time the user stopped being on call, instead it indicates
// they were still on-call at that time.
type Shift struct {
	UserID    string               `json:"user_id"`
	Start     time.Time            `json:"start_time"`
	End       time.Time            `json:"end_time"`
	Truncated bool                 `json:"truncated"`
	Target    assignment.RawTarget `json:"target"`
}

// Store retrieves and calculates on-call information on the Delta backend.
type Store struct {
	delta *deltaOnCall

	ruleStore  *rule.Store
	schedStore *schedule.Store

	histLim chan struct{}
}

// OnCallUsersByService will return the current set of users who are on-call for the given service.
func (s *Store) OnCallUsersByService(ctx context.Context, serviceID string) ([]ServiceOnCallUser, error) {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return nil, err
	}
	err = validate.UUID("ServiceID", serviceID)
	if err != nil {
		return nil, err
	}
	return s.delta.usersByService(ctx, serviceID)
}

func (s *Store) OnCallUsersBySchedule(ctx context.Context, scheduleID string) ([]ScheduleOnCallUser, error) {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return nil, err
	}
	err = validate.UUID("ScheduleID", scheduleID)
	if err != nil {
		return nil, err
	}
	return s.delta.usersBySchedule(ctx, scheduleID)
}

// resolveRules returns the rules with their rotations, by rotation ID.
func resolveRules(rawRules []rule.Rule, rots map[string]*ResolvedRotation) []ResolvedRule {
	var rules []ResolvedRule
	for _, r := range rawRules {
		if r.Target.TargetType() == assignment.TargetTypeRotation {
			rules = append(rules, ResolvedRule{
				Rule:     r,
				Rotation: rots[r.Target.TargetID()],
			})
		} else {
			rules = append(rules, ResolvedRule{Rule: r})
		}
	}
	return rules
}

// HistoryBySchedule will return the list of shifts that overlap the start and end time for the given schedule.
func (s *Store) HistoryBySchedule(ctx context.Context, scheduleID string, start, end time.Time) ([]Shift, error) {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return nil, err
	}
	err = validate.UUID("ScheduleID", scheduleID)
	if err != nil {
		return nil, err
	}

	// History calculation is expensive, so limit concurrent operations.
	select {
	case s.histLim <- struct{}{}:
		defer func() { <-s.histLim }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	st, err := s.delta.historyState(ctx, s, scheduleID, start, end)
	if err != nil {
		return nil, err
	}
	return st.CalculateShifts(start, end), nil
}

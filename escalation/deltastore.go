package escalation

import (
	"cmp"
	"context"
	"database/sql"
	"slices"
	"strings"
	"unicode"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/target/goalert/alert/alertlog"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/notification/nfydest"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/schedule"
	"github.com/target/goalert/schedule/rotation"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/user"
	"github.com/target/goalert/util/log"
)

// DeltaLookups are what this package needs from other packages' tables. They
// run in its transactions, so they must do no I/O. What needs one that is nil
// returns deltatxn.ErrNotPorted.
type DeltaLookups struct {
	// StepLimit and ActionLimit are the ep_steps_per_policy and
	// ep_actions_per_step rows of config_limits, -1 for no limit.
	StepLimit, ActionLimit func(r filestore.Reader) int

	// IsFavorite reports whether userID has the policy as a favorite
	// (user_favorites). Without it, no policy is one.
	IsFavorite func(r filestore.Reader, userID, policyID string) bool

	// OnCallSteps returns the ids of the steps that userID is on call for now
	// (ep_step_on_call_users without an end_time), each once.
	OnCallSteps func(r filestore.Reader, userID string) []string

	// ChannelDest returns a notification channel's destination
	// (notification_channels).
	ChannelDest func(r filestore.Reader, channelID string) (gadb.DestV1, error)

	// ServicePolicy returns the policy of a service, and PolicyServices the
	// ids of the services of a policy (services).
	ServicePolicy  func(r filestore.Reader, serviceID string) (policyID string, ok bool)
	PolicyServices func(r filestore.Reader, policyID string) []string

	// AlertService returns the service of an alert, and OpenAlerts the ids of
	// a service's alerts that are not closed (alerts).
	AlertService func(r filestore.Reader, alertID int64) (serviceID string, ok bool)
	OpenAlerts   func(r filestore.Reader, serviceID string) []int64

	// TriggerAlert sets an alert's status to triggered, with whatever else
	// changing the status does (alerts).
	TriggerAlert func(tx *filestore.Tx, alertID int64) error

	// MapDestToID and LookupDestID are the notification channel store's
	// methods.
	MapDestToID, LookupDestID func(ctx context.Context, tx txn.Tx, dest gadb.DestV1) (uuid.UUID, error)

	// LogEP is the alert log store's LogEPTx.
	LogEP func(ctx context.Context, tx txn.Tx, policyID string, typ alertlog.Type, meta *alertlog.EscalationMetaData) error
}

// NewDeltaStore returns an escalation policy store on the Delta backend.
func NewDeltaStore(db txn.DB, tables *DeltaTables, reg *nfydest.Registry, lookups DeltaLookups) *Store {
	tables.SetLookups(lookups)
	return &Store{reg: reg, delta: &deltaStore{db: db, DeltaTables: tables}}
}

type deltaStore struct {
	db txn.DB
	*DeltaTables
}

// rowID returns a valid UUID as Postgres gives it back, and as rows have it.
func rowID(id string) string { return strings.ToLower(id) }

// run runs fn in tx, or in a transaction of its own when tx is nil.
func (d *deltaStore) run(ctx context.Context, tx txn.Tx, fn func(tx txn.Tx) error) error {
	if tx != nil {
		return fn(tx)
	}
	return d.db.Run(ctx, nil, fn)
}

// logChange records a change of a policy in the logs of its open alerts, in
// the same transaction. A change that can't be recorded is still made.
func (d *deltaStore) logChange(ctx context.Context, tx txn.Tx, policyID string) error {
	if d.lookups.LogEP == nil {
		return missing("LogEP")
	}
	if err := d.lookups.LogEP(ctx, tx, policyID, alertlog.TypePolicyUpdated, nil); err != nil {
		log.Log(ctx, errors.Wrap(err, "append alertlog (escalation policy update)"))
	}
	return nil
}

func (d *deltaStore) isFavorite(r filestore.Reader, userID, policyID string) bool {
	return d.lookups.IsFavorite != nil && userID != "" && d.lookups.IsFavorite(r, userID, policyID)
}

// currentUser returns the id of the user making a request, or "".
func currentUser(ctx context.Context) string {
	id := permission.UserNullUUID(ctx)
	if !id.Valid {
		return ""
	}
	return id.UUID.String()
}

func (p PolicyRow) policy(isFavorite bool) Policy {
	return Policy{ID: p.ID, Name: p.Name, Description: p.Description, Repeat: int(p.Repeat), isUserFavorite: isFavorite}
}

func (st StepRow) step() (Step, error) {
	id, err := uuid.Parse(st.ID)
	if err != nil {
		return Step{}, errors.Wrapf(err, "escalation_policy_steps %s", st.ID)
	}
	return Step{
		ID: id, PolicyID: st.PolicyID, DelayMinutes: int(st.Delay),
		StepNumber: int(st.StepNumber), MultiAck: st.MultiAck,
	}, nil
}

// toSteps returns rows as steps, and nil for none.
func toSteps(rows []StepRow) ([]Step, error) {
	var steps []Step
	for _, row := range rows {
		st, err := row.step()
		if err != nil {
			return nil, err
		}
		steps = append(steps, st)
	}
	return steps, nil
}

// findManyPolicies returns those of the policies that exist, each once, in
// the order of ids.
func (d *deltaStore) findManyPolicies(ctx context.Context, ids []string) ([]Policy, error) {
	var policies []Policy
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		policies = nil
		me := currentUser(ctx)
		seen := make(map[string]bool, len(ids))
		for _, id := range ids {
			id = rowID(id)
			if seen[id] {
				continue // like `e.id = any($1)`
			}
			seen[id] = true
			if row, ok := d.Policies.Get(s.Reader, id); ok {
				policies = append(policies, row.policy(d.isFavorite(s.Reader, me, id)))
			}
		}
		return nil
	})
	return policies, err
}

// findOnePolicy returns sql.ErrNoRows for a missing policy. With forUpdate,
// the favorite flag is false, as on Postgres. In a transaction, no other
// change can come between this and the transaction's own (F4).
func (d *deltaStore) findOnePolicy(ctx context.Context, tx txn.Tx, id string, forUpdate bool) (*Policy, error) {
	var p Policy
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.Policies.Get(s.Reader, rowID(id))
		if !ok {
			return sql.ErrNoRows
		}
		p = row.policy(!forUpdate && d.isFavorite(s.Reader, currentUser(ctx), row.ID))
		return nil
	})
	return &p, err
}

func (d *deltaStore) createPolicy(ctx context.Context, tx txn.Tx, p *Policy) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		return d.Policies.Insert(s.Tx, PolicyRow{
			ID: p.ID, Name: p.Name, Description: p.Description, Repeat: int32(p.Repeat),
		}, "escalation_policies_pkey")
	})
}

func (d *deltaStore) updatePolicy(ctx context.Context, tx txn.Tx, p *Policy) error {
	return d.run(ctx, tx, func(tx txn.Tx) error {
		err := deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
			row, ok := d.Policies.Get(s.Reader, rowID(p.ID))
			if !ok {
				return nil // an UPDATE of no rows
			}
			row.Name, row.Description, row.Repeat = p.Name, p.Description, int32(p.Repeat)
			return d.Policies.Put(s.Tx, row)
		})
		if err != nil {
			return err
		}
		return d.logChange(ctx, tx, rowID(p.ID))
	})
}

func (d *deltaStore) deletePolicies(ctx context.Context, tx txn.Tx, ids []string) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		for _, id := range ids {
			if _, err := d.Policies.Delete(s.Tx, rowID(id)); err != nil {
				return err
			}
		}
		return nil
	})
}

// findAllPoliciesBySchedule returns policies in the order of their ids.
func (d *deltaStore) findAllPoliciesBySchedule(ctx context.Context, scheduleID string) ([]Policy, error) {
	var policies []Policy
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		policies = nil
		seen := make(map[string]bool)
		d.actionsBySchedule.Ascend(s.Reader, rowID(scheduleID), func(a ActionRow) bool {
			st, ok := d.Steps.Get(s.Reader, a.StepID)
			if !ok || seen[st.PolicyID] {
				return true
			}
			seen[st.PolicyID] = true
			if p, ok := d.Policies.Get(s.Reader, st.PolicyID); ok {
				policies = append(policies, p.policy(false))
			}
			return true
		})
		slices.SortFunc(policies, func(a, b Policy) int { return cmp.Compare(a.ID, b.ID) })
		return nil
	})
	return policies, err
}

// findOneStep returns sql.ErrNoRows for a missing step. In a transaction, no
// other change can come between this and the transaction's own, as with FOR
// UPDATE (F4).
func (d *deltaStore) findOneStep(ctx context.Context, tx txn.Tx, id string) (*Step, error) {
	var st Step
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.Steps.Get(s.Reader, rowID(id))
		if !ok {
			return sql.ErrNoRows
		}
		var err error
		st, err = row.step()
		return err
	})
	if err != nil {
		return nil, err
	}
	return &st, nil
}

func (d *deltaStore) findAllSteps(ctx context.Context, tx txn.Tx, policyID string) ([]Step, error) {
	var steps []Step
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		var err error
		steps, err = toSteps(d.PolicySteps(s.Reader, rowID(policyID)))
		return err
	})
	return steps, err
}

// findAllOnCallSteps returns the steps in the order of their policies' ids
// and their numbers.
func (d *deltaStore) findAllOnCallSteps(ctx context.Context, tx txn.Tx, userID string) ([]Step, error) {
	if d.lookups.OnCallSteps == nil {
		return nil, missing("OnCallSteps")
	}
	var steps []Step
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		var rows []StepRow
		for _, id := range d.lookups.OnCallSteps(s.Reader, rowID(userID)) {
			if row, ok := d.Steps.Get(s.Reader, id); ok {
				rows = append(rows, row)
			}
		}
		slices.SortFunc(rows, func(a, b StepRow) int {
			return cmp.Or(cmp.Compare(a.PolicyID, b.PolicyID), cmp.Compare(a.StepNumber, b.StepNumber))
		})
		var err error
		steps, err = toSteps(rows)
		return err
	})
	return steps, err
}

// createStep adds a step and sets its number.
func (d *deltaStore) createStep(ctx context.Context, tx txn.Tx, st *Step) error {
	return d.run(ctx, tx, func(tx txn.Tx) error {
		err := deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
			row, err := d.InsertStep(s.Tx, StepRow{
				ID: st.ID.String(), PolicyID: rowID(st.PolicyID), Delay: int32(st.DelayMinutes), MultiAck: st.MultiAck,
			})
			st.StepNumber = int(row.StepNumber)
			return err
		})
		if err != nil {
			return err
		}
		return d.logChange(ctx, tx, rowID(st.PolicyID))
	})
}

func (d *deltaStore) updateStepNumber(ctx context.Context, tx txn.Tx, id uuid.UUID, number int) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		return d.setStepNumber(s.Tx, id.String(), number, tx == nil)
	})
}

func (d *deltaStore) updateStepDelay(ctx context.Context, tx txn.Tx, id uuid.UUID, delay int) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		return d.updateStep(s.Tx, id.String(), func(st *StepRow) { st.Delay = int32(delay) })
	})
}

func (d *deltaStore) updateStepMultiAck(ctx context.Context, tx txn.Tx, id uuid.UUID, multiAck bool) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		return d.updateStep(s.Tx, id.String(), func(st *StepRow) { st.MultiAck = multiAck })
	})
}

// deleteStep returns the policy of the step, or sql.ErrNoRows for a missing
// one.
func (d *deltaStore) deleteStep(ctx context.Context, tx txn.Tx, id uuid.UUID) (string, error) {
	var policyID string
	err := d.run(ctx, tx, func(tx txn.Tx) error {
		err := deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
			row, ok, err := d.DeleteStep(s.Tx, id.String())
			if err != nil {
				return err
			}
			if !ok {
				return sql.ErrNoRows
			}
			policyID = row.PolicyID
			return nil
		})
		if err != nil {
			return err
		}
		return d.logChange(ctx, tx, policyID)
	})
	if err != nil {
		return "", err
	}
	return policyID, nil
}

func (t actionTarget) row(stepID uuid.UUID) ActionRow {
	ref := func(id uuid.NullUUID) *string {
		if !id.Valid {
			return nil
		}
		s := id.UUID.String()
		return &s
	}
	return ActionRow{
		StepID: stepID.String(), UserID: ref(t.userID), ScheduleID: ref(t.scheduleID),
		RotationID: ref(t.rotationID), ChannelID: ref(t.channelID),
	}
}

// addAction adds an action to a step.
func (d *deltaStore) addAction(ctx context.Context, tx txn.Tx, stepID uuid.UUID, dest gadb.DestV1) error {
	return d.run(ctx, tx, func(tx txn.Tx) error {
		tgt, err := parseTarget(dest, func() (uuid.UUID, error) {
			if d.lookups.MapDestToID == nil {
				return uuid.Nil, missing("MapDestToID")
			}
			return d.lookups.MapDestToID(ctx, tx, dest)
		})
		if err != nil {
			return err
		}
		return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
			return d.InsertAction(s.Tx, tgt.row(stepID))
		})
	})
}

// removeAction removes an action from a step.
func (d *deltaStore) removeAction(ctx context.Context, tx txn.Tx, stepID uuid.UUID, dest gadb.DestV1) error {
	return d.run(ctx, tx, func(tx txn.Tx) error {
		tgt, err := parseTarget(dest, func() (uuid.UUID, error) {
			if d.lookups.LookupDestID == nil {
				return uuid.Nil, missing("LookupDestID")
			}
			return d.lookups.LookupDestID(ctx, tx, dest)
		})
		if err != nil {
			return err
		}
		return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
			return d.deleteAction(s.Tx, tgt.row(stepID))
		})
	})
}

// findAllActions returns a step's actions in the order they were added.
func (d *deltaStore) findAllActions(ctx context.Context, tx txn.Tx, stepID uuid.UUID) ([]gadb.DestV1, error) {
	if d.lookups.ChannelDest == nil {
		return nil, missing("ChannelDest")
	}
	var dests []gadb.DestV1
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		dests = []gadb.DestV1{}
		var err error
		d.ActionsByStep.Ascend(s.Reader, stepID.String(), func(a ActionRow) bool {
			switch {
			case a.UserID != nil:
				dests = append(dests, user.DestFromID(*a.UserID))
			case a.ScheduleID != nil:
				dests = append(dests, schedule.DestFromID(*a.ScheduleID))
			case a.RotationID != nil:
				dests = append(dests, rotation.DestFromID(*a.RotationID))
			case a.ChannelID != nil:
				var dest gadb.DestV1
				dest, err = d.lookups.ChannelDest(s.Reader, *a.ChannelID)
				dests = append(dests, dest)
			}
			return err == nil
		})
		return err
	})
	if err != nil {
		return nil, err
	}
	return dests, nil
}

// search finds policies, in the order of their names without regard to case.
func (d *deltaStore) search(ctx context.Context, opts *renderData) ([]Policy, error) {
	omit := make(map[string]bool, len(opts.Omit))
	for _, id := range opts.Omit {
		omit[rowID(id)] = true
	}
	favUser := rowID(opts.FavoritesUserID)
	after := strings.ToLower(opts.After.Name)

	var policies []Policy
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		policies = nil
		d.Policies.Ascend(s.Reader, func(p PolicyRow) bool {
			if omit[p.ID] || opts.Search != "" && !matches(opts.Search, p) {
				return true
			}
			fav := d.isFavorite(s.Reader, favUser, p.ID)
			if opts.FavoritesOnly && !fav {
				return true
			}
			if after != "" && !opts.isAfter(strings.ToLower(p.Name) > after, fav) {
				return true
			}
			policies = append(policies, p.policy(fav))
			return true
		})
		slices.SortFunc(policies, func(a, b Policy) int {
			if opts.FavoritesFirst && a.isUserFavorite != b.isUserFavorite {
				if a.isUserFavorite {
					return -1
				}
				return 1
			}
			return cmp.Compare(strings.ToLower(a.Name), strings.ToLower(b.Name))
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return slices.Clip(policies[:min(len(policies), opts.Limit)]), nil
}

// isAfter reports whether a policy comes after the cursor, given whether its
// name does and whether it is a favorite.
func (opts *renderData) isAfter(name, fav bool) bool {
	switch {
	case !opts.FavoritesFirst:
		return name
	case opts.After.IsFavorite:
		return fav && name || !fav
	default:
		return !fav && name
	}
}

// matches reports whether a policy matches a search: its name or description
// contains it, or its name has words that start with the search's words, in
// the same order.
func matches(search string, p PolicyRow) bool {
	find := strings.ToLower(search)
	if strings.Contains(strings.ToLower(p.Description), find) {
		return true
	}
	name := strings.ToLower(p.Name)
	if strings.Contains(name, find) {
		return true
	}
	words := strings.FieldsFunc(name, func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) })
	terms := strings.FieldsFunc(find, func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) })
	if len(terms) == 0 {
		return len(words) > 0 // the pattern is then the start of any word
	}
	for _, word := range words {
		if strings.HasPrefix(word, terms[0]) {
			terms = terms[1:]
		}
		if len(terms) == 0 {
			return true
		}
	}
	return false
}

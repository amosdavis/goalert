package schedulemanager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/target/goalert/assignment"
	"github.com/target/goalert/deltastores"
	"github.com/target/goalert/engine/enginetables"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/notification"
	"github.com/target/goalert/override"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/schedule"
	"github.com/target/goalert/util"
	"github.com/target/goalert/util/log"
)

// DeltaDB manages who is on call for schedules on the Delta backend.
type DeltaDB struct {
	t *deltastores.Tables

	mu   sync.Mutex
	data map[string]parsedData // of schedules, so that it is not parsed every cycle
}

type parsedData struct {
	raw  []byte
	data schedule.Data
}

// NewDeltaDB returns a schedule manager on the Delta backend.
func NewDeltaDB(t *deltastores.Tables) *DeltaDB {
	return &DeltaDB{t: t, data: make(map[string]parsedData)}
}

// Name returns the name of the module.
func (db *DeltaDB) Name() string { return "Engine.ScheduleManager" }

// UpdateAll records who goes on call and off call, and notifies the channels
// that follow a schedule. A schedule that can't be updated is logged, and stays
// as it is.
func (db *DeltaDB) UpdateAll(ctx context.Context, tx *filestore.Tx) error {
	if err := permission.LimitCheckAny(ctx, permission.System); err != nil {
		return err
	}
	type sched struct{ id, timeZone string }
	var scheds []sched
	db.t.Schedule.EachSchedule(tx, func(id, timeZone string) bool {
		scheds = append(scheds, sched{id, timeZone})
		return true
	})
	err := enginetables.Each(tx, scheds, func(s sched) error {
		return errors.Wrapf(db.update(ctx, tx, s.id, s.timeZone), "update schedule %s", s.id)
	})
	if err != nil {
		log.Log(ctx, err)
	}
	return nil
}

func (db *DeltaDB) update(ctx context.Context, tx *filestore.Tx, id, timeZone string) error {
	now := tx.Now()
	info, err := db.info(ctx, tx, id, timeZone)
	if err != nil {
		return err
	}
	res, err := info.calcUpdates(now)
	if err != nil {
		return fmt.Errorf("calc updates: %w", err)
	}

	for _, userID := range sorted(res.UsersToStart) {
		if _, ok := db.t.User.Users.Get(tx, userID); !ok {
			continue // deleted, but still in a temporary schedule
		}
		if err := db.t.OnCall.StartScheduleOnCall(tx, id, userID); err != nil {
			return fmt.Errorf("record shift start for user %s: %w", userID, err)
		}
	}
	for _, userID := range sorted(res.UsersToStop) {
		if err := db.t.OnCall.EndScheduleOnCall(tx, id, userID); err != nil {
			return fmt.Errorf("record shift end for user %s: %w", userID, err)
		}
	}
	if res.NewRawScheduleData != nil {
		row, _ := db.t.Schedule.Data.Get(tx, id)
		row.Data = res.NewRawScheduleData
		if err := db.t.Schedule.Data.Put(tx, row); err != nil {
			return fmt.Errorf("set schedule data: %w", err)
		}
	}
	for _, chanID := range sorted(res.NotificationChannels) {
		// A channel of a rule that has since been deleted is not notified.
		if _, ok := db.t.NotificationChannel.Channels.Get(tx, chanID); !ok {
			log.Log(ctx, fmt.Errorf("schedule %s: notify channel %s: no such channel", id, chanID))
			continue
		}
		_, err := db.t.Notification.InsertMessage(tx, notification.MessageRow{
			MessageType: string(gadb.EnumOutgoingMessagesTypeScheduleOnCallNotification),
			ChannelID:   &chanID, ScheduleID: &id,
		})
		if err != nil {
			return fmt.Errorf("insert notification message for channel %s: %w", chanID, err)
		}
	}
	return nil
}

// sorted are ids in order, so that what follows does not depend on a map's.
func sorted(ids mapset.Set[uuid.UUID]) []string {
	out := make([]string, 0, ids.Cardinality())
	for id := range mapset.Elements(ids) {
		out = append(out, id.String())
	}
	slices.Sort(out)
	return out
}

// info reads what a schedule's on-call users are calculated from.
func (db *DeltaDB) info(ctx context.Context, tx *filestore.Tx, id, timeZone string) (*updateInfo, error) {
	now := tx.Now()
	info := &updateInfo{CurrentOnCall: mapset.NewThreadUnsafeSet[uuid.UUID]()}
	var err error
	if info.ScheduleID, err = uuid.Parse(id); err != nil {
		return nil, err
	}
	if info.TimeZone, err = util.LoadLocation(timeZone); err != nil {
		return nil, fmt.Errorf("load time zone %q: %w", timeZone, err)
	}
	for _, userID := range db.t.OnCall.ScheduleUsers(tx, id) {
		u, err := uuid.Parse(userID)
		if err != nil {
			return nil, err
		}
		info.CurrentOnCall.Add(u)
	}

	if row, ok := db.t.Schedule.Data.Get(tx, id); ok {
		info.RawScheduleData = row.Data
		data, err := db.parse(id, row.Data)
		if err != nil {
			log.Log(ctx, errors.Wrapf(err, "unmarshal data of schedule %s", id))
		}
		info.ScheduleData = data
	}
	if isTemp, users := info.ScheduleData.TempOnCall(now); isTemp {
		// They are in a JSON document, so they may have been deleted since.
		info.MissingUsers = mapset.NewThreadUnsafeSet[uuid.UUID]()
		for _, u := range users {
			if _, ok := db.t.User.Users.Get(tx, u.String()); !ok {
				info.MissingUsers.Add(u)
			}
		}
		return info, nil // nothing else counts
	}

	rules, err := db.t.Rule.ScheduleRules(tx, id)
	if err != nil {
		return nil, fmt.Errorf("get rules: %w", err)
	}
	for _, r := range rules {
		userID := r.Target.TargetID()
		if r.Target.TargetType() == assignment.TargetTypeRotation {
			var ok bool
			if userID, ok = db.t.Rotation.ActiveUserID(tx, userID); !ok {
				continue
			}
		}
		u, err := uuid.Parse(userID)
		if err != nil {
			return nil, err
		}
		info.Rules = append(info.Rules, ruleInfo{Rule: r, UserID: u})
	}

	var overrides []override.OverrideRow
	db.t.Override.BySchedule.Ascend(tx, id, func(o override.OverrideRow) bool {
		if !now.Before(o.Start) && !now.After(o.End) {
			overrides = append(overrides, o)
		}
		return true
	})
	slices.SortStableFunc(overrides, func(a, b override.OverrideRow) int { return a.Start.Compare(b.Start) })
	for _, o := range overrides {
		add, err := nullUUID(o.AddUserID)
		if err != nil {
			return nil, err
		}
		remove, err := nullUUID(o.RemoveUserID)
		if err != nil {
			return nil, err
		}
		info.ActiveOverrides = append(info.ActiveOverrides, overrideInfo{AddUserID: add, RemoveUserID: remove})
	}
	return info, nil
}

func nullUUID(s *string) (uuid.NullUUID, error) {
	if s == nil {
		return uuid.NullUUID{}, nil
	}
	id, err := uuid.Parse(*s)
	return uuid.NullUUID{UUID: id, Valid: err == nil}, err
}

// parse parses a schedule's data, unless it is the same as in the last cycle.
// The result is shared, and must not be changed.
func (db *DeltaDB) parse(id string, raw []byte) (schedule.Data, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if p, ok := db.data[id]; ok && bytes.Equal(p.raw, raw) {
		return p.data, nil
	}
	var data schedule.Data
	if err := json.Unmarshal(raw, &data); err != nil {
		delete(db.data, id)
		return schedule.Data{}, err
	}
	db.data[id] = parsedData{raw: raw, data: data}
	return data, nil
}

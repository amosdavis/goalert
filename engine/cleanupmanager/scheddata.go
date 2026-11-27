package cleanupmanager

import (
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/schedule"
)

// collectUsers will collect all user ids from the schedule data.
func collectUsers(data schedule.Data) (users []uuid.UUID) {
	for _, sched := range data.V1.TemporarySchedules {
		for _, shift := range sched.Shifts {
			id, err := uuid.Parse(shift.UserID)
			if err != nil {
				// invalid id, skip it
				continue
			}

			if slices.Contains(users, id) {
				continue
			}

			users = append(users, id)
		}
	}

	return users
}

// cleanupData will cleanup the schedule data, removing temporary-schedules that occur in the past; and removing shifts for users that no longer exist.
func cleanupData(data *schedule.Data, validUsers []uuid.UUID, now time.Time) (changed bool) {
	newTempSched := data.V1.TemporarySchedules[:0]
	for _, temp := range data.V1.TemporarySchedules {
		if temp.End.Before(now) {
			changed = true
			continue
		}

		cleanShifts := temp.Shifts[:0]
		for _, shift := range temp.Shifts {
			id, err := uuid.Parse(shift.UserID)
			if err != nil {
				changed = true
				// invalid user id/shift, delete it
				continue
			}
			if !slices.Contains(validUsers, id) {
				changed = true
				continue
			}
			cleanShifts = append(cleanShifts, shift)
		}
		temp.Shifts = cleanShifts
		newTempSched = append(newTempSched, temp)
	}
	data.V1.TemporarySchedules = newTempSched
	return changed
}

package schedulemanager

import (
	"time"

	"github.com/target/goalert/schedule"
	"github.com/target/goalert/util/timeutil"
)

// nextOnCallNotification is when a rule next notifies, or nil if it never does.
func nextOnCallNotification(nowInZone time.Time, rule schedule.OnCallNotificationRule) *time.Time {
	if rule.Time == nil {
		return nil
	}
	if rule.WeekdayFilter == nil || rule.WeekdayFilter.IsAlways() {
		newTime := rule.Time.FirstOfDay(nowInZone)
		if !newTime.After(nowInZone) {
			// add a day
			newTime = rule.Time.FirstOfDay(timeutil.AddDays(nowInZone, 1))
		}

		return &newTime
	}

	if rule.WeekdayFilter.IsNever() {
		return nil
	}

	var newTime time.Time
	if rule.WeekdayFilter.Day(nowInZone.Weekday()) {
		newTime = rule.Time.FirstOfDay(nowInZone)
	} else {
		newTime = rule.Time.FirstOfDay(rule.WeekdayFilter.NextActive(nowInZone))
	}
	if !newTime.After(nowInZone) {
		newTime = rule.Time.FirstOfDay(rule.WeekdayFilter.NextActive(newTime))
	}

	return &newTime
}

func equalTimePtr(a, b *time.Time) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == nil {
		return true
	}

	return a.Equal(*b)
}

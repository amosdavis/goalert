package gcsstore

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/target/goalert/objstore"
)

// The ends of the names the Delta log stores: commits, data files and
// checkpoints, and the checkpoint hint.
var objectEnds = []string{".json", ".parquet", "_last_checkpoint"}

// CheckSettings reports bucket settings that break or weaken the Delta log
// (F34). It needs permission to read the bucket's metadata
// (storage.buckets.get); without it, the error matches objstore.ErrDenied
// and the settings are unchecked. Settings only an IAM policy shows, such as
// public grants, are not checked.
func (s *Store) CheckSettings(ctx context.Context) ([]objstore.SettingProblem, error) {
	attrs, err := s.bucket.Attrs(ctx)
	if err != nil {
		return nil, opErr("read the settings of", "the bucket", err)
	}
	return checkAttrs(attrs, s.prefix), nil
}

func checkAttrs(a *storage.BucketAttrs, prefix string) []objstore.SettingProblem {
	var out []objstore.SettingProblem
	add := func(breaks bool, setting, format string, args ...any) {
		out = append(out, objstore.SettingProblem{Breaks: breaks, Setting: setting, Problem: fmt.Sprintf(format, args...)})
	}
	for _, r := range a.Lifecycle.Rules {
		if r.Action.Type == storage.DeleteAction && reachesLiveObjects(r.Condition, prefix) {
			add(true, "lifecycle rule", "a Delete rule can remove live commits or data files under %q, and a data file stays in use as long as its rows don't change (F6)", prefix)
		}
	}
	if a.RetentionPolicy != nil && a.RetentionPolicy.RetentionPeriod > 0 {
		add(true, "retention policy", "objects can't be replaced or deleted for %s, so _last_checkpoint can't move on and cleanup fails (F16, F36)", a.RetentionPolicy.RetentionPeriod)
	}
	if a.DefaultEventBasedHold {
		add(true, "default event-based hold", "new objects are held, so they can't be replaced or deleted (F16, F36)")
	}
	if a.VersioningEnabled {
		add(false, "object versioning", "deleted and replaced objects are kept as noncurrent versions, which costs storage; add a lifecycle rule that deletes noncurrent versions (F37)")
	}
	if a.SoftDeletePolicy != nil && a.SoftDeletePolicy.RetentionDuration > 0 {
		add(false, "soft delete", "deleted objects are kept for %s, which costs storage (F37)", a.SoftDeletePolicy.RetentionDuration)
	}
	if a.PublicAccessPrevention != storage.PublicAccessPreventionEnforced {
		add(false, "public access prevention", "not enforced, so the bucket's alert data could be made public (F20)")
	}
	return out
}

// reachesLiveObjects reports whether a lifecycle condition can match a live
// object the Delta log stores under prefix.
func reachesLiveObjects(c storage.LifecycleCondition, prefix string) bool {
	if c.Liveness == storage.Archived || c.NumNewerVersions > 0 || c.DaysSinceNoncurrentTime > 0 || !c.NoncurrentTimeBefore.IsZero() {
		return false // noncurrent versions only
	}
	if len(c.MatchesPrefix) > 0 && !overlaps(c.MatchesPrefix, func(p string) bool {
		return strings.HasPrefix(prefix, p) || strings.HasPrefix(p, prefix)
	}) {
		return false
	}
	if len(c.MatchesSuffix) > 0 && !overlaps(c.MatchesSuffix, func(s string) bool {
		for _, end := range objectEnds {
			if strings.HasSuffix(end, s) || strings.HasSuffix(s, end) {
				return true
			}
		}
		return false
	}) {
		return false
	}
	return true
}

func overlaps(values []string, match func(string) bool) bool {
	for _, v := range values {
		if match(v) {
			return true
		}
	}
	return false
}

package gcsstore

import (
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/target/goalert/objstore"
)

func breaking(problems []objstore.SettingProblem) []string {
	var out []string
	for _, p := range problems {
		if p.Breaks {
			out = append(out, p.Setting)
		}
	}
	return out
}

func TestCheckAttrs(t *testing.T) {
	clean := func() *storage.BucketAttrs {
		return &storage.BucketAttrs{PublicAccessPrevention: storage.PublicAccessPreventionEnforced}
	}
	assert.Empty(t, checkAttrs(clean(), "goalert/"), "a bucket set up as documented has no problems")

	withDelete := func(c storage.LifecycleCondition) *storage.BucketAttrs {
		a := clean()
		a.Lifecycle.Rules = []storage.LifecycleRule{{Action: storage.LifecycleAction{Type: storage.DeleteAction}, Condition: c}}
		return a
	}
	for name, c := range map[string]storage.LifecycleCondition{
		"every object after a year": {AgeInDays: 365},
		"the table's prefix":        {AgeInDays: 30, MatchesPrefix: []string{"goalert/"}},
		"a shorter prefix":          {AgeInDays: 30, MatchesPrefix: []string{"go"}},
		"a longer prefix":           {AgeInDays: 30, MatchesPrefix: []string{"goalert/_delta_log/"}},
		"commit files":              {AgeInDays: 30, MatchesSuffix: []string{".json"}},
		"some data files":           {AgeInDays: 30, MatchesSuffix: []string{"y.parquet"}},
	} {
		assert.Equal(t, []string{"lifecycle rule"}, breaking(checkAttrs(withDelete(c), "goalert/")), name)
	}
	for name, c := range map[string]storage.LifecycleCondition{
		"noncurrent versions":     {NumNewerVersions: 3},
		"archived versions":       {AgeInDays: 30, Liveness: storage.Archived},
		"days as noncurrent":      {DaysSinceNoncurrentTime: 7},
		"another prefix":          {AgeInDays: 30, MatchesPrefix: []string{"logs/"}},
		"another kind of object":  {AgeInDays: 30, MatchesSuffix: []string{".log"}},
		"a prefix inside another": {AgeInDays: 30, MatchesPrefix: []string{"goalertx/"}},
	} {
		assert.Empty(t, breaking(checkAttrs(withDelete(c), "goalert/")), name)
	}

	a := clean()
	a.RetentionPolicy = &storage.RetentionPolicy{RetentionPeriod: 24 * time.Hour}
	assert.Equal(t, []string{"retention policy"}, breaking(checkAttrs(a, "goalert/")))
	a = clean()
	a.DefaultEventBasedHold = true
	assert.Equal(t, []string{"default event-based hold"}, breaking(checkAttrs(a, "goalert/")))

	a = &storage.BucketAttrs{VersioningEnabled: true, SoftDeletePolicy: &storage.SoftDeletePolicy{RetentionDuration: 7 * 24 * time.Hour}}
	problems := checkAttrs(a, "goalert/")
	assert.Empty(t, breaking(problems), "costs and exposure risks are warnings")
	assert.Len(t, problems, 3)
}

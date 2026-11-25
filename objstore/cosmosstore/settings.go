package cosmosstore

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/target/goalert/objstore"
)

// ContainerProperties describes a container for a Store, to create it with.
// The contents of objects are not indexed, which would only cost request units.
func ContainerProperties(id string) azcosmos.ContainerProperties {
	return azcosmos.ContainerProperties{
		ID:                     id,
		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{Paths: []string{"/pk"}, Version: 2},
		IndexingPolicy: &azcosmos.IndexingPolicy{
			Automatic:     true,
			IndexingMode:  azcosmos.IndexingModeConsistent,
			IncludedPaths: []azcosmos.IncludedPath{{Path: "/name/?"}},
			ExcludedPaths: []azcosmos.ExcludedPath{{Path: "/*"}},
		},
	}
}

// CheckSettings reports account and container settings that break or weaken
// the store (F34, F44).
func (s *Store) CheckSettings(ctx context.Context) ([]objstore.SettingProblem, error) {
	resp, err := s.container.Read(ctx, nil)
	if err != nil {
		return nil, opErr("read the container's settings", err)
	}
	if resp.ContainerProperties == nil {
		return nil, fmt.Errorf("cosmosstore: the container's settings are missing")
	}
	problems := checkContainer(*resp.ContainerProperties)

	// Only the account's default consistency can be asked for, or a weaker
	// one, so an account with a weaker default refuses this query.
	err = s.query(ctx, s.manifestPK(), "SELECT TOP 1 VALUE c.id FROM c", nil,
		func([]byte, time.Time) error { return nil })
	switch {
	case statusCode(err) == http.StatusBadRequest:
		problems = append(problems, objstore.SettingProblem{
			Breaks: true, Setting: "consistency level",
			Problem: "the account refuses Strong consistency, so a pod could read an old version as the latest (F44); make Strong the account's default: " + err.Error(),
		})
	case err != nil:
		return nil, opErr("check the consistency level", err)
	}
	return problems, nil
}

func checkContainer(p azcosmos.ContainerProperties) []objstore.SettingProblem {
	var out []objstore.SettingProblem
	add := func(breaks bool, setting, format string, args ...any) {
		out = append(out, objstore.SettingProblem{Breaks: breaks, Setting: setting, Problem: fmt.Sprintf(format, args...)})
	}
	if paths := p.PartitionKeyDefinition.Paths; len(paths) != 1 || paths[0] != "/pk" {
		add(true, "partition key", "it is %v, not /pk, so no item can be found", paths)
	}
	if ttl := p.DefaultTimeToLive; ttl != nil && *ttl > 0 {
		add(true, "time to live", "items expire after %d seconds, which deletes live commits and data (F6)", *ttl)
	}
	if u := p.UniqueKeyPolicy; u != nil && len(u.UniqueKeys) > 0 {
		add(true, "unique keys", "they refuse items the store needs to write")
	}
	ix := p.IndexingPolicy
	switch {
	case ix == nil:
		// The response leaves it out only if it is unknown; the default policy
		// indexes everything.
		add(false, "indexing policy", "it is unknown; listing needs /name/? indexed")
	case !strings.EqualFold(string(ix.IndexingMode), string(azcosmos.IndexingModeConsistent)) || !ix.Automatic:
		add(true, "indexing policy", "items are not indexed as they are written, so listings can miss commits (F16)")
	case !indexed(ix, "name"):
		add(true, "indexing policy", "/name/? is not indexed, which listing needs")
	default:
		if indexed(ix, "data") || indexed(ix, "inline") {
			add(false, "indexing policy", "/data/? and /inline/? are indexed, which costs request units and is never used (F37)")
		}
	}
	return out
}

// indexed reports whether a policy indexes a top-level scalar property. The
// most specific path decides: the property's own before the root's.
func indexed(ix *azcosmos.IndexingPolicy, property string) bool {
	own := []string{"/" + property + "/?", `/"` + property + `"/?`, "/" + property + "/*", `/"` + property + `"/*`}
	for _, path := range own {
		for _, p := range ix.ExcludedPaths {
			if p.Path == path {
				return false
			}
		}
	}
	for _, path := range own {
		for _, p := range ix.IncludedPaths {
			if p.Path == path {
				return true
			}
		}
	}
	for _, p := range ix.IncludedPaths {
		if p.Path == "/*" {
			return true
		}
	}
	return false
}

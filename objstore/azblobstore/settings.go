package azblobstore

import (
	"context"
	"fmt"

	"github.com/target/goalert/objstore"
)

// CheckSettings reports container settings that break or weaken the Delta
// log (F34). Account settings, such as blob soft delete, versioning and
// lifecycle management policies, can't be read with container credentials
// and are not checked; docs/filestore-plan.md lists the required ones. An
// error matching objstore.ErrDenied means the container's properties can't
// be read.
func (s *Store) CheckSettings(ctx context.Context) ([]objstore.SettingProblem, error) {
	props, err := s.c.GetProperties(ctx, nil)
	if err != nil {
		return nil, opErr("read the settings of", "the container", err)
	}
	var out []objstore.SettingProblem
	add := func(breaks bool, setting, format string, args ...any) {
		out = append(out, objstore.SettingProblem{Breaks: breaks, Setting: setting, Problem: fmt.Sprintf(format, args...)})
	}
	if props.BlobPublicAccess != nil && *props.BlobPublicAccess != "" {
		add(true, "public access", "the container allows anonymous reads (%s), exposing alert data (F20)", *props.BlobPublicAccess)
	}
	if props.HasImmutabilityPolicy != nil && *props.HasImmutabilityPolicy {
		add(true, "immutability policy", "blobs can't be replaced or deleted, so _last_checkpoint can't move on and cleanup fails (F16, F36)")
	}
	if props.HasLegalHold != nil && *props.HasLegalHold {
		add(true, "legal hold", "blobs can't be replaced or deleted, so _last_checkpoint can't move on and cleanup fails (F16, F36)")
	}
	if props.IsImmutableStorageWithVersioningEnabled != nil && *props.IsImmutableStorageWithVersioningEnabled {
		add(false, "version-level immutability", "enabled; a default policy on it would block replacing and deleting blobs (F16, F36)")
	}
	return out, nil
}

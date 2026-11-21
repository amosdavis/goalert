package deltalog

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/target/goalert/objstore"
)

// Cleanup deletes the commits and checkpoints no reader needs any more
// (F16): those before the newest usable checkpoint older than retention. The
// log from that checkpoint on is kept, so it can always be loaded, and
// nothing newer than retention is deleted. Files are deleted oldest first, so
// a cleanup cut short leaves an unbroken log. When anything is unclear,
// nothing is deleted. It returns how many files it deleted.
//
// A pod that falls behind the deleted part of the log must reload from a
// checkpoint rather than read on from its version (F16); the store does
// that.
func (l *Log) Cleanup(ctx context.Context, now time.Time, retention time.Duration) (int, error) {
	names, err := l.store.List(ctx, logDir, "")
	if err != nil {
		return 0, err
	}
	type file struct {
		name    string
		version int64
	}
	var files []file
	var checkpoints []int64
	for _, n := range names {
		if v, ok := parseCheckpointName(n); ok {
			checkpoints = append(checkpoints, v)
			files = append(files, file{n, v})
		} else if v, ok := parseCommitName(n); ok {
			files = append(files, file{n, v})
		}
	}
	sort.Slice(checkpoints, func(i, j int) bool { return checkpoints[i] > checkpoints[j] })

	cutoff := now.Add(-retention)
	keepFrom := int64(-1)
	for _, v := range checkpoints {
		t, ok, err := l.versionTime(ctx, v)
		if err != nil {
			return 0, err
		}
		if !ok || !t.Before(cutoff) {
			continue
		}
		// Everything before the boundary goes, so it must be a checkpoint
		// readers can load.
		if _, err := l.readCheckpoint(ctx, v); skippable(err) {
			continue
		} else if err != nil {
			return 0, err
		}
		keepFrom = v
		break
	}
	if keepFrom <= 0 {
		return 0, nil
	}

	sort.Slice(files, func(i, j int) bool { return files[i].version < files[j].version })
	deleted := 0
	for _, f := range files {
		if f.version >= keepFrom {
			break
		}
		if err := l.store.Delete(ctx, f.name); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

// versionTime returns a checkpointed version's commit time: from its small
// commit file while that exists, otherwise from the checkpoint's footer. It
// reports false if neither can be used.
func (l *Log) versionTime(ctx context.Context, version int64) (time.Time, bool, error) {
	info, _, err := l.Read(ctx, version)
	if err == nil {
		return time.UnixMicro(info.GoAlertTime).UTC(), true, nil
	}
	if !errors.Is(err, objstore.ErrNotFound) {
		return time.Time{}, false, err
	}
	s, err := l.readCheckpoint(ctx, version)
	if skippable(err) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	return s.Time, true, nil
}

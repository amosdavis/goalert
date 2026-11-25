package deltalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/target/goalert/objstore"
)

// A checkpoint is a Parquet file holding a snapshot's actions, in the
// classic single-file format every Delta reader supports (F35). A pod starts
// from the newest one instead of replaying the whole log (F16, F38).

const lastCheckpointName = logDir + "_last_checkpoint"

// DefaultTombstoneRetention is how long a checkpoint keeps the tombstone of
// a removed file, as Delta's delta.deletedFileRetentionDuration default.
const DefaultTombstoneRetention = 7 * 24 * time.Hour

// Checkpoint footer keys. A checkpoint without them was not written by
// GoAlert, so a pod does not trust it (F11).
const (
	keyStoreID  = "goalert.storeId"
	keyCommitID = "goalert.commitId"
	keyFormat   = "goalert.format"
	keyVersion  = "goalert.version"
	keyTime     = "goalert.time"
)

// CheckpointName is the object name of a version's checkpoint.
func CheckpointName(version int64) string {
	return fmt.Sprintf("%s%020d.checkpoint.parquet", logDir, version)
}

func parseCheckpointName(name string) (int64, bool) {
	base, ok := strings.CutPrefix(name, logDir)
	if !ok || len(base) != 39 || !strings.HasSuffix(base, ".checkpoint.parquet") {
		return 0, false
	}
	v, err := strconv.ParseInt(base[:20], 10, 64)
	return v, err == nil && v >= 0
}

type lastCheckpoint struct {
	Version     int64 `json:"version"`
	Size        int64 `json:"size"`
	SizeInBytes int64 `json:"sizeInBytes,omitempty"`
}

// The Delta checkpoint schema (PROTOCOL.md, "Checkpoint Schema"), for the
// actions reader version 1 and writer version 2 have.
type checkpointRow struct {
	Txn      *cpTxn      `parquet:"txn,optional"`
	Add      *cpAdd      `parquet:"add,optional"`
	Remove   *cpRemove   `parquet:"remove,optional"`
	MetaData *cpMetaData `parquet:"metaData,optional"`
	Protocol *cpProtocol `parquet:"protocol,optional"`
}

type cpTxn struct {
	AppID       string `parquet:"appId"`
	Version     int64  `parquet:"version"`
	LastUpdated *int64 `parquet:"lastUpdated,optional"`
}

type cpAdd struct {
	Path             string            `parquet:"path"`
	PartitionValues  map[string]string `parquet:"partitionValues"`
	Size             int64             `parquet:"size"`
	ModificationTime int64             `parquet:"modificationTime"`
	DataChange       bool              `parquet:"dataChange"`
	Stats            *string           `parquet:"stats,optional"`
}

type cpRemove struct {
	Path                 string            `parquet:"path"`
	DeletionTimestamp    *int64            `parquet:"deletionTimestamp,optional"`
	DataChange           bool              `parquet:"dataChange"`
	ExtendedFileMetadata *bool             `parquet:"extendedFileMetadata,optional"`
	PartitionValues      map[string]string `parquet:"partitionValues,optional"`
	Size                 *int64            `parquet:"size,optional"`
}

type cpFormat struct {
	Provider string            `parquet:"provider"`
	Options  map[string]string `parquet:"options"`
}

type cpMetaData struct {
	ID               string            `parquet:"id"`
	Name             *string           `parquet:"name,optional"`
	Description      *string           `parquet:"description,optional"`
	Format           cpFormat          `parquet:"format"`
	SchemaString     string            `parquet:"schemaString"`
	PartitionColumns []string          `parquet:"partitionColumns,list"`
	Configuration    map[string]string `parquet:"configuration"`
	CreatedTime      *int64            `parquet:"createdTime,optional"`
}

type cpProtocol struct {
	MinReaderVersion int32 `parquet:"minReaderVersion"`
	MinWriterVersion int32 `parquet:"minWriterVersion"`
}

func ptrIf[T comparable](v T) *T {
	var zero T
	if v == zero {
		return nil
	}
	return &v
}

func deref[T any](p *T) T {
	var v T
	if p != nil {
		v = *p
	}
	return v
}

func nonNilMap(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

// checkpointRows returns a snapshot's actions in a stable order. Tombstones
// older than retention are dropped; those without a deletion time are kept,
// since dropping one early could let VACUUM delete a file still in use (F6).
func checkpointRows(s *Snapshot, now time.Time, retention time.Duration) []checkpointRow {
	rows := []checkpointRow{
		{Protocol: &cpProtocol{MinReaderVersion: int32(s.Protocol.MinReaderVersion), MinWriterVersion: int32(s.Protocol.MinWriterVersion)}},
		{MetaData: &cpMetaData{
			ID:               s.MetaData.ID,
			Name:             ptrIf(s.MetaData.Name),
			Description:      ptrIf(s.MetaData.Description),
			Format:           cpFormat{Provider: s.MetaData.Format.Provider, Options: nonNilMap(s.MetaData.Format.Options)},
			SchemaString:     s.MetaData.SchemaString,
			PartitionColumns: s.MetaData.PartitionColumns,
			Configuration:    nonNilMap(s.MetaData.Configuration),
			CreatedTime:      ptrIf(s.MetaData.CreatedTime),
		}},
	}
	for _, id := range sortedKeys(s.Txns) {
		rows = append(rows, checkpointRow{Txn: &cpTxn{AppID: id, Version: s.Txns[id]}})
	}
	for _, p := range sortedKeys(s.Files) {
		a := s.Files[p]
		rows = append(rows, checkpointRow{Add: &cpAdd{
			Path: a.Path, PartitionValues: nonNilMap(a.PartitionValues), Size: a.Size,
			ModificationTime: a.ModificationTime, DataChange: false, Stats: ptrIf(a.Stats),
		}})
	}
	expired := now.Add(-retention).UnixMilli()
	for _, p := range sortedKeys(s.Removed) {
		r := s.Removed[p]
		if r.DeletionTimestamp != 0 && r.DeletionTimestamp < expired {
			continue
		}
		rows = append(rows, checkpointRow{Remove: &cpRemove{
			Path: r.Path, DeletionTimestamp: ptrIf(r.DeletionTimestamp), DataChange: false,
			ExtendedFileMetadata: ptrIf(r.ExtendedFileMetadata), PartitionValues: r.PartitionValues, Size: ptrIf(r.Size),
		}})
	}
	return rows
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// checkpointFooter labels a checkpoint with the table, commit and format it
// belongs to.
func checkpointFooter(s *Snapshot) map[string]string {
	return map[string]string{
		keyStoreID:  s.MetaData.ID,
		keyCommitID: s.CommitID,
		keyFormat:   strconv.Itoa(GoAlertFormat),
		keyVersion:  strconv.FormatInt(s.Version, 10),
		keyTime:     strconv.FormatInt(s.Time.UnixMicro(), 10),
	}
}

func encodeCheckpoint(rows []checkpointRow, footer map[string]string) ([]byte, error) {
	opts := []parquet.WriterOption{parquet.Compression(&parquet.Snappy)}
	for _, k := range sortedKeys(footer) {
		opts = append(opts, parquet.KeyValueMetadata(k, footer[k]))
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[checkpointRow](&buf, opts...)
	if _, err := w.Write(rows); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// WriteCheckpoint writes a checkpoint of s and points _last_checkpoint at
// it. s must not change while it runs. A checkpoint only saves work, so an
// error leaves the table correct: the caller logs it and tries again later.
func (l *Log) WriteCheckpoint(ctx context.Context, s *Snapshot, now time.Time, retention time.Duration) error {
	if s.CommitID == "" {
		return fmt.Errorf("deltalog: checkpoint %d: snapshot has no commit ID", s.Version)
	}
	rows := checkpointRows(s, now, retention)
	data, err := encodeCheckpoint(rows, checkpointFooter(s))
	if err != nil {
		return fmt.Errorf("deltalog: checkpoint %d: %w", s.Version, err)
	}

	// Two pods may checkpoint the same version; the first one's file stays.
	err = l.store.PutIfAbsent(ctx, CheckpointName(s.Version), data)
	if err != nil && !errors.Is(err, objstore.ErrExists) {
		return fmt.Errorf("deltalog: checkpoint %d: %w", s.Version, err)
	}

	// _last_checkpoint is the only object GoAlert ever rewrites, once per
	// checkpoint at most (F36). It is never moved back to an older version.
	if v, err := l.readLastCheckpoint(ctx); err == nil && v >= s.Version {
		return nil
	}
	hint, err := json.Marshal(lastCheckpoint{Version: s.Version, Size: int64(len(rows)), SizeInBytes: int64(len(data))})
	if err != nil {
		return err
	}
	if err := l.store.Put(ctx, lastCheckpointName, hint); err != nil {
		return fmt.Errorf("deltalog: update %s: %w", lastCheckpointName, err)
	}
	return nil
}

func (l *Log) readLastCheckpoint(ctx context.Context) (int64, error) {
	data, err := l.store.Get(ctx, lastCheckpointName)
	if err != nil {
		return 0, err
	}
	var lc lastCheckpoint
	if err := json.Unmarshal(data, &lc); err != nil {
		return 0, fmt.Errorf("%w: %s: %v", ErrCorrupt, lastCheckpointName, err)
	}
	if lc.Version < 0 {
		return 0, fmt.Errorf("%w: %s: version %d", ErrCorrupt, lastCheckpointName, lc.Version)
	}
	return lc.Version, nil
}

// skippable reports whether Load should work around a checkpoint error.
// Anything else, such as a storage outage, stops Load, so a passing problem
// is never reported as a damaged log (F21).
func skippable(err error) bool {
	return errors.Is(err, ErrCorrupt) || errors.Is(err, ErrForeign) ||
		errors.Is(err, ErrUnsupported) || errors.Is(err, objstore.ErrNotFound)
}

// readCheckpoint reads a checkpoint GoAlert wrote and checks it against the
// commit it summarizes, if that commit is still in the log.
func (l *Log) readCheckpoint(ctx context.Context, version int64) (*Snapshot, error) {
	data, err := l.store.Get(ctx, CheckpointName(version))
	if err != nil {
		return nil, fmt.Errorf("deltalog: checkpoint %d: %w", version, err)
	}
	s, err := decodeCheckpoint(version, data)
	if err != nil {
		return nil, fmt.Errorf("deltalog: checkpoint %d: %w", version, err)
	}
	info, _, err := l.Read(ctx, version)
	switch {
	case errors.Is(err, objstore.ErrNotFound):
		// Cleaned up after the retention window; the footer is all there is.
	case err != nil:
		return nil, fmt.Errorf("deltalog: checkpoint %d: %w", version, err)
	case info.GoAlertCommitID != s.CommitID:
		return nil, fmt.Errorf("%w: checkpoint %d summarizes commit %q, but the log has %q", ErrForeign, version, s.CommitID, info.GoAlertCommitID)
	}
	return s, nil
}

func decodeCheckpoint(version int64, data []byte) (*Snapshot, error) {
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	storeID, _ := f.Lookup(keyStoreID)
	commitID, _ := f.Lookup(keyCommitID)
	formatStr, _ := f.Lookup(keyFormat)
	versionStr, _ := f.Lookup(keyVersion)
	if storeID == "" || commitID == "" || formatStr == "" {
		return nil, fmt.Errorf("%w: not written by GoAlert", ErrForeign)
	}
	format, err := strconv.Atoi(formatStr)
	if err != nil || format < 1 {
		return nil, fmt.Errorf("%w: GoAlert format %q", ErrCorrupt, formatStr)
	}
	if format > GoAlertFormat {
		return nil, fmt.Errorf("%w: GoAlert format %d (this version reads %d)", ErrUnsupported, format, GoAlertFormat)
	}
	if versionStr != strconv.FormatInt(version, 10) {
		return nil, fmt.Errorf("%w: footer says version %q", ErrCorrupt, versionStr)
	}
	timeStr, _ := f.Lookup(keyTime)
	micros, err := strconv.ParseInt(timeStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: footer time %q", ErrCorrupt, timeStr)
	}

	rows := make([]checkpointRow, f.NumRows())
	r := parquet.NewGenericReader[checkpointRow](bytes.NewReader(data))
	defer func() { _ = r.Close() }()
	for n := 0; n < len(rows); {
		k, err := r.Read(rows[n:])
		n += k
		if errors.Is(err, io.EOF) {
			if n < len(rows) {
				return nil, fmt.Errorf("%w: %d of %d rows", ErrCorrupt, n, len(rows))
			}
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
	}

	s := newSnapshot()
	s.Version, s.CommitID, s.Time = version, commitID, time.UnixMicro(micros).UTC()
	sawProtocol, sawMeta := false, false
	for i, row := range rows {
		n := 0
		for _, set := range []bool{row.Protocol != nil, row.MetaData != nil, row.Add != nil, row.Remove != nil, row.Txn != nil} {
			if set {
				n++
			}
		}
		if n != 1 {
			return nil, fmt.Errorf("%w: row %d holds %d actions", ErrCorrupt, i, n)
		}
		switch {
		case row.Protocol != nil:
			if sawProtocol {
				return nil, fmt.Errorf("%w: two protocol rows", ErrCorrupt)
			}
			s.Protocol = Protocol{MinReaderVersion: int(row.Protocol.MinReaderVersion), MinWriterVersion: int(row.Protocol.MinWriterVersion)}
			if err := checkProtocol(s.Protocol); err != nil {
				return nil, err
			}
			sawProtocol = true
		case row.MetaData != nil:
			if sawMeta {
				return nil, fmt.Errorf("%w: two metaData rows", ErrCorrupt)
			}
			m := row.MetaData
			s.MetaData = MetaData{
				ID: m.ID, Name: deref(m.Name), Description: deref(m.Description),
				Format:           Format{Provider: m.Format.Provider, Options: nonNilMap(m.Format.Options)},
				SchemaString:     m.SchemaString,
				PartitionColumns: append([]string{}, m.PartitionColumns...),
				Configuration:    nonNilMap(m.Configuration),
				CreatedTime:      deref(m.CreatedTime),
			}
			if err := checkMetaData(s.MetaData); err != nil {
				return nil, err
			}
			sawMeta = true
		case row.Add != nil:
			a := row.Add
			if _, dup := s.Files[a.Path]; dup || a.Path == "" {
				return nil, fmt.Errorf("%w: add of %q", ErrCorrupt, a.Path)
			}
			s.Files[a.Path] = snapshotAdd(Add{
				Path: a.Path, PartitionValues: a.PartitionValues, Size: a.Size,
				ModificationTime: a.ModificationTime, Stats: deref(a.Stats),
			})
		case row.Remove != nil:
			r := row.Remove
			if _, dup := s.Removed[r.Path]; dup || r.Path == "" {
				return nil, fmt.Errorf("%w: remove of %q", ErrCorrupt, r.Path)
			}
			s.Removed[r.Path] = snapshotRemove(Remove{
				Path: r.Path, DeletionTimestamp: deref(r.DeletionTimestamp),
				ExtendedFileMetadata: deref(r.ExtendedFileMetadata), PartitionValues: r.PartitionValues, Size: deref(r.Size),
			})
		case row.Txn != nil:
			s.Txns[row.Txn.AppID] = row.Txn.Version
		}
	}
	if !sawProtocol || !sawMeta {
		return nil, fmt.Errorf("%w: no protocol or metadata", ErrCorrupt)
	}
	if s.MetaData.ID != storeID {
		return nil, fmt.Errorf("%w: checkpoint of table %q is labeled %q", ErrForeign, s.MetaData.ID, storeID)
	}
	return s, nil
}

// Load returns the latest snapshot: the newest usable checkpoint plus the
// commits after it, or a replay of the whole log. A checkpoint GoAlert did
// not write, or cannot read, is skipped and reported through Warn (F11,
// F35).
func (l *Log) Load(ctx context.Context) (*Snapshot, error) {
	finish := func(s *Snapshot) (*Snapshot, error) {
		if err := l.catchUp(ctx, s); err != nil {
			return nil, err
		}
		if m := s.MetaData.Configuration[ManagerKey]; m != l.Manager {
			return nil, fmt.Errorf("%w: by %q, not %q (F43)", ErrManaged, m, l.Manager)
		}
		return s, nil
	}
	var skipped []error
	skip := func(err error) {
		skipped = append(skipped, err)
		l.warn(fmt.Errorf("deltalog: skipped: %w", err))
	}

	// Usually _last_checkpoint names a good checkpoint, and only the commits
	// after it are listed and read.
	tried := map[int64]bool{}
	v, err := l.readLastCheckpoint(ctx)
	switch {
	case err == nil:
		tried[v] = true
		s, err := l.readCheckpoint(ctx, v)
		if err == nil {
			return finish(s)
		}
		if !skippable(err) {
			return nil, err
		}
		skip(err)
	case errors.Is(err, objstore.ErrNotFound):
	case skippable(err):
		skip(err)
	default:
		return nil, err
	}

	// Otherwise look through the whole log.
	names, err := l.store.List(ctx, logDir, "")
	if err != nil {
		return nil, err
	}
	var checkpoints []int64
	commits, hasZero := 0, false
	for _, n := range names {
		if v, ok := parseCheckpointName(n); ok && !tried[v] {
			checkpoints = append(checkpoints, v)
		}
		if v, ok := parseCommitName(n); ok {
			commits++
			hasZero = hasZero || v == 0
		}
	}
	sort.Slice(checkpoints, func(i, j int) bool { return checkpoints[i] > checkpoints[j] })
	for _, v := range checkpoints {
		s, err := l.readCheckpoint(ctx, v)
		if err == nil {
			return finish(s)
		}
		if !skippable(err) {
			return nil, err
		}
		skip(err)
	}
	if hasZero {
		return finish(newSnapshot())
	}
	if commits == 0 && len(checkpoints) == 0 && len(skipped) == 0 {
		return nil, ErrNoTable
	}
	return nil, fmt.Errorf("%w: no usable checkpoint, and the log no longer starts at version 0: %w", ErrCorrupt, errors.Join(skipped...))
}

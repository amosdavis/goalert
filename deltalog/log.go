package deltalog

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/objstore"
)

const (
	logDir = "_delta_log/"

	// FormatKey is the table configuration key that holds GoAlert's data
	// format version.
	FormatKey = "goalert.format"

	// GoAlertFormat is the GoAlert data format this code reads and writes.
	GoAlertFormat = 1
)

var (
	// ErrNoTable means the location holds no table. A table is only ever
	// created by an explicit Create, never on its own (F27).
	ErrNoTable = errors.New("deltalog: no Delta table at this location")

	// ErrTableExists means Create found a table already there.
	ErrTableExists = errors.New("deltalog: a Delta table already exists at this location")

	// ErrConflict means another writer committed the version first. The
	// caller catches up and tries its transaction again (F4).
	ErrConflict = errors.New("deltalog: another writer committed this version first")

	// ErrUnsupported means the table needs a protocol feature or GoAlert
	// format this code does not have (F19, F30).
	ErrUnsupported = errors.New("deltalog: unsupported table protocol or format")

	// ErrForeign means a commit was not written by GoAlert for this table
	// (F11).
	ErrForeign = errors.New("deltalog: commit was not written by GoAlert for this table")

	// ErrCorrupt means the log is inconsistent.
	ErrCorrupt = errors.New("deltalog: log is inconsistent")
)

// CommitName is the object name of a version's commit file.
func CommitName(version int64) string { return fmt.Sprintf("%s%020d.json", logDir, version) }

// parseCommitName returns the version of a commit file name.
func parseCommitName(name string) (int64, bool) {
	base, ok := strings.CutPrefix(name, logDir)
	if !ok || len(base) != 25 || !strings.HasSuffix(base, ".json") {
		return 0, false
	}
	v, err := strconv.ParseInt(base[:20], 10, 64)
	return v, err == nil && v >= 0
}

// Log is a Delta log in a store, written by one GoAlert process.
type Log struct {
	store    objstore.Store
	writerID string

	// MaxAttempts bounds how often a commit is retried after an unclear
	// result before Commit gives up and returns objstore.ErrUnknown.
	MaxAttempts int
	// Backoff is the first retry delay. It doubles each attempt, with
	// jitter.
	Backoff time.Duration

	// Warn, if set, receives the problems Load works around, such as a
	// checkpoint it skipped, so they are not hidden (F21).
	Warn func(error)
}

func (l *Log) warn(err error) {
	if l.Warn != nil {
		l.Warn(err)
	}
}

// New returns a Log in store. writerID names this process in every commit it
// writes, for diagnosing which pod wrote what.
func New(store objstore.Store, writerID string) *Log {
	return &Log{store: store, writerID: writerID, MaxAttempts: 8, Backoff: 100 * time.Millisecond}
}

// encode writes a commit file: commitInfo first, as Spark does, then the
// actions, one JSON object per line.
func encode(info CommitInfo, actions []Action) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	if err := enc.Encode(Action{CommitInfo: &info}); err != nil {
		return nil, err
	}
	for _, a := range actions {
		if a.CommitInfo != nil {
			return nil, fmt.Errorf("deltalog: commitInfo is added by Commit")
		}
		if err := enc.Encode(a); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// decode parses a commit file into its commitInfo and other actions.
func decode(data []byte) (CommitInfo, []Action, error) {
	var info CommitInfo
	var actions []Action
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 64<<10), 64<<20)
	seenInfo := false
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var a Action
		if err := json.Unmarshal(line, &a); err != nil {
			return info, nil, fmt.Errorf("%w: bad action %q: %v", ErrCorrupt, line, err)
		}
		if n := countSet(a); n != 1 {
			return info, nil, fmt.Errorf("%w: an action must set exactly one field, %q sets %d", ErrCorrupt, line, n)
		}
		if a.CommitInfo != nil {
			if seenInfo {
				return info, nil, fmt.Errorf("%w: two commitInfo actions", ErrCorrupt)
			}
			info, seenInfo = *a.CommitInfo, true
			continue
		}
		actions = append(actions, a)
	}
	return info, actions, sc.Err()
}

func countSet(a Action) int {
	n := 0
	for _, set := range []bool{a.Protocol != nil, a.MetaData != nil, a.Add != nil, a.Remove != nil, a.Txn != nil, a.CommitInfo != nil} {
		if set {
			n++
		}
	}
	return n
}

// Read returns a version's commitInfo and actions. The error wraps
// objstore.ErrNotFound if the version does not exist yet.
func (l *Log) Read(ctx context.Context, version int64) (CommitInfo, []Action, error) {
	data, err := l.store.Get(ctx, CommitName(version))
	if err != nil {
		return CommitInfo{}, nil, err
	}
	return decode(data)
}

// Create creates the table at version 0 with the given metadata. It returns
// ErrTableExists if a table is already there.
func (l *Log) Create(ctx context.Context, meta MetaData, at time.Time) error {
	if meta.ID == "" {
		return fmt.Errorf("deltalog: table ID is required")
	}
	if meta.Configuration == nil {
		meta.Configuration = map[string]string{}
	}
	meta.Configuration[FormatKey] = strconv.Itoa(GoAlertFormat)
	if meta.Format.Provider == "" {
		meta.Format = Format{Provider: "parquet", Options: map[string]string{}}
	}
	if meta.PartitionColumns == nil {
		meta.PartitionColumns = []string{}
	}
	meta.CreatedTime = at.UnixMilli()
	actions := []Action{
		{Protocol: &Protocol{MinReaderVersion: ReaderVersion, MinWriterVersion: WriterVersion}},
		{MetaData: &meta},
	}
	_, err := l.commit(ctx, 0, meta.ID, "CREATE TABLE", at, actions)
	if errors.Is(err, ErrConflict) {
		return ErrTableExists
	}
	return err
}

// Commit writes the given version, which must be one past the latest version
// the caller has seen.
//
// It returns the commit's commitInfo, and no error only when the version
// holds this commit (F1). It returns ErrConflict when another commit holds
// it. If the write's outcome stays unclear after its retries, the error wraps
// objstore.ErrUnknown: the commit may still appear, so the caller must not
// report failure, and must find out by reading the version before writing
// again (F32).
func (l *Log) Commit(ctx context.Context, version int64, storeID, operation string, at time.Time, actions []Action) (CommitInfo, error) {
	if version < 1 {
		return CommitInfo{}, fmt.Errorf("deltalog: commits start at version 1")
	}
	return l.commit(ctx, version, storeID, operation, at, actions)
}

func (l *Log) commit(ctx context.Context, version int64, storeID, operation string, at time.Time, actions []Action) (CommitInfo, error) {
	info := CommitInfo{
		Timestamp:       at.UnixMilli(),
		Operation:       operation,
		EngineInfo:      "GoAlert",
		GoAlertStoreID:  storeID,
		GoAlertWriterID: l.writerID,
		GoAlertCommitID: uuid.NewString(),
		GoAlertFormat:   GoAlertFormat,
		GoAlertTime:     at.UnixMicro(),
	}
	data, err := encode(info, actions)
	if err != nil {
		return CommitInfo{}, err
	}
	name := CommitName(version)
	delay := l.Backoff
	var lastErr error
	// unsure is set once an attempt may have landed. From then on only
	// reading the version settles the outcome: a later attempt being
	// rejected says nothing about the earlier one (F32).
	unsure := false
	for attempt := 0; attempt < l.MaxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return CommitInfo{}, objstore.Unknown(fmt.Errorf("deltalog: commit %d: %w (after %v)", version, ctx.Err(), lastErr))
			case <-time.After(delay/2 + rand.N(delay)):
			}
			delay *= 2
		}

		err := l.store.PutIfAbsent(ctx, name, data)
		switch {
		case err == nil:
			return info, nil
		case errors.Is(err, objstore.ErrExists):
			// Someone created the version: another writer, or an earlier
			// attempt of ours whose reply was lost.
			ours, rerr := l.isOurs(ctx, name, info.GoAlertCommitID)
			if rerr != nil {
				lastErr = rerr
				continue
			}
			if ours {
				return info, nil
			}
			return CommitInfo{}, ErrConflict
		case errors.Is(err, objstore.ErrUnknown):
			// The write may have happened. Look before trying again.
			lastErr, unsure = err, true
			ours, rerr := l.isOurs(ctx, name, info.GoAlertCommitID)
			switch {
			case rerr == nil && ours:
				return info, nil
			case rerr == nil:
				return CommitInfo{}, ErrConflict
			case errors.Is(rerr, objstore.ErrNotFound):
				// Not there (yet). The same write is retried, so if the
				// earlier one lands first, the retry recognizes it.
			default:
				lastErr = rerr
			}
		case unsure:
			// This attempt was rejected, but an earlier one may have landed.
			return CommitInfo{}, objstore.Unknown(fmt.Errorf("deltalog: commit %d: an earlier attempt may have landed, then: %v", version, err))
		default:
			// The store rejected the write outright, so it did not happen.
			return CommitInfo{}, fmt.Errorf("deltalog: commit %d: %w", version, err)
		}
	}
	return CommitInfo{}, objstore.Unknown(fmt.Errorf("deltalog: commit %d: outcome still unknown after %d attempts: %v", version, l.MaxAttempts, lastErr))
}

// isOurs reads a commit file and reports whether it holds commitID. The
// error wraps objstore.ErrNotFound if the file does not exist.
func (l *Log) isOurs(ctx context.Context, name, commitID string) (bool, error) {
	data, err := l.store.Get(ctx, name)
	if err != nil {
		return false, err
	}
	info, _, err := decode(data)
	if err != nil {
		return false, err
	}
	return info.GoAlertCommitID == commitID, nil
}

// Latest returns the newest version in the log, by listing the commit files
// after version from. It returns ErrNoTable if there are none at all.
func (l *Log) Latest(ctx context.Context, from int64) (int64, error) {
	after := ""
	if from >= 0 {
		after = CommitName(from)
	}
	names, err := l.store.List(ctx, logDir, after)
	if err != nil {
		return 0, err
	}
	latest := from
	for _, n := range names {
		if v, ok := parseCommitName(n); ok && v > latest {
			latest = v
		}
	}
	if latest < 0 {
		return 0, ErrNoTable
	}
	return latest, nil
}

// Snapshot is the table's state as of a version.
type Snapshot struct {
	Version int64
	// CommitID is the GoAlert commit ID of Version, and Time its commit
	// time.
	CommitID string
	Time     time.Time
	Protocol Protocol
	MetaData MetaData
	Files    map[string]Add
	// Removed holds the tombstones of removed files. VACUUM keeps a removed
	// file until its tombstone expires, since a pod that lags behind may
	// still read it (F6).
	Removed map[string]Remove
	Txns    map[string]int64
}

// snapshotAdd and snapshotRemove give an action as a snapshot holds it,
// which is how a checkpoint stores it, so a snapshot loaded from a
// checkpoint equals one built by replaying the log (F25).
func snapshotAdd(a Add) Add {
	a.DataChange = false
	if a.PartitionValues == nil {
		a.PartitionValues = map[string]string{}
	}
	return a
}

func snapshotRemove(r Remove) Remove {
	r.DataChange = false
	if len(r.PartitionValues) == 0 {
		r.PartitionValues = nil
	}
	return r
}

func newSnapshot() *Snapshot {
	return &Snapshot{Version: -1, Files: map[string]Add{}, Removed: map[string]Remove{}, Txns: map[string]int64{}}
}

// catchUp applies every commit after the snapshot's version, up to the
// latest one.
func (l *Log) catchUp(ctx context.Context, s *Snapshot) error {
	latest, err := l.Latest(ctx, s.Version)
	if err != nil {
		return err
	}
	for v := s.Version + 1; v <= latest; v++ {
		info, actions, err := l.Read(ctx, v)
		if errors.Is(err, objstore.ErrNotFound) {
			return fmt.Errorf("%w: version %d is missing, but %d exists", ErrCorrupt, v, latest)
		}
		if err != nil {
			return err
		}
		if err := s.Apply(v, info, actions); err != nil {
			return err
		}
	}
	return nil
}

// checkProtocol refuses protocol versions this code does not implement
// (F19).
func checkProtocol(p Protocol) error {
	if p.MinReaderVersion > ReaderVersion || p.MinWriterVersion > WriterVersion {
		return fmt.Errorf("%w: protocol reader %d / writer %d (supported: %d / %d)", ErrUnsupported, p.MinReaderVersion, p.MinWriterVersion, ReaderVersion, WriterVersion)
	}
	return nil
}

// checkMetaData refuses tables in a GoAlert format, or with settings, that
// this code does not handle (F19, F30).
func checkMetaData(m MetaData) error {
	if f := m.Configuration[FormatKey]; f != strconv.Itoa(GoAlertFormat) {
		return fmt.Errorf("%w: table GoAlert format %q (this version reads %d)", ErrUnsupported, f, GoAlertFormat)
	}
	if m.Configuration["delta.appendOnly"] == "true" {
		return fmt.Errorf("%w: delta.appendOnly tables", ErrUnsupported)
	}
	return nil
}

// Next applies the commit after the snapshot's version, if it exists, and
// reports whether there was one.
func (l *Log) Next(ctx context.Context, s *Snapshot) (bool, error) {
	info, actions, err := l.Read(ctx, s.Version+1)
	if errors.Is(err, objstore.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, s.Apply(s.Version+1, info, actions)
}

// Apply validates one commit and applies it to the snapshot. It refuses
// commits it cannot trust: from another writer or table (F11), or needing a
// newer protocol or format (F19, F30).
func (s *Snapshot) Apply(version int64, info CommitInfo, actions []Action) error {
	if version != s.Version+1 {
		return fmt.Errorf("%w: version %d applied after %d", ErrCorrupt, version, s.Version)
	}
	if info.GoAlertFormat == 0 || info.GoAlertCommitID == "" {
		return fmt.Errorf("%w: version %d (operation %q, engine %q)", ErrForeign, version, info.Operation, info.EngineInfo)
	}
	if info.GoAlertFormat > GoAlertFormat {
		return fmt.Errorf("%w: version %d uses GoAlert format %d (this version reads %d)", ErrUnsupported, version, info.GoAlertFormat, GoAlertFormat)
	}

	next := Snapshot{
		Version:  version,
		Protocol: s.Protocol,
		MetaData: s.MetaData,
		Files:    s.Files,
		Txns:     s.Txns,
	}
	sawProtocol, sawMeta := false, false
	added := map[string]bool{}
	for _, a := range actions {
		switch {
		case a.Protocol != nil:
			p := *a.Protocol
			if err := checkProtocol(p); err != nil {
				return err
			}
			next.Protocol, sawProtocol = p, true
		case a.MetaData != nil:
			m := *a.MetaData
			if err := checkMetaData(m); err != nil {
				return err
			}
			if version > 0 && m.ID != s.MetaData.ID {
				return fmt.Errorf("%w: version %d changes the table ID", ErrCorrupt, version)
			}
			next.MetaData, sawMeta = m, true
		case a.Add != nil:
			if a.Add.Path == "" || added[a.Add.Path] {
				return fmt.Errorf("%w: version %d adds %q badly", ErrCorrupt, version, a.Add.Path)
			}
			added[a.Add.Path] = true
		case a.Remove != nil:
			if _, ok := next.Files[a.Remove.Path]; !ok && !added[a.Remove.Path] {
				return fmt.Errorf("%w: version %d removes unknown file %q", ErrCorrupt, version, a.Remove.Path)
			}
		}
	}
	if version == 0 && (!sawProtocol || !sawMeta) {
		return fmt.Errorf("%w: version 0 must set the protocol and metadata", ErrCorrupt)
	}
	if info.GoAlertStoreID != next.MetaData.ID {
		return fmt.Errorf("%w: version %d belongs to table %q, not %q", ErrForeign, version, info.GoAlertStoreID, next.MetaData.ID)
	}

	// Validated; now change the snapshot.
	for _, a := range actions {
		switch {
		case a.Add != nil:
			s.Files[a.Add.Path] = snapshotAdd(*a.Add)
			delete(s.Removed, a.Add.Path)
		case a.Remove != nil:
			delete(s.Files, a.Remove.Path)
			s.Removed[a.Remove.Path] = snapshotRemove(*a.Remove)
		case a.Txn != nil:
			s.Txns[a.Txn.AppID] = a.Txn.Version
		}
	}
	s.Version, s.CommitID, s.Protocol, s.MetaData = next.Version, info.GoAlertCommitID, next.Protocol, next.MetaData
	s.Time = time.UnixMicro(info.GoAlertTime).UTC()
	return nil
}

// Clone returns a copy that later changes to s do not affect.
func (s *Snapshot) Clone() *Snapshot {
	c := *s
	c.Files = make(map[string]Add, len(s.Files))
	for k, v := range s.Files {
		c.Files[k] = v
	}
	c.Removed = make(map[string]Remove, len(s.Removed))
	for k, v := range s.Removed {
		c.Removed[k] = v
	}
	c.Txns = make(map[string]int64, len(s.Txns))
	for k, v := range s.Txns {
		c.Txns[k] = v
	}
	return &c
}

// FilePaths returns the snapshot's active data files, sorted.
func (s *Snapshot) FilePaths() []string {
	paths := make([]string, 0, len(s.Files))
	for p := range s.Files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	return paths
}

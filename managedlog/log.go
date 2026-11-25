// Package managedlog is a tablelog.Log for a Delta table in an object store
// whose commits something else decides: an arbiter, such as an Iceberg catalog
// (iceberglog) or BigQuery (bigquerylog).
//
// A commit takes three steps. It is staged as an object. Then the arbiter is
// asked to decide that it holds its version, which it does for one commit only
// (F4). Last, the commit is published: its staged object is copied into the
// Delta log, where pods and Delta readers look for it. Whoever next needs a
// commit that was not published, because its writer crashed, publishes it.
//
// Three rules make that safe (docs/filestore-failure-modes.md, F43):
//
//   - Only a commit that was decided is published, so the Delta log never holds
//     one that did not happen.
//   - A decision is asked for only once the commit before is published, so at
//     most the newest is not, and an arbiter need not remember older ones.
//   - The table's metadata names its arbiter, so a pod that would ask another,
//     or none, refuses the table.
//
// To add an arbiter, implement the interface and pass managedlogtest.Run.
package managedlog

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/tablelog"
)

const (
	stagedDir = "_delta_log/_staged_commits/"

	// publishTimeout bounds how long a commit that has happened waits for its
	// publication, which another pod can do as well.
	publishTimeout = 10 * time.Second

	// maxCatchUp bounds how many commits Load reads after the Delta log's
	// newest. The caller reads any later ones.
	maxCatchUp = 1000
)

var (
	// ErrRejected means the arbiter did not do what it was asked to, and would
	// not if it were asked again.
	ErrRejected = errors.New("managedlog: the arbiter rejected the request")

	// ErrBusy means the arbiter did not do what it was asked to, and may if it is
	// asked again, as with a quota.
	ErrBusy = errors.New("managedlog: the arbiter is busy")
)

// A Proposal is a commit that asks to hold a version.
type Proposal struct {
	Version int64
	Info    tablelog.CommitInfo
	Actions []tablelog.Action

	// Base is the ID of the commit that holds the version before, as the Delta
	// log has it.
	Base string

	// Put stores an object whose name nothing else uses, with the log's retries.
	Put func(ctx context.Context, name string, data []byte) error
}

// A Decision is what an arbiter knows of a version.
type Decision struct {
	// CommitID is the commit that holds the version, or empty.
	CommitID string

	// Later means that a later version was decided too, and that the arbiter
	// does not know which commit holds this one. The Delta log does, since it is
	// published. If CommitID is empty and Later is not set, no commit holds the
	// version yet.
	Later bool
}

// Ask asks the arbiter to decide for a proposal. Every call asks for the same
// decision, so however often and late the calls are done, the proposal holds
// its version once, or never.
//
// It returns nil if the proposal holds its version. These errors mean that the
// call did nothing: tablelog.ErrConflict (the version may be taken, or the
// arbiter was doing something else), ErrBusy, ErrRejected and objstore.ErrDenied.
// Any other error means that the call may have done it (F32).
type Ask func(ctx context.Context) error

// An Arbiter decides which commit holds each version of one table. It is one
// pod's connection to whatever decides, and is used by several goroutines.
type Arbiter interface {
	// Manager is the deltalog.ManagerKey of the arbiter's tables. Arbiters that
	// could decide differently, such as those of two projects, have different
	// ones (F43).
	Manager() string

	// Prepare stores what it takes to decide for a proposal, without any effect
	// on the table yet. It returns tablelog.ErrConflict if the version is taken,
	// and tablelog.ErrForeign if the version before is decided for another
	// commit than the proposal's base.
	Prepare(ctx context.Context, p Proposal) (Ask, error)

	// Decision returns what the arbiter knows of a version of the table with an
	// ID. The ID is empty if the Delta log has no table. The error is
	// tablelog.ErrNoTable if the arbiter has none either.
	//
	// A decision may be missing from the answer for a moment, but not be wrong.
	Decision(ctx context.Context, storeID string, version int64) (Decision, error)

	// Check compares the arbiter's state with a snapshot that is, or was until
	// a moment ago, the newest. It returns tablelog.ErrForeign if the snapshot's
	// version was not decided for its commit.
	Check(ctx context.Context, s *tablelog.Snapshot) error
}

// A Creator is an Arbiter that also decides which call creates the table, and
// so which commit holds version 0. For other arbiters the object store does.
//
// Create returns nil if the table is the caller's, tablelog.ErrTableExists if
// another call made it, and objstore.ErrUnknown if that can't be known. After
// any other error there is no new table.
type Creator interface {
	Create(ctx context.Context, meta tablelog.MetaData, info tablelog.CommitInfo) error
}

// A Maintainer is an Arbiter that keeps something for every commit.
type Maintainer interface {
	// Checkpointed is called after a checkpoint, to drop what is not needed.
	Checkpointed(ctx context.Context) error

	// Cleanup deletes what nothing needs and is older than the retention, and
	// returns how much.
	Cleanup(ctx context.Context, now time.Time, retention time.Duration) (int, error)
}

// A SelfTester is an Arbiter that can check, when a pod starts, that it does
// what this package relies on (F33).
type SelfTester interface {
	SelfTest(ctx context.Context) error
}

// Config is what a Log needs.
type Config struct {
	Arbiter Arbiter
	Store   objstore.Store

	// Warn, if set, receives problems that fail nothing (F21).
	Warn func(error)
}

// Log is one pod's handle to a table.
type Log struct {
	arbiter  Arbiter
	store    objstore.Store
	writerID string
	delta    *deltalog.Log
	warnFn   func(error)

	attempts int
	backoff  time.Duration

	mu      sync.Mutex
	storeID string
	tested  bool
	// last is the newest commit this pod knows, which is in the Delta log unless
	// it is pending.
	last    known
	pending *pending
}

// known is which commit holds a version.
type known struct {
	version int64
	id      string
}

// pending is a commit of this pod's that could not be published yet.
type pending struct {
	known
	data []byte
}

var _ tablelog.Log = (*Log)(nil)

// New returns a Log. It does no I/O.
func New(cfg Config, writerID string) (*Log, error) {
	if cfg.Arbiter == nil {
		return nil, errors.New("managedlog: nil arbiter")
	}
	if cfg.Store == nil {
		return nil, errors.New("managedlog: nil object store")
	}
	if cfg.Arbiter.Manager() == "" {
		return nil, errors.New("managedlog: the arbiter has no name")
	}
	l := &Log{
		arbiter: cfg.Arbiter, store: cfg.Store, writerID: writerID,
		delta: deltalog.New(cfg.Store, writerID), warnFn: cfg.Warn,
		attempts: 8, backoff: 100 * time.Millisecond, last: known{version: -1},
	}
	l.delta.Manager = cfg.Arbiter.Manager()
	l.delta.Warn = l.warn
	return l, nil
}

func (l *Log) warn(err error) {
	if l.warnFn != nil {
		l.warnFn(err)
	}
}

// SetRetry sets how often an unclear commit is tried, and its first delay.
// Values that are not positive change nothing.
func (l *Log) SetRetry(attempts int, backoff time.Duration) {
	if attempts > 0 {
		l.attempts = attempts
	}
	if backoff > 0 {
		l.backoff = backoff
	}
	l.delta.SetRetry(attempts, backoff)
}

// definite reports whether an error means a request to the arbiter did
// nothing.
func definite(err error) bool {
	return again(err) || errors.Is(err, ErrRejected) || errors.Is(err, objstore.ErrDenied)
}

// again reports whether it makes sense to repeat a request that did nothing.
func again(err error) bool {
	return errors.Is(err, tablelog.ErrConflict) || errors.Is(err, ErrBusy)
}

// fatal reports whether an error means the table is not one this pod may write
// to, which trying again does not change.
func fatal(err error) bool {
	return errors.Is(err, tablelog.ErrForeign) || errors.Is(err, tablelog.ErrCorrupt) ||
		errors.Is(err, tablelog.ErrUnsupported) || errors.Is(err, tablelog.ErrNoTable)
}

func stagedName(version int64, commitID string) string {
	return fmt.Sprintf("%s%020d.%s.json", stagedDir, version, commitID)
}

func sleep(ctx context.Context, delay *time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(*delay/2 + rand.N(*delay)):
	}
	*delay *= 2
	return nil
}

// putNew stores an object whose name no other call uses, so that nothing
// refers to it yet.
func (l *Log) putNew(ctx context.Context, name string, data []byte) error {
	delay := l.backoff
	var err error
	for attempt := range l.attempts {
		if attempt > 0 {
			if serr := sleep(ctx, &delay); serr != nil {
				break
			}
		}
		err = l.store.PutIfAbsent(ctx, name, data)
		if err == nil || errors.Is(err, objstore.ErrExists) {
			return nil // an earlier attempt's
		}
		if !errors.Is(err, objstore.ErrUnknown) {
			return fmt.Errorf("managedlog: store %s: %w", name, err)
		}
	}
	// %v, not %w: whether it was stored or not, nothing has changed.
	return fmt.Errorf("managedlog: store %s: %v", name, err)
}

// learn notes that a version is in the Delta log, like all before it.
func (l *Log) learn(version int64, commitID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if version > l.last.version {
		l.last = known{version, commitID}
	}
	if l.pending != nil && l.pending.version <= version {
		l.pending = nil
	}
}

// identity returns the table's ID, or "" if the Delta log has no table.
func (l *Log) identity(ctx context.Context) (string, error) {
	l.mu.Lock()
	id := l.storeID
	l.mu.Unlock()
	if id != "" {
		return id, nil
	}
	s, err := l.delta.Load(ctx)
	if errors.Is(err, tablelog.ErrNoTable) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	l.setIdentity(s.MetaData.ID)
	l.learn(s.Version, s.CommitID)
	return s.MetaData.ID, nil
}

func (l *Log) setIdentity(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.storeID = id
}

// Create creates the table. It returns ErrTableExists if a table is already
// there.
func (l *Log) Create(ctx context.Context, meta tablelog.MetaData, at time.Time) error {
	conf := map[string]string{deltalog.ManagerKey: l.arbiter.Manager()}
	for k, v := range meta.Configuration {
		conf[k] = v
	}
	if m := conf[deltalog.ManagerKey]; m != l.arbiter.Manager() {
		return fmt.Errorf("%w: by %q", tablelog.ErrManaged, m)
	}
	meta.Configuration = conf
	meta, err := deltalog.CheckMeta(meta, at)
	if err != nil {
		return err
	}
	creator, ok := l.arbiter.(Creator)
	if !ok {
		if err := l.delta.Create(ctx, meta, at); err != nil {
			return err
		}
		l.setIdentity(meta.ID)
		return nil
	}

	switch _, err := l.delta.Latest(ctx, -1); {
	case err == nil:
		return fmt.Errorf("%w: the object store holds one", tablelog.ErrTableExists)
	case !errors.Is(err, tablelog.ErrNoTable):
		return err
	}
	info := deltalog.NewCommitInfo(meta.ID, l.writerID, "CREATE TABLE", at)
	data, err := deltalog.Encode(info, deltalog.CreateActions(meta))
	if err != nil {
		return err
	}
	if err := l.putNew(ctx, stagedName(0, info.GoAlertCommitID), data); err != nil {
		return err
	}
	if err := creator.Create(ctx, meta, info); err != nil {
		return err
	}
	l.setIdentity(meta.ID)
	l.decided(ctx, known{0, info.GoAlertCommitID}, data)
	return nil
}

// decided publishes a commit of this pod's that was decided. The commit has
// happened, so nothing that goes wrong here may fail it: reading it, or the
// next commit, publishes it if this does not.
func (l *Log) decided(ctx context.Context, k known, data []byte) {
	l.mu.Lock()
	if k.version > l.last.version {
		l.last = k
	}
	l.pending = &pending{known: k, data: data}
	l.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), publishTimeout)
	defer cancel()
	if err := l.publishData(ctx, k, data); err != nil {
		l.warn(err)
	}
}

// publishData copies a commit that was decided into the Delta log.
func (l *Log) publishData(ctx context.Context, k known, data []byte) error {
	err := l.store.PutIfAbsent(ctx, deltalog.CommitName(k.version), data)
	if err != nil {
		// Perhaps someone did it first, or an attempt of this pod's.
		if cerr := l.checkPublished(ctx, k); cerr != nil {
			if errors.Is(cerr, tablelog.ErrForeign) {
				return cerr
			}
			return fmt.Errorf("managedlog: publish version %d: %w", k.version, err)
		}
	}
	l.learn(k.version, k.id)
	return nil
}

// checkPublished checks that the Delta log has a commit as a version. The error
// wraps objstore.ErrNotFound if it has none.
func (l *Log) checkPublished(ctx context.Context, k known) error {
	info, _, err := l.delta.Read(ctx, k.version)
	if err != nil {
		return err
	}
	if info.GoAlertCommitID != k.id {
		return fmt.Errorf("%w: the Delta log has commit %q as version %d, but %s was decided (F43)", tablelog.ErrForeign, info.GoAlertCommitID, k.version, k.id)
	}
	return nil
}

// publish makes sure a commit that was decided is in the Delta log.
func (l *Log) publish(ctx context.Context, k known) error {
	data, err := l.store.Get(ctx, stagedName(k.version, k.id))
	if err == nil {
		return l.publishData(ctx, k, data)
	}
	if !errors.Is(err, objstore.ErrNotFound) {
		return fmt.Errorf("managedlog: read the staged commit %d: %w", k.version, err)
	}
	// A staged commit is deleted only once it is published.
	err = l.checkPublished(ctx, k)
	if errors.Is(err, objstore.ErrNotFound) {
		newest, lerr := l.delta.Latest(ctx, -1)
		if lerr != nil {
			return lerr
		}
		if newest > k.version {
			return fmt.Errorf("managedlog: version %d is not kept any more: %w", k.version, err)
		}
		return fmt.Errorf("%w: commit %s was decided to be version %d, but the object store does not have it (F6)", tablelog.ErrCorrupt, k.id, k.version)
	}
	if err != nil {
		return err
	}
	l.learn(k.version, k.id)
	return nil
}

// settle publishes a version if it was decided. It reports whether it was.
func (l *Log) settle(ctx context.Context, storeID string, version int64) (bool, error) {
	d, err := l.arbiter.Decision(ctx, storeID, version)
	switch {
	case err != nil:
		return false, err
	case d.CommitID != "":
		return true, l.publish(ctx, known{version, d.CommitID})
	}
	return d.Later, nil // if so, it is published
}

// Load returns the newest snapshot, and has the arbiter check it.
func (l *Log) Load(ctx context.Context) (*tablelog.Snapshot, error) {
	if t, ok := l.arbiter.(SelfTester); ok {
		l.mu.Lock()
		tested := l.tested
		l.mu.Unlock()
		if !tested {
			if err := t.SelfTest(ctx); err != nil {
				return nil, fmt.Errorf("managedlog: self-test: %w", err)
			}
			l.mu.Lock()
			l.tested = true
			l.mu.Unlock()
		}
	}
	s, err := l.delta.Load(ctx)
	if errors.Is(err, tablelog.ErrNoTable) {
		// Its creation may have been decided, and not published.
		found, serr := l.settle(ctx, "", 0)
		if serr != nil {
			return nil, serr
		}
		if !found {
			return nil, err
		}
		s, err = l.delta.Load(ctx)
	}
	if err != nil {
		return nil, err
	}
	l.setIdentity(s.MetaData.ID)
	l.learn(s.Version, s.CommitID)
	for range maxCatchUp {
		info, actions, err := l.Read(ctx, s.Version+1)
		if errors.Is(err, objstore.ErrNotFound) {
			break
		}
		if err != nil {
			return nil, err
		}
		if err := s.Apply(s.Version+1, info, actions); err != nil {
			return nil, err
		}
	}
	if err := l.arbiter.Check(ctx, s); err != nil {
		return nil, err
	}
	return s, nil
}

// Read returns a commit. The error wraps objstore.ErrNotFound if the version
// does not exist yet, or not any more.
func (l *Log) Read(ctx context.Context, version int64) (tablelog.CommitInfo, []tablelog.Action, error) {
	info, actions, err := l.delta.Read(ctx, version)
	if err == nil {
		l.learn(version, info.GoAlertCommitID)
		return info, actions, nil
	}
	if !errors.Is(err, objstore.ErrNotFound) {
		return info, actions, err
	}
	notFound := err
	storeID, err := l.identity(ctx)
	if err != nil {
		return tablelog.CommitInfo{}, nil, err
	}
	found, err := l.settle(ctx, storeID, version)
	if err != nil {
		return tablelog.CommitInfo{}, nil, err
	}
	if !found {
		return tablelog.CommitInfo{}, nil, notFound
	}
	// Published just now, or before the next version was decided.
	info, actions, err = l.delta.Read(ctx, version)
	if err == nil {
		l.learn(version, info.GoAlertCommitID)
	}
	return info, actions, err
}

// base returns the commit that holds a version, which is published, for a
// commit on top of it.
func (l *Log) base(ctx context.Context, version int64) (string, error) {
	l.mu.Lock()
	last, pend := l.last, l.pending
	l.mu.Unlock()
	if last.version > version {
		return "", tablelog.ErrConflict
	}
	if pend != nil {
		if err := l.publishData(ctx, pend.known, pend.data); err != nil {
			return "", err
		}
	}
	if last.version == version {
		return last.id, nil
	}
	info, _, err := l.Read(ctx, version)
	if errors.Is(err, objstore.ErrNotFound) {
		return "", fmt.Errorf("managedlog: there is no version %d to commit on top of", version)
	}
	return info.GoAlertCommitID, err
}

// Commit makes a version, which must be one past the latest the caller has
// seen. See deltalog.Log.Commit, which has the same contract.
func (l *Log) Commit(ctx context.Context, version int64, storeID, operation string, at time.Time, actions []tablelog.Action) (tablelog.CommitInfo, error) {
	fail := func(err error) (tablelog.CommitInfo, error) { return tablelog.CommitInfo{}, err }
	if version < 1 {
		return fail(errors.New("managedlog: commits start at version 1"))
	}
	id, err := l.identity(ctx)
	if err != nil {
		return fail(err)
	}
	if id != "" && id != storeID {
		return fail(fmt.Errorf("%w: the table is %q, not %q", tablelog.ErrForeign, id, storeID))
	}
	base, err := l.base(ctx, version-1)
	if err != nil {
		return fail(err)
	}

	info := deltalog.NewCommitInfo(storeID, l.writerID, operation, at)
	data, err := deltalog.Encode(info, actions)
	if err != nil {
		return fail(err)
	}
	ask, err := l.prepare(ctx, Proposal{Version: version, Info: info, Actions: actions, Base: base, Put: l.putNew}, data)
	if err != nil {
		return fail(err)
	}

	k := known{version, info.GoAlertCommitID}
	delay := l.backoff
	var lastErr error
	// unsure is set once a call may have been done. From then on only the
	// arbiter's state settles the outcome: a later call being refused says
	// nothing about the earlier one (F32).
	unsure := false
	for attempt := range l.attempts {
		if attempt > 0 {
			if err := sleep(ctx, &delay); err != nil {
				break
			}
		}
		err := ask(ctx)
		if err == nil {
			l.decided(ctx, k, data)
			return info, nil
		}
		err = fmt.Errorf("managedlog: commit %d: %w", version, err)
		unsure = unsure || !definite(err)
		lastErr = err

		holder, derr := l.holder(ctx, storeID, version)
		switch {
		case derr != nil:
			switch {
			case unsure:
			case !again(err):
				return fail(err)
			case fatal(derr):
				// Not a race with another pod, to be run again (F11).
				return fail(derr)
			}
			lastErr = errors.Join(err, derr)
		case holder == k.id:
			l.decided(ctx, k, data)
			return info, nil
		case holder != "":
			return fail(tablelog.ErrConflict)
		case !unsure && !again(err):
			return fail(err)
		}
		// No commit holds the version, at least not yet, so ask again.
	}
	if unsure {
		return fail(objstore.Unknown(fmt.Errorf("managedlog: commit %d: the outcome is still unknown after %d attempts: %v", version, l.attempts, lastErr)))
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	if errors.Is(lastErr, ErrBusy) {
		return fail(lastErr)
	}
	return fail(fmt.Errorf("%w (the arbiter kept refusing: %v)", tablelog.ErrConflict, lastErr))
}

// prepare stages a commit, and has the arbiter prepare for it at the same time.
func (l *Log) prepare(ctx context.Context, p Proposal, data []byte) (Ask, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	staged := make(chan error, 1)
	go func() {
		err := l.putNew(ctx, stagedName(p.Version, p.Info.GoAlertCommitID), data)
		if err != nil {
			cancel()
		}
		staged <- err
	}()
	ask, err := l.arbiter.Prepare(ctx, p)
	if err != nil {
		cancel()
	}
	serr := <-staged
	if err == nil || (serr != nil && errors.Is(err, context.Canceled)) {
		err = serr
	}
	if err != nil {
		return nil, err
	}
	return ask, nil
}

// holder returns the commit that holds a version, or "" if none does.
func (l *Log) holder(ctx context.Context, storeID string, version int64) (string, error) {
	d, err := l.arbiter.Decision(ctx, storeID, version)
	if err != nil || d.CommitID != "" || !d.Later {
		return d.CommitID, err
	}
	info, _, err := l.delta.Read(ctx, version)
	return info.GoAlertCommitID, err
}

// WriteCheckpoint writes a Delta checkpoint.
func (l *Log) WriteCheckpoint(ctx context.Context, snapshot *tablelog.Snapshot, now time.Time, retention time.Duration) error {
	// A checkpoint must not be newer than the log.
	if _, _, err := l.Read(ctx, snapshot.Version); err != nil {
		return err
	}
	if err := l.delta.WriteCheckpoint(ctx, snapshot, now, retention); err != nil {
		return err
	}
	if m, ok := l.arbiter.(Maintainer); ok {
		if err := m.Checkpointed(ctx); err != nil {
			// The next checkpoint tries again.
			l.warn(err)
		}
	}
	return nil
}

// Cleanup deletes what no reader needs any more: old commits and checkpoints of
// the Delta log, as deltalog.Log.Cleanup does, staged commits that are published
// or lost, and what the arbiter keeps.
func (l *Log) Cleanup(ctx context.Context, now time.Time, retention time.Duration) (int, error) {
	newest, err := l.delta.Latest(ctx, -1)
	if err != nil {
		return 0, err
	}
	total, err := l.delta.Cleanup(ctx, now, retention)
	if err != nil {
		return total, err
	}
	n, err := l.cleanStaged(ctx, newest)
	total += n
	if err != nil {
		return total, err
	}
	if m, ok := l.arbiter.(Maintainer); ok {
		n, err = m.Cleanup(ctx, now, retention)
		total += n
	}
	return total, err
}

// cleanStaged deletes the staged commits of versions before one that is
// published. Those are published, if they happened, and if not, no longer can
// happen.
func (l *Log) cleanStaged(ctx context.Context, published int64) (int, error) {
	names, err := l.store.List(ctx, stagedDir, "")
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, name := range names {
		v, _, _ := strings.Cut(strings.TrimPrefix(name, stagedDir), ".")
		version, err := strconv.ParseInt(v, 10, 64)
		if err != nil || version >= published {
			continue
		}
		if err := l.store.Delete(ctx, name); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

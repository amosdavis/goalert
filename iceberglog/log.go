// Package iceberglog is a tablelog.Log whose commits are decided by an Apache
// Iceberg catalog. One table is then readable both as Iceberg and as Delta
// Lake, over the same data files.
//
// A commit is first staged as an object. Then the catalog is asked, in one
// atomic request, to add a snapshot on top of the one the commit is based on.
// Only one writer's request can succeed for a version, and that decides it
// (F4). Last, the commit is published: its staged object is copied into the
// Delta log, where pods and Delta readers look for it. Whoever next needs a
// commit that was not published, because its writer crashed, publishes it.
//
// Three rules make that safe (docs/filestore-failure-modes.md, F43):
//
//   - Only a commit the catalog has is published, so the Delta log never holds
//     one that did not happen.
//   - A commit is made only once the one before it is published, so at most the
//     newest is not, and the catalog need not keep older snapshots.
//   - The table's metadata names the catalog as what decides commits, so a pod
//     configured to use the Delta log alone refuses the table.
//
// The catalog's metadata stays small, since a catalog rewrites all of it for
// every commit: a snapshot says which commit it is and no more, few snapshots
// are kept, and the number of manifests is fixed (F46).
package iceberglog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/tablelog"
)

const (
	// Manager is the deltalog.ManagerKey of the tables this package makes.
	Manager = "iceberg"

	// DefaultSnapshots is how many snapshots the catalog keeps. GoAlert needs
	// only the newest; others let Iceberg readers finish or look back.
	DefaultSnapshots = 100

	// The table's properties.
	propStoreID = "goalert.store-id"
	propFormat  = deltalog.FormatKey
	propCreate  = "goalert.create-commit-id"

	// A snapshot's properties.
	sumVersion  = "goalert.version"
	sumCommitID = "goalert.commit-id"
	sumStoreID  = "goalert.store-id"
	sumWriterID = "goalert.writer-id"
	sumFormat   = "goalert.format"

	stagedDir = "_delta_log/_staged_commits/"

	// publishTimeout bounds how long a commit that has happened waits for its
	// publication, which another pod can do as well.
	publishTimeout = 10 * time.Second
)

// ErrRejected marks an error that means the catalog did not do what it was
// asked to. Config.Classify uses it.
var ErrRejected = errors.New("iceberglog: the catalog rejected the request")

// Config says where the table is.
type Config struct {
	// Catalog holds the table, and Ident is its name there. The namespace must
	// exist.
	Catalog catalog.Catalog
	Ident   table.Identifier

	// Store holds the table's files, and Location is the URI by which Iceberg
	// readers reach the store's root, such as "gs://bucket/prefix". If the two
	// are not the same place, readers find no data (F49).
	Store    objstore.Store
	Location string

	// Classify, if set, marks the catalog's errors: with objstore.ErrDenied if
	// it refused the credentials, and ErrRejected if a request did not happen.
	// Any other error may mean that it did, but for table.ErrCommitFailed.
	Classify func(err error) error

	// Snapshots is how many snapshots the catalog keeps, DefaultSnapshots if 0.
	Snapshots int

	// Warn, if set, receives problems that fail nothing (F21).
	Warn func(error)
}

// Log is one writer's handle to a table.
type Log struct {
	cfg      Config
	location string
	writerID string
	delta    *deltalog.Log

	attempts int
	backoff  time.Duration

	mu sync.Mutex
	// head is the newest state this process knows, or nil.
	head *head
	// newest is the newest version the catalog has reported.
	newest int64
	// published is a version that is in the Delta log, like all before it.
	published int64
	// files are the metadata files of head's snapshot, which never change.
	files map[string][]byte
}

var _ tablelog.Log = (*Log)(nil)

// New returns a Log. It does no I/O, and the caller keeps ownership of the
// catalog.
func New(cfg Config, writerID string) (*Log, error) {
	switch {
	case cfg.Catalog == nil:
		return nil, errors.New("iceberglog: nil catalog")
	case cfg.Store == nil:
		return nil, errors.New("iceberglog: nil object store")
	case len(cfg.Ident) < 2:
		return nil, errors.New("iceberglog: the table's name needs a namespace")
	case cfg.Snapshots < 0:
		return nil, errors.New("iceberglog: a negative number of snapshots")
	}
	location := strings.TrimRight(cfg.Location, "/")
	if u, err := url.Parse(location); err != nil || u.Scheme == "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("iceberglog: the location %q is not a URI", cfg.Location)
	}
	if cfg.Snapshots == 0 {
		cfg.Snapshots = DefaultSnapshots
	}
	if cfg.Classify == nil {
		cfg.Classify = func(err error) error { return err }
	}
	l := &Log{
		cfg: cfg, location: location, writerID: writerID, delta: deltalog.New(cfg.Store, writerID),
		attempts: 8, backoff: 100 * time.Millisecond, published: -1, files: map[string][]byte{},
	}
	l.delta.Manager = Manager
	l.delta.Warn = l.warn
	return l, nil
}

func (l *Log) warn(err error) {
	if l.cfg.Warn != nil {
		l.cfg.Warn(err)
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

func (l *Log) catalogErr(op string, err error) error {
	return l.cfg.Classify(fmt.Errorf("iceberglog: %s: %w", op, err))
}

// definite reports whether an error means a request to the catalog did not
// happen.
func definite(err error) bool {
	return errors.Is(err, table.ErrCommitFailed) || errors.Is(err, ErrRejected) || errors.Is(err, objstore.ErrDenied)
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

// head is the catalog's state of the table.
type head struct {
	meta     table.Metadata
	storeID  string
	version  int64
	commitID string
	// snapshot is nil at version 0, which the table was created with.
	snapshot *table.Snapshot
}

// parse checks that the catalog's table is GoAlert's, and at the version a
// GoAlert commit made.
func (l *Log) parse(meta table.Metadata) (*head, error) {
	props := meta.Properties()
	h := &head{meta: meta, storeID: props[propStoreID], commitID: props[propCreate]}
	if h.storeID == "" || h.commitID == "" {
		return nil, fmt.Errorf("%w: GoAlert did not create the Iceberg table", tablelog.ErrForeign)
	}
	if f := props[propFormat]; f != strconv.Itoa(deltalog.GoAlertFormat) {
		return nil, fmt.Errorf("%w: the Iceberg table has GoAlert format %q (this version reads %d)", tablelog.ErrUnsupported, f, deltalog.GoAlertFormat)
	}
	if loc := strings.TrimRight(meta.Location(), "/"); loc != l.location {
		return nil, fmt.Errorf("%w: the Iceberg table is at %s, but the object store is %s (F49)", tablelog.ErrUnsupported, loc, l.location)
	}
	if v := meta.Version(); v != 1 && v != 2 {
		return nil, fmt.Errorf("%w: Iceberg format version %d", tablelog.ErrUnsupported, v)
	}
	if spec := meta.PartitionSpec(); !spec.IsUnpartitioned() {
		return nil, fmt.Errorf("%w: the Iceberg table was partitioned", tablelog.ErrForeign)
	}
	h.snapshot = meta.CurrentSnapshot()
	if h.snapshot == nil {
		return h, nil
	}
	var p iceberg.Properties
	if s := h.snapshot.Summary; s != nil {
		p = s.Properties
	}
	if p[sumVersion] == "" || p[sumCommitID] == "" || p[sumStoreID] != h.storeID {
		return nil, fmt.Errorf("%w: GoAlert did not make Iceberg snapshot %d (F11)", tablelog.ErrForeign, h.snapshot.SnapshotID)
	}
	format, err := strconv.Atoi(p[sumFormat])
	if err != nil || format < 1 {
		return nil, fmt.Errorf("%w: Iceberg snapshot %d has GoAlert format %q", tablelog.ErrCorrupt, h.snapshot.SnapshotID, p[sumFormat])
	}
	if format > deltalog.GoAlertFormat {
		return nil, fmt.Errorf("%w: Iceberg snapshot %d uses GoAlert format %d (this version reads %d)", tablelog.ErrUnsupported, h.snapshot.SnapshotID, format, deltalog.GoAlertFormat)
	}
	h.version, err = strconv.ParseInt(p[sumVersion], 10, 64)
	if err != nil || h.version < 1 {
		return nil, fmt.Errorf("%w: Iceberg snapshot %d has GoAlert version %q", tablelog.ErrCorrupt, h.snapshot.SnapshotID, p[sumVersion])
	}
	h.commitID = p[sumCommitID]
	return h, nil
}

// remember makes h the newest state this process knows.
func (l *Log) remember(h *head) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.head != nil && l.head.meta.TableUUID() != h.meta.TableUUID() {
		return fmt.Errorf("%w: the Iceberg table was replaced (F27)", tablelog.ErrCorrupt)
	}
	if h.version < l.newest {
		// Either a stale answer, which the next one corrects, or someone rolled
		// the table back, which stays an error.
		return fmt.Errorf("iceberglog: the catalog went back from version %d to %d", l.newest, h.version)
	}
	l.head, l.newest = h, h.version
	return nil
}

// load asks the catalog for the newest state.
func (l *Log) load(ctx context.Context) (*head, error) {
	tbl, err := l.cfg.Catalog.LoadTable(ctx, l.cfg.Ident)
	if errors.Is(err, catalog.ErrNoSuchTable) || errors.Is(err, catalog.ErrNoSuchNamespace) {
		return nil, fmt.Errorf("%w: the catalog has no table %s", tablelog.ErrNoTable, strings.Join(l.cfg.Ident, "."))
	}
	if err != nil {
		return nil, l.catalogErr("load the table", err)
	}
	h, err := l.parse(tbl.Metadata())
	if err != nil {
		return nil, err
	}
	return h, l.remember(h)
}

// Create creates the table, in the catalog first, which decides whether this
// call did. It returns ErrTableExists if a table is already there.
func (l *Log) Create(ctx context.Context, meta tablelog.MetaData, at time.Time) error {
	conf := map[string]string{deltalog.ManagerKey: Manager}
	for k, v := range meta.Configuration {
		conf[k] = v
	}
	if conf[deltalog.ManagerKey] != Manager {
		return fmt.Errorf("%w: by %q", tablelog.ErrManaged, conf[deltalog.ManagerKey])
	}
	meta.Configuration = conf
	meta, err := deltalog.CheckMeta(meta, at)
	if err != nil {
		return err
	}
	schema, err := icebergSchema(meta)
	if err != nil {
		return err
	}
	switch _, err := l.delta.Latest(ctx, -1); {
	case err == nil:
		return fmt.Errorf("%w: the object store holds a Delta table", tablelog.ErrTableExists)
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
	tbl, err := l.cfg.Catalog.CreateTable(ctx, l.cfg.Ident, schema,
		catalog.WithLocation(l.location),
		catalog.WithPartitionSpec(iceberg.UnpartitionedSpec),
		catalog.WithProperties(iceberg.Properties{
			propStoreID: meta.ID,
			propFormat:  strconv.Itoa(deltalog.GoAlertFormat),
			propCreate:  info.GoAlertCommitID,
			"comment":   "GoAlert's data. Only GoAlert may write to this table.",

			table.PropertyFormatVersion: "2",
			// GoAlert deletes data files itself, once no pod needs them (F6).
			table.GCEnabledKey: "false",
			// The catalog deletes old versions of its metadata (F46).
			table.MetadataDeleteAfterCommitEnabledKey: "true",
			table.MetadataPreviousVersionsMaxKey:      "20",
		}))
	if err == nil {
		err = l.created(ctx, tbl.Metadata(), info.GoAlertCommitID, data)
		return err
	}
	if cerr := l.catalogErr("create the table", err); !errors.Is(err, catalog.ErrTableAlreadyExists) && definite(cerr) {
		return cerr
	}
	// The table may be there, and be this call's: the reply may have been lost,
	// or a retry may have met the first attempt.
	found, lerr := l.cfg.Catalog.LoadTable(ctx, l.cfg.Ident)
	switch {
	case lerr == nil && found.Properties()[propCreate] == info.GoAlertCommitID:
		return l.created(ctx, found.Metadata(), info.GoAlertCommitID, data)
	case lerr == nil || errors.Is(err, catalog.ErrTableAlreadyExists):
		return fmt.Errorf("%w: in the catalog", tablelog.ErrTableExists)
	case errors.Is(err, catalog.ErrNoSuchNamespace):
		return fmt.Errorf("iceberglog: create the table: %w", err)
	}
	return objstore.Unknown(l.catalogErr("create the table", fmt.Errorf("%w (and reading it back: %v)", err, lerr)))
}

func (l *Log) created(ctx context.Context, meta table.Metadata, commitID string, data []byte) error {
	h, err := l.parse(meta)
	if err != nil {
		return fmt.Errorf("iceberglog: the catalog created a table GoAlert can't use; drop it: %w", err)
	}
	if err := l.remember(h); err != nil {
		return err
	}
	if err := l.publishData(ctx, 0, commitID, data); err != nil {
		// It is created, and opening it publishes version 0.
		l.warn(err)
	}
	return nil
}

// putNew stores an object whose name no other call uses.
func (l *Log) putNew(ctx context.Context, name string, data []byte) error {
	delay := l.backoff
	var err error
	for attempt := range l.attempts {
		if attempt > 0 {
			if serr := sleep(ctx, &delay); serr != nil {
				break
			}
		}
		err = l.cfg.Store.PutIfAbsent(ctx, name, data)
		if err == nil || errors.Is(err, objstore.ErrExists) {
			return nil // an earlier attempt's
		}
		if !errors.Is(err, objstore.ErrUnknown) {
			return fmt.Errorf("iceberglog: store %s: %w", name, err)
		}
	}
	// %v, not %w: nothing refers to the object yet, so nothing has changed.
	return fmt.Errorf("iceberglog: store %s: %v", name, err)
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

// publishData copies a commit the catalog has into the Delta log.
func (l *Log) publishData(ctx context.Context, version int64, commitID string, data []byte) error {
	name := deltalog.CommitName(version)
	err := l.cfg.Store.PutIfAbsent(ctx, name, data)
	if err != nil {
		// Perhaps someone did it first, or an attempt of this pod's.
		id, rerr := l.commitID(ctx, version)
		switch {
		case rerr == nil && id != commitID:
			return fmt.Errorf("%w: the Delta log has commit %s as version %d, but the catalog has %s (F43)", tablelog.ErrForeign, id, version, commitID)
		case rerr != nil:
			return fmt.Errorf("iceberglog: publish version %d: %w", version, err)
		}
	}
	l.mu.Lock()
	l.published = max(l.published, version)
	l.mu.Unlock()
	return nil
}

// commitID reads which commit the Delta log has as a version.
func (l *Log) commitID(ctx context.Context, version int64) (string, error) {
	info, _, err := l.delta.Read(ctx, version)
	if err != nil {
		return "", err
	}
	return info.GoAlertCommitID, nil
}

// publish makes sure a commit the catalog has is in the Delta log.
func (l *Log) publish(ctx context.Context, h *head) error {
	l.mu.Lock()
	done := l.published >= h.version
	l.mu.Unlock()
	if done {
		return nil
	}
	data, err := l.cfg.Store.Get(ctx, stagedName(h.version, h.commitID))
	if err == nil {
		return l.publishData(ctx, h.version, h.commitID, data)
	}
	if !errors.Is(err, objstore.ErrNotFound) {
		return fmt.Errorf("iceberglog: read the staged commit %d: %w", h.version, err)
	}
	// A staged commit is deleted only once it is published.
	id, err := l.commitID(ctx, h.version)
	if errors.Is(err, objstore.ErrNotFound) {
		return fmt.Errorf("%w: the catalog has commit %s as version %d, but the object store does not (F6)", tablelog.ErrCorrupt, h.commitID, h.version)
	}
	if err != nil {
		return err
	}
	if id != h.commitID {
		return fmt.Errorf("%w: the Delta log has commit %s as version %d, but the catalog has %s (F43)", tablelog.ErrForeign, id, h.version, h.commitID)
	}
	l.mu.Lock()
	l.published = max(l.published, h.version)
	l.mu.Unlock()
	return nil
}

// Load returns the newest snapshot, and checks that Iceberg's files are the
// same (F47).
func (l *Log) Load(ctx context.Context) (*tablelog.Snapshot, error) {
	h, err := l.load(ctx)
	if err != nil {
		return nil, err
	}
	if err := l.publish(ctx, h); err != nil {
		return nil, err
	}
	s, err := l.delta.Load(ctx)
	if err != nil {
		return nil, err
	}
	if s.MetaData.ID != h.storeID {
		return nil, fmt.Errorf("%w: the Delta log is table %q's, but the catalog's table is %q", tablelog.ErrForeign, s.MetaData.ID, h.storeID)
	}
	if s.Version != h.version {
		// Other pods committed meanwhile, or the Delta log holds what the
		// catalog does not know.
		if h, err = l.load(ctx); err != nil {
			return nil, err
		}
		if s.Version > h.version {
			return nil, fmt.Errorf("%w: the Delta log has version %d, but the catalog is at %d (F43)", tablelog.ErrForeign, s.Version, h.version)
		}
	}
	if s.Version == h.version {
		if err := l.verify(ctx, h, s); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Read returns a commit. The error wraps objstore.ErrNotFound if the version
// does not exist yet, or not any more.
func (l *Log) Read(ctx context.Context, version int64) (tablelog.CommitInfo, []tablelog.Action, error) {
	info, actions, err := l.delta.Read(ctx, version)
	if err == nil || !errors.Is(err, objstore.ErrNotFound) {
		return info, actions, err
	}
	notFound := err
	h, err := l.load(ctx)
	if err != nil {
		return tablelog.CommitInfo{}, nil, err
	}
	switch {
	case h.version < version:
		return tablelog.CommitInfo{}, nil, notFound
	case h.version == version:
		if err := l.publish(ctx, h); err != nil {
			return tablelog.CommitInfo{}, nil, err
		}
	}
	// Published just now, or before the next commit was made.
	return l.delta.Read(ctx, version)
}

// Commit makes a version, which must be one past the latest the caller has
// seen. See deltalog.Log.Commit, which has the same contract.
func (l *Log) Commit(ctx context.Context, version int64, storeID, operation string, at time.Time, actions []tablelog.Action) (tablelog.CommitInfo, error) {
	fail := func(err error) (tablelog.CommitInfo, error) { return tablelog.CommitInfo{}, err }
	if version < 1 {
		return fail(errors.New("iceberglog: commits start at version 1"))
	}
	for _, a := range actions {
		if a.MetaData != nil || a.Protocol != nil {
			// Iceberg's schema would have to change with it.
			return fail(fmt.Errorf("%w: changing the metadata of an Iceberg table", tablelog.ErrUnsupported))
		}
	}
	l.mu.Lock()
	h := l.head
	l.mu.Unlock()
	if h == nil || h.version != version-1 {
		var err error
		if h, err = l.load(ctx); err != nil {
			return fail(err)
		}
	}
	switch {
	case h.version >= version:
		return fail(tablelog.ErrConflict)
	case h.version < version-1:
		return fail(fmt.Errorf("iceberglog: version %d on top of %d, which is the catalog's newest", version, h.version))
	case storeID != h.storeID:
		return fail(fmt.Errorf("%w: the catalog's table is %q, not %q", tablelog.ErrForeign, h.storeID, storeID))
	}
	if err := l.publish(ctx, h); err != nil {
		return fail(err)
	}

	info := deltalog.NewCommitInfo(storeID, l.writerID, operation, at)
	data, err := deltalog.Encode(info, actions)
	if err != nil {
		return fail(err)
	}
	snap, files, err := l.stage(ctx, h, info, version, actions, data)
	if err != nil {
		return fail(err)
	}
	updates := []table.Update{
		table.NewAddSnapshotUpdate(snap),
		table.NewSetSnapshotRefUpdate(table.MainBranch, snap.SnapshotID, table.BranchRef, 0, 0, 0),
	}
	if h.meta.Properties()[table.DefaultNameMappingKey] == "" {
		// GoAlert's data files have column names and no field IDs, so without
		// this, readers find no columns (F35).
		mapping, err := json.Marshal(h.meta.CurrentSchema().NameMapping())
		if err != nil {
			return fail(err)
		}
		updates = append(updates, table.NewSetPropertiesUpdate(iceberg.Properties{table.DefaultNameMappingKey: string(mapping)}))
	}
	var parent *int64
	if h.snapshot != nil {
		parent = &h.snapshot.SnapshotID
	}
	reqs := []table.Requirement{
		table.AssertTableUUID(h.meta.TableUUID()),
		table.AssertRefSnapshotID(table.MainBranch, parent),
	}

	committed := func(now *head) (tablelog.CommitInfo, error) {
		l.mu.Lock()
		if now != nil && now.version == version {
			l.files = files
		} else {
			l.files = map[string][]byte{}
		}
		l.mu.Unlock()
		// The commit has happened, so this must not fail it. Reading it, or the
		// next commit, publishes it if this does not.
		pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), publishTimeout)
		defer cancel()
		if err := l.publishData(pctx, version, info.GoAlertCommitID, data); err != nil {
			l.warn(err)
		}
		return info, nil
	}

	delay := l.backoff
	var lastErr error
	// unsure is set once a request may have been done. From then on only the
	// catalog's state settles the outcome: a later request being refused says
	// nothing about the earlier one (F32).
	unsure := false
	for attempt := range l.attempts {
		if attempt > 0 {
			if err := sleep(ctx, &delay); err != nil {
				break
			}
		}
		// Every attempt asks for the same snapshot on top of the same parent,
		// so at most one of them is done.
		meta, _, err := l.cfg.Catalog.CommitTable(ctx, l.cfg.Ident, reqs, updates)
		if err == nil {
			now, perr := l.parse(meta)
			if perr == nil {
				perr = l.remember(now)
			}
			if perr != nil {
				now = nil
				l.warn(perr)
			}
			return committed(now)
		}
		err = l.catalogErr(fmt.Sprintf("commit %d", version), err)
		unsure = unsure || !definite(err)
		lastErr = err

		now, lerr := l.load(ctx)
		switch {
		case lerr != nil:
			switch {
			case unsure:
			case !errors.Is(err, table.ErrCommitFailed):
				return fail(err)
			case fatal(lerr):
				// Not a conflict with another pod, to be run again (F11).
				return fail(lerr)
			}
			lastErr = errors.Join(err, lerr)
		case now.version >= version:
			id := now.commitID
			if now.version > version {
				// The versions before the newest are published.
				if id, lerr = l.commitID(ctx, version); lerr != nil {
					lastErr, unsure = errors.Join(err, lerr), true
					continue
				}
			}
			if id == info.GoAlertCommitID {
				return committed(now)
			}
			return fail(tablelog.ErrConflict)
		case !unsure && !errors.Is(err, table.ErrCommitFailed):
			return fail(err)
		}
		// Not there, at least not yet, so ask again. A conflict that left the
		// version free is the catalog's, with a change of something else, such
		// as a snapshot's expiry.
	}
	if unsure {
		return fail(objstore.Unknown(fmt.Errorf("iceberglog: commit %d: the outcome is still unknown after %d attempts: %v", version, l.attempts, lastErr)))
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	return fail(fmt.Errorf("%w (the catalog kept refusing: %v)", tablelog.ErrConflict, lastErr))
}

// WriteCheckpoint writes a Delta checkpoint, and has the catalog drop the
// snapshots it need not keep.
func (l *Log) WriteCheckpoint(ctx context.Context, snapshot *tablelog.Snapshot, now time.Time, retention time.Duration) error {
	h, err := l.load(ctx)
	if err != nil {
		return err
	}
	if h.version == snapshot.Version {
		if err := l.publish(ctx, h); err != nil {
			return err
		}
	}
	if err := l.delta.WriteCheckpoint(ctx, snapshot, now, retention); err != nil {
		return err
	}
	if _, err := l.expire(ctx, h); err != nil {
		// The next checkpoint tries again.
		l.warn(err)
	}
	return nil
}

// expire has the catalog drop all but the newest snapshots. It deletes no
// files.
func (l *Log) expire(ctx context.Context, h *head) (int, error) {
	keep := make(map[int64]bool)
	for _, ref := range h.meta.Refs() {
		keep[ref.SnapshotID] = true
	}
	snaps := h.meta.Snapshots()
	ids := expired(snaps, keep, l.cfg.Snapshots)
	if len(ids) == 0 {
		return 0, nil
	}
	_, _, err := l.cfg.Catalog.CommitTable(ctx, l.cfg.Ident,
		[]table.Requirement{table.AssertTableUUID(h.meta.TableUUID())},
		[]table.Update{table.NewRemoveSnapshotsUpdate(ids, false)})
	if err != nil {
		return 0, l.catalogErr("expire snapshots", err)
	}
	return len(ids), nil
}

// Cleanup deletes what no reader needs any more: old commits and checkpoints
// of the Delta log, as deltalog.Log.Cleanup does, staged commits that are
// published or lost, and manifests of snapshots that were dropped, or never
// added, longer ago than retention.
func (l *Log) Cleanup(ctx context.Context, now time.Time, retention time.Duration) (int, error) {
	h, err := l.load(ctx)
	if err != nil {
		return 0, err
	}
	if err := l.publish(ctx, h); err != nil {
		return 0, err
	}
	total, err := l.delta.Cleanup(ctx, now, retention)
	if err != nil {
		return total, err
	}
	n, err := l.cleanStaged(ctx, h)
	total += n
	if err != nil {
		return total, err
	}
	if n, err = l.expire(ctx, h); err != nil {
		return total, err
	}
	if n > 0 {
		if h, err = l.load(ctx); err != nil {
			return total, err
		}
	}
	n, err = l.cleanMetadata(ctx, h, now.Add(-max(retention, minMetadataAge)))
	return total + n, err
}

// cleanStaged deletes the staged commits of versions before the newest. Those
// are published, if they happened, and if not, no longer can happen.
func (l *Log) cleanStaged(ctx context.Context, h *head) (int, error) {
	names, err := l.cfg.Store.List(ctx, stagedDir, "")
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, name := range names {
		v, _, _ := strings.Cut(strings.TrimPrefix(name, stagedDir), ".")
		version, err := strconv.ParseInt(v, 10, 64)
		if err != nil || version >= h.version {
			continue
		}
		if err := l.cfg.Store.Delete(ctx, name); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

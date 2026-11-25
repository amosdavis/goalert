package filestore

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/deltatable"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/tablelog"
)

// This file keeps the store in a Delta table that several pods share
// (docs/filestore-plan.md). Every pod holds the whole state in memory, as
// with the local log, and follows the table's log. A transaction is one
// Delta commit. It rewrites the data file of every bucket it changed, and is
// made on top of the latest version: if another pod commits first, the
// transaction runs again on the new state (F4).

// ErrConflicts means a write kept losing to other pods' commits and gave up
// (F40). Nothing was changed.
var ErrConflicts = errors.New("filestore: too many conflicting commits from other pods")

const (
	// defaultBuckets is how many buckets a table's rows are spread over,
	// unless Table.Buckets says otherwise.
	defaultBuckets = 16

	// bucketsKey is the table configuration that records a table's bucket
	// count, which can never change once rows are stored by it.
	bucketsKey = "goalert.buckets."

	bucketIndex = "_bucket"

	// minChunkGrace is the shortest grace period a chunked object store
	// accepts (chunkstore.MinGracePeriod).
	minChunkGrace = time.Hour
)

// deltaDef is what a Delta table needs from a table definition.
type deltaDef interface {
	deltaInit() error
	deltaColumn() deltatable.Column
	deltaBuckets() int
}

// deltaData is what a Delta table needs from a table's data.
type deltaData interface {
	encodeBucket(bucket int) (data []byte, rows int, err error)
	bucketKeys(bucket int) []string
	decodeFile(data []byte) ([]deltaRow, error)
	hasKey(key string) bool
	removeKey(key string)
	putRow(r deltaRow) error
}

type deltaRow struct {
	key string
	val any
}

// Buckets sets how many data files the table's rows are spread over in a
// Delta table, 16 by default. A change rewrites its bucket's whole file, so
// more buckets mean less to rewrite (F39). The count is recorded in the
// table and cannot change later.
func (t *Table[V]) Buckets(n int) *Table[V] {
	t.schema.checkOpen()
	if n < 1 || n > 4096 {
		panic(fmt.Sprintf("filestore: table %s: bucket count %d is out of range", t.name, n))
	}
	t.buckets = n
	return t
}

func (t *Table[V]) deltaInit() error {
	rt, err := deltatable.NewRowType[V](t.name)
	if err != nil {
		return err
	}
	if t.buckets == 0 {
		t.buckets = defaultBuckets
	}
	n := t.buckets
	t.bucketIx = t.Index(bucketIndex, func(v V) []string {
		return []string{strconv.Itoa(deltatable.Bucket(t.keyFn(v), n))}
	})
	t.rowType = rt
	return nil
}

func (t *Table[V]) deltaColumn() deltatable.Column { return t.rowType }
func (t *Table[V]) deltaBuckets() int              { return t.buckets }

func (d *typedTable[V]) bucketScan(bucket int, fn func(key string, v V)) {
	prefix := strconv.Itoa(bucket) + "\x00"
	d.idx[d.def.bucketIx.pos].AscendGreaterOrEqual(prefix, func(e string) bool {
		if !strings.HasPrefix(e, prefix) {
			return false
		}
		it, _ := d.rows.Get(item[V]{key: e[len(prefix):]})
		fn(it.key, it.val)
		return true
	})
}

func (d *typedTable[V]) encodeBucket(bucket int) ([]byte, int, error) {
	var rows []V
	d.bucketScan(bucket, func(_ string, v V) { rows = append(rows, v) })
	if len(rows) == 0 {
		return nil, 0, nil
	}
	data, err := d.def.rowType.Encode(rows)
	return data, len(rows), err
}

func (d *typedTable[V]) bucketKeys(bucket int) []string {
	var keys []string
	d.bucketScan(bucket, func(key string, _ V) { keys = append(keys, key) })
	return keys
}

func (d *typedTable[V]) decodeFile(data []byte) ([]deltaRow, error) {
	rows, err := d.def.rowType.Decode(data)
	if err != nil {
		return nil, corruptf("table %s: %v", d.def.name, err)
	}
	out := make([]deltaRow, len(rows))
	for i, v := range rows {
		out[i] = deltaRow{key: d.def.keyFn(v), val: v}
	}
	return out, nil
}

func (d *typedTable[V]) hasKey(key string) bool {
	_, ok := d.rows.Get(item[V]{key: key})
	return ok
}

func (d *typedTable[V]) removeKey(key string) { d.delete(key) }

func (d *typedTable[V]) putRow(r deltaRow) error {
	if err := checkKey(d.def.name, "primary", r.key); err != nil {
		return corruptf("%v", err)
	}
	if err := d.put(r.key, r.val.(V)); err != nil {
		return corruptf("table %s: %v", d.def.name, err)
	}
	return nil
}

// deltaInit sets the schema up for a Delta table. After that it can't be
// used with the local log, whose rows are normalized differently (F25).
func (s *Schema) deltaInit() error {
	if s.delta {
		return s.deltaErr
	}
	if s.frozen {
		return errors.New("filestore: this schema is in use by a local store")
	}
	s.delta = true
	for _, name := range s.names {
		if err := s.tables[name].deltaInit(); err != nil {
			s.deltaErr = fmt.Errorf("filestore: table %s: %w", name, err)
			return s.deltaErr
		}
	}
	return nil
}

// deltaMetaData describes the Delta table this schema needs.
func (s *Schema) deltaMetaData() (deltalog.MetaData, error) {
	cols := make([]deltatable.Column, 0, len(s.names))
	conf := make(map[string]string, len(s.names))
	for _, name := range s.names {
		def := s.tables[name]
		cols = append(cols, def.deltaColumn())
		conf[bucketsKey+name] = strconv.Itoa(def.deltaBuckets())
	}
	schema, err := deltatable.SchemaString(cols...)
	if err != nil {
		return deltalog.MetaData{}, err
	}
	return deltalog.MetaData{
		Name: "goalert", SchemaString: schema,
		PartitionColumns: deltatable.PartitionColumns(), Configuration: conf,
	}, nil
}

// checkDeltaMetaData refuses a table whose schema or bucket counts differ
// from this version's. Reading it could drop columns this version does not
// know when it rewrites a file, or look for rows in the wrong bucket (F19,
// F30).
func checkDeltaMetaData(got, want deltalog.MetaData) error {
	if got.SchemaString != want.SchemaString {
		return fmt.Errorf("%w: the Delta table's schema is not this version's", ErrVersion)
	}
	if !slices.Equal(got.PartitionColumns, want.PartitionColumns) {
		return fmt.Errorf("%w: the Delta table is partitioned by %v, not %v", ErrVersion, got.PartitionColumns, want.PartitionColumns)
	}
	for k, v := range want.Configuration {
		if got.Configuration[k] != v {
			return fmt.Errorf("%w: the Delta table has %s=%q, this version uses %q", ErrVersion, k, got.Configuration[k], v)
		}
	}
	for k := range got.Configuration {
		if _, ok := want.Configuration[k]; strings.HasPrefix(k, bucketsKey) && !ok {
			return fmt.Errorf("%w: the Delta table has %s, a table this version does not know", ErrVersion, k)
		}
	}
	return nil
}

// DeltaOptions configure a store in a Delta table. The zero value is ready
// to use.
type DeltaOptions struct {
	// Clock supplies transaction timestamps. The default is the system clock.
	Clock Clock

	// WriterID names this pod in the commits it writes. The default is
	// random.
	WriterID string

	// CheckpointEvery is how many versions apart checkpoints are written.
	// The default is 100; a negative value turns them off.
	CheckpointEvery int64

	// TombstoneRetention is how long a removed data file is kept for pods
	// that still read an older version (F6). The default is
	// deltalog.DefaultTombstoneRetention.
	TombstoneRetention time.Duration

	// ChunkGracePeriod is how long an object store that splits objects into
	// chunks, such as a document database's, keeps chunks nobody uses (F42).
	// It defaults to TombstoneRetention, and is never under an hour.
	ChunkGracePeriod time.Duration

	// LogRetention is how long commits and checkpoints are kept before
	// CleanLog deletes them. The default is 7 days. A pod that goes half of
	// the shorter retention without confirming it is current reloads from a
	// checkpoint (F16).
	LogRetention time.Duration

	// MaxConflicts is how often Update tries a transaction that loses to
	// other pods' commits before it returns ErrConflicts. The default is 20.
	MaxConflicts int

	// MaxBatch is the most transactions one commit groups. BatchTimeout
	// bounds the storage work of a batch. The defaults are 256 and one minute.
	MaxBatch     int
	BatchTimeout time.Duration

	// ConflictBackoff is the first delay after another pod wins a commit.
	// It doubles to MaxConflictBackoff, with jitter. The defaults are 5 ms
	// and one second.
	ConflictBackoff    time.Duration
	MaxConflictBackoff time.Duration

	// CommitAttempts bounds retries after an unclear commit result, and
	// CommitBackoff is their first delay. The defaults are 8 and 100 ms.
	CommitAttempts int
	CommitBackoff  time.Duration

	// UploadConcurrency is how many of a commit's data files are uploaded at
	// once. The default is 8.
	UploadConcurrency int

	// PollInterval is how often the store looks for other pods' commits.
	// The default is 1s; a negative value turns following off. While the
	// table is idle it backs off to MaxPollInterval, 10s by default (F37).
	PollInterval    time.Duration
	MaxPollInterval time.Duration

	// LoadConcurrency is how many data files are read at once on start
	// (F38). The default is 16.
	LoadConcurrency int

	// Logf reports background errors. The default is log.Printf.
	Logf func(format string, args ...any)
}

func (o DeltaOptions) withDefaults() DeltaOptions {
	base := Options{Clock: o.Clock, Logf: o.Logf}.withDefaults()
	o.Clock, o.Logf = base.Clock, base.Logf
	if o.WriterID == "" {
		o.WriterID = "goalert-" + uuid.NewString()[:8]
	}
	if o.CheckpointEvery == 0 {
		o.CheckpointEvery = 100
	}
	if o.TombstoneRetention == 0 {
		o.TombstoneRetention = deltalog.DefaultTombstoneRetention
	}
	if o.ChunkGracePeriod == 0 {
		o.ChunkGracePeriod = o.TombstoneRetention
	}
	o.ChunkGracePeriod = max(o.ChunkGracePeriod, minChunkGrace)
	if o.LogRetention == 0 {
		o.LogRetention = 7 * 24 * time.Hour
	}
	if o.MaxConflicts == 0 {
		o.MaxConflicts = 20
	}
	if o.MaxBatch <= 0 {
		o.MaxBatch = 256
	}
	if o.BatchTimeout <= 0 {
		o.BatchTimeout = time.Minute
	}
	if o.ConflictBackoff <= 0 {
		o.ConflictBackoff = 5 * time.Millisecond
	}
	if o.MaxConflictBackoff < o.ConflictBackoff {
		o.MaxConflictBackoff = max(time.Second, o.ConflictBackoff)
	}
	if o.CommitAttempts <= 0 {
		o.CommitAttempts = 8
	}
	if o.CommitBackoff <= 0 {
		o.CommitBackoff = 100 * time.Millisecond
	}
	if o.UploadConcurrency <= 0 {
		o.UploadConcurrency = 8
	}
	if o.PollInterval == 0 {
		o.PollInterval = time.Second
	}
	if o.MaxPollInterval < o.PollInterval {
		o.MaxPollInterval = max(10*time.Second, o.PollInterval)
	}
	if o.LoadConcurrency <= 0 {
		o.LoadConcurrency = 16
	}
	return o
}

type bucketID struct {
	table  string
	bucket int
}

// DeltaDB is a store in a Delta table shared by several pods. Its methods
// are safe for concurrent use.
type DeltaDB struct {
	schema *Schema
	store  objstore.Store
	log    tablelog.Log
	opts   DeltaOptions
	want   deltalog.MetaData

	sem     chan struct{}         // held by the one writer, or by catching up
	snap    *deltalog.Snapshot    // writer-owned, like the fields up to view
	master  map[string]tableData  // never seen by readers
	buckets map[bucketID][]string // active data files by bucket
	// When this pod last confirmed it had every commit, by the wall clock
	// and by the monotonic one.
	currentWall, currentMono time.Time
	view                     atomic.Pointer[View]

	conflicts, batches, batched atomic.Int64

	watchMu  sync.Mutex
	watchers map[string][]chan struct{} // for Changes

	queueMu sync.Mutex
	queue   []*pendingTx // transactions waiting for a writer

	mu     sync.Mutex // guards the fields below
	failed error
	closed bool
	// denied is the last time storage refused this pod's credentials, until
	// storage works for it again (F33).
	denied        error
	deniedOnWrite bool

	stop chan struct{}
	wg   sync.WaitGroup // the follower and checkpoints
}

// CreateDelta creates the Delta table for schema in store. This is the only
// way a table comes to exist: OpenDelta never creates one, so a pod pointed
// at the wrong location can't start a second GoAlert (F27).
func CreateDelta(ctx context.Context, store objstore.Store, schema *Schema, opts DeltaOptions) error {
	opts = opts.withDefaults()
	return createBackend(ctx, store, deltalog.New(store, opts.WriterID), schema, opts)
}

// CreateBackend creates a table through log, whose implementation stores the
// commit sequence. Store holds its data files. Like CreateDelta, this is an
// explicit operation, never something opening a pod does on its own (F27).
func CreateBackend(ctx context.Context, store objstore.Store, log tablelog.Log, schema *Schema, opts DeltaOptions) error {
	opts = opts.withDefaults()
	return createBackend(ctx, store, log, schema, opts)
}

func createBackend(ctx context.Context, store objstore.Store, log tablelog.Log, schema *Schema, opts DeltaOptions) error {
	if log == nil {
		return errors.New("filestore: nil table log")
	}
	log.SetRetry(opts.CommitAttempts, opts.CommitBackoff)
	if err := schema.deltaInit(); err != nil {
		return err
	}
	meta, err := schema.deltaMetaData()
	if err != nil {
		return err
	}
	if err := objstore.SelfTest(ctx, store); err != nil {
		return err
	}
	meta.ID = uuid.NewString()
	return log.Create(ctx, meta, opts.Clock.Now())
}

// OpenDelta opens the store in the Delta table in store, which CreateDelta
// made. It returns an error matching deltalog.ErrNoTable if there is none.
func OpenDelta(ctx context.Context, store objstore.Store, schema *Schema, opts DeltaOptions) (*DeltaDB, error) {
	opts = opts.withDefaults()
	log := deltalog.New(store, opts.WriterID)
	log.Warn = func(err error) { opts.Logf("filestore: %v", err) }
	return openBackend(ctx, store, log, schema, opts)
}

// OpenBackend opens a store through log, whose implementation stores the
// commit sequence, and store, which holds its data files. DeltaDB's name is
// kept for compatibility; the commit log need not use Delta's representation.
func OpenBackend(ctx context.Context, store objstore.Store, log tablelog.Log, schema *Schema, opts DeltaOptions) (*DeltaDB, error) {
	opts = opts.withDefaults()
	return openBackend(ctx, store, log, schema, opts)
}

func openBackend(ctx context.Context, store objstore.Store, log tablelog.Log, schema *Schema, opts DeltaOptions) (*DeltaDB, error) {
	if log == nil {
		return nil, errors.New("filestore: nil table log")
	}
	log.SetRetry(opts.CommitAttempts, opts.CommitBackoff)
	if err := schema.deltaInit(); err != nil {
		return nil, err
	}
	schema.frozen = true
	want, err := schema.deltaMetaData()
	if err != nil {
		return nil, err
	}
	db := &DeltaDB{
		schema: schema, store: store, opts: opts, want: want, log: log,
		sem: make(chan struct{}, 1), stop: make(chan struct{}),
	}
	// Find broken credentials or a store that can't do what the log needs
	// now, not at the first page (F33).
	if err := objstore.SelfTest(ctx, store); err != nil {
		return nil, err
	}
	if err := db.load(ctx); err != nil {
		return nil, err
	}
	if opts.PollInterval > 0 {
		db.wg.Add(1)
		go db.follow()
	}
	return db, nil
}

// load reads the latest snapshot and every data file in it.
func (db *DeltaDB) load(ctx context.Context) error {
	snap, err := db.log.Load(ctx)
	if err != nil {
		return err
	}
	if err := checkDeltaMetaData(snap.MetaData, db.want); err != nil {
		return err
	}
	tables := make(map[string]tableData, len(db.schema.tables))
	for name, def := range db.schema.tables {
		tables[name] = def.newData()
	}
	paths := snap.FilePaths()
	buckets := make(map[bucketID][]string)
	ids := make([]bucketID, len(paths))
	for i, p := range paths {
		id, err := db.fileBucket(snap.Files[p])
		if err != nil {
			return err
		}
		ids[i] = id
		buckets[id] = append(buckets[id], p)
	}
	rows, err := db.readFiles(ctx, paths, ids, tables)
	if err != nil {
		return err
	}
	for i, id := range ids {
		if err := db.putRows(tables[id.table].(deltaData), id, rows[i]); err != nil {
			return err
		}
	}
	if db.snap != nil {
		// Reloading: it must be the same table, and never an older version.
		if snap.MetaData.ID != db.snap.MetaData.ID {
			return corruptf("the table at this location was replaced: its ID is %s, not %s (F27)", snap.MetaData.ID, db.snap.MetaData.ID)
		}
		if snap.Version < db.snap.Version {
			return corruptf("the log went back from version %d to %d", db.snap.Version, snap.Version)
		}
	}
	db.snap, db.master, db.buckets = snap, tables, buckets
	db.publish(snap.Version, tables)
	db.markCurrent()
	return nil
}

func (db *DeltaDB) markCurrent() {
	db.currentWall, db.currentMono = db.opts.Clock.Now(), time.Now()
}

// stale reports whether this pod went too long without confirming it had
// every commit. Both clocks count: a paused VM's monotonic clock stops, and
// a wall clock can jump back (F13).
func (db *DeltaDB) stale() bool {
	limit := min(db.opts.LogRetention, db.opts.TombstoneRetention) / 2
	return db.opts.Clock.Now().Sub(db.currentWall) > limit || time.Since(db.currentMono) > limit
}

// fileBucket returns the bucket a data file belongs to.
func (db *DeltaDB) fileBucket(add deltalog.Add) (bucketID, error) {
	table := add.PartitionValues[deltatable.TableColumn]
	def, ok := db.schema.tables[table]
	if !ok {
		return bucketID{}, fmt.Errorf("%w: data file %s holds table %q, which this version does not know", ErrVersion, add.Path, table)
	}
	b, err := strconv.Atoi(add.PartitionValues[deltatable.BucketColumn])
	if err != nil || b < 0 || b >= def.deltaBuckets() {
		return bucketID{}, corruptf("data file %s has bucket %q", add.Path, add.PartitionValues[deltatable.BucketColumn])
	}
	return bucketID{table: table, bucket: b}, nil
}

// readFiles reads and decodes data files, several at once (F38).
func (db *DeltaDB) readFiles(ctx context.Context, paths []string, ids []bucketID, tables map[string]tableData) ([][]deltaRow, error) {
	out := make([][]deltaRow, len(paths))
	errs := make([]error, len(paths))
	sem := make(chan struct{}, db.opts.LoadConcurrency)
	var wg sync.WaitGroup
	for i, p := range paths {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			data, err := db.store.Get(ctx, p)
			if errors.Is(err, objstore.ErrNotFound) {
				errs[i] = corruptf("data file %s is in the log but missing from storage (F6)", p)
				return
			}
			if err != nil {
				errs[i] = fmt.Errorf("filestore: read data file %s: %w", p, err)
				return
			}
			out[i], errs[i] = tables[ids[i].table].(deltaData).decodeFile(data)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// putRows adds a data file's rows to their table. Every row must belong to
// the file's bucket and appear in no other file.
func (db *DeltaDB) putRows(d deltaData, id bucketID, rows []deltaRow) error {
	n := db.schema.tables[id.table].deltaBuckets()
	for _, r := range rows {
		if deltatable.Bucket(r.key, n) != id.bucket {
			return corruptf("table %s: row %q is in bucket %d, not %d", id.table, r.key, id.bucket, deltatable.Bucket(r.key, n))
		}
		if d.hasKey(r.key) {
			return corruptf("table %s: row %q is in two data files", id.table, r.key)
		}
		if err := d.putRow(r); err != nil {
			return err
		}
	}
	return nil
}

func (db *DeltaDB) publish(version int64, changed map[string]tableData) {
	old := db.view.Load()
	tables := make(map[string]tableData, len(db.master))
	if old == nil {
		for name, d := range db.master {
			tables[name] = d.clone()
		}
	} else {
		for name, d := range old.tables {
			tables[name] = d
		}
		for name := range changed {
			tables[name] = db.master[name].clone()
		}
	}
	db.view.Store(&View{index: uint64(version), time: db.snap.Time, tables: tables})
	db.notifyChanged(changed)
}

// Changes returns a channel that gets a value whenever this pod applies a
// commit that changes table, its own or another pod's: the Delta backend's
// stand-in for a Postgres NOTIFY trigger. Sends never block the store, so
// a burst of changes may arrive as one.
func (db *DeltaDB) Changes(table string) <-chan struct{} {
	ch := make(chan struct{}, 1)
	db.watchMu.Lock()
	defer db.watchMu.Unlock()
	if db.watchers == nil {
		db.watchers = make(map[string][]chan struct{})
	}
	db.watchers[table] = append(db.watchers[table], ch)
	return ch
}

func (db *DeltaDB) notifyChanged(changed map[string]tableData) {
	db.watchMu.Lock()
	defer db.watchMu.Unlock()
	for name := range changed {
		for _, ch := range db.watchers[name] {
			select {
			case ch <- struct{}{}:
			default: // one is already waiting
			}
		}
	}
}

// Now is the time for reads outside a transaction, like now() in Postgres:
// this pod's clock, but never earlier than the latest commit it has applied
// (F13).
func (db *DeltaDB) Now() time.Time {
	t := db.opts.Clock.Now().UTC().Truncate(time.Microsecond)
	if v := db.View(); t.Before(v.time) {
		return v.time
	}
	return t
}

// View returns an immutable view of the latest version this pod has
// applied. Its Index is that version.
func (db *DeltaDB) View() *View { return db.view.Load() }

// Conflicts returns how many commits this pod lost to other pods (F40).
func (db *DeltaDB) Conflicts() int64 { return db.conflicts.Load() }

// Health returns why the store refuses writes (F21), or why storage refuses
// this pod's credentials (F33), if either is the case.
func (db *DeltaDB) Health() error {
	if err := db.writeErr(); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.denied
}

// noteStorage keeps Health red while storage refuses this pod's
// credentials. A success turns it green again: any write, or a read if a
// read was refused.
func (db *DeltaDB) noteStorage(err error, write bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	switch {
	case errors.Is(err, objstore.ErrDenied):
		if db.denied == nil {
			db.opts.Logf("filestore: storage refused this pod's credentials: %v", err)
		}
		db.denied, db.deniedOnWrite = err, write
	case err == nil && (write || !db.deniedOnWrite):
		db.denied = nil
	}
}

func (db *DeltaDB) writeErr() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	return db.failed
}

func (db *DeltaDB) fail(err error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.failed == nil {
		db.failed = err
		db.opts.Logf("filestore: refusing writes: %v", err)
	}
}

func (db *DeltaDB) lock(ctx context.Context) error {
	select {
	case db.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (db *DeltaDB) unlock() { <-db.sem }

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type pendingTx struct {
	ctx  context.Context
	fn   func(tx *Tx) error
	done chan txResult // buffered, so a result can always be delivered
}

type txResult struct {
	version int64
	err     error
}

// Update runs fn in a transaction and commits its changes on top of the
// latest version. It catches up with other pods' commits first, and if one
// still commits first, it catches up and runs fn again (F4). fn must do no
// I/O, since it can run more than once (F3).
//
// Transactions that wait for each other in one pod share a Delta commit
// (F15). Each runs on top of those before it, and one that fails is left out
// without affecting the others.
//
// Update returns the version that holds the changes; a read that must see
// them waits for it with WaitFor (F29). An error matching
// objstore.ErrUnknown means the commit may or may not have happened: report
// the outcome as unknown, never as a failure (F32). This includes giving up,
// through ctx, while the transaction is being committed. After any other
// error, nothing changed.
func (db *DeltaDB) Update(ctx context.Context, fn func(tx *Tx) error) (int64, error) {
	if err := db.writeErr(); err != nil {
		return 0, err
	}
	p := &pendingTx{ctx: ctx, fn: fn, done: make(chan txResult, 1)}
	db.queueMu.Lock()
	db.queue = append(db.queue, p)
	db.queueMu.Unlock()
	for {
		select {
		case r := <-p.done:
			return r.finish()
		case db.sem <- struct{}{}:
			// Commit whatever is waiting, this transaction included unless an
			// earlier batch took it. The batch runs on its own goroutine, so
			// this caller can still give up through ctx while it runs.
			go func() {
				defer db.unlock()
				db.commitQueued()
			}()
		case <-ctx.Done():
			if db.dequeue(p) {
				return 0, ctx.Err()
			}
			select {
			case r := <-p.done:
				return r.finish()
			default:
				return 0, objstore.Unknown(fmt.Errorf("filestore: %w while the transaction was being committed", ctx.Err()))
			}
		}
	}
}

// dequeue removes p from the queue, and reports whether it was still there,
// so that no batch has taken it.
func (db *DeltaDB) dequeue(p *pendingTx) bool {
	db.queueMu.Lock()
	defer db.queueMu.Unlock()
	i := slices.Index(db.queue, p)
	if i < 0 {
		return false
	}
	db.queue = slices.Delete(db.queue, i, i+1)
	return true
}

// commitQueued commits the transactions waiting in the queue as one batch,
// and gives each its result. The caller holds the writer lock.
func (db *DeltaDB) commitQueued() {
	db.queueMu.Lock()
	n := min(len(db.queue), db.opts.MaxBatch)
	batch := slices.Clone(db.queue[:n])
	db.queue = slices.Delete(db.queue, 0, n)
	db.queueMu.Unlock()

	live := batch[:0]
	for _, p := range batch {
		if err := p.ctx.Err(); err != nil {
			p.done <- txResult{err: err} // not started, so nothing changed
			continue
		}
		live = append(live, p)
	}
	if len(live) == 0 {
		return
	}
	// The batch serves several callers, so no single caller's cancellation
	// stops it halfway.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(live[0].ctx), db.opts.BatchTimeout)
	defer cancel()
	version, errs, err := db.commitBatch(ctx, live)
	for i, p := range live {
		switch {
		case err != nil:
			p.done <- txResult{err: err}
		case errs[i] != nil:
			p.done <- txResult{err: errs[i]}
		default:
			p.done <- txResult{version: version}
		}
	}
	db.batches.Add(1)
	db.batched.Add(int64(len(live)))
}

// commitBatch runs the transactions, each on top of those before it that
// succeeded, and commits their changes as one Delta commit. errs holds each
// transaction's own error; err fails them all.
func (db *DeltaDB) commitBatch(ctx context.Context, batch []*pendingTx) (version int64, errs []error, err error) {
	if err := db.writeErr(); err != nil {
		return 0, nil, err
	}
	delay := db.opts.ConflictBackoff
	uploads := make(map[bucketID]uploaded)
	for attempt := 1; ; attempt++ {
		if err := db.catchUp(ctx); err != nil {
			return 0, nil, err
		}
		now := db.commitTime()
		base := maps.Clone(db.master)
		working := make(map[string]tableData)
		var muts []mutation
		errs = make([]error, len(batch))
		for i, p := range batch {
			tx := &Tx{schema: db.schema, base: base, working: make(map[string]tableData), now: now}
			if err := runTx(p.fn, tx); err != nil {
				if pe, ok := err.(*TxPanic); ok {
					db.opts.Logf("filestore: a transaction panicked: %v", pe.Value)
				}
				errs[i] = err
				continue
			}
			for name, d := range tx.working {
				base[name], working[name] = d, d
			}
			muts = append(muts, tx.muts...)
		}

		all := &Tx{schema: db.schema, base: db.master, working: working, muts: muts, now: now, done: true}
		actions, files, err := db.prepare(ctx, all, uploads)
		if err != nil {
			db.noteStorage(err, true)
			return 0, nil, err
		}
		if len(actions) == 0 {
			return db.snap.Version, errs, nil
		}

		version := db.snap.Version + 1
		info, err := db.log.Commit(ctx, version, db.snap.MetaData.ID, "WRITE", now, actions)
		db.noteStorage(err, true)
		if err == nil {
			db.committed(version, info, actions, files, working)
			return version, errs, nil
		}
		if !errors.Is(err, deltalog.ErrConflict) {
			// A definite failure, or an unknown outcome that the next catch
			// up settles by reading the version.
			db.failIfFatal(err)
			return 0, nil, err
		}
		db.conflicts.Add(1)
		if attempt >= db.opts.MaxConflicts {
			return 0, nil, fmt.Errorf("%w (%d attempts)", ErrConflicts, attempt)
		}
		// The last attempt lost to another commit, so if this wait is cut
		// short, nothing of the batch is in the log.
		if err := sleepCtx(ctx, delay/2+rand.N(delay)); err != nil {
			return 0, nil, err
		}
		delay = min(2*delay, db.opts.MaxConflictBackoff)
	}
}

// Batches returns how many commits this pod made from queued transactions,
// and how many transactions they held (F15).
func (db *DeltaDB) Batches() (commits, transactions int64) {
	return db.batches.Load(), db.batched.Load()
}

// commitTime is the next transaction's time: the clock, but never earlier
// than the latest commit, whichever pod's clock that came from (F13).
func (db *DeltaDB) commitTime() time.Time {
	t := db.opts.Clock.Now().UTC().Truncate(time.Microsecond)
	if t.Before(db.snap.Time) {
		t = db.snap.Time
	}
	return t
}

// uploaded is a data file that no commit has added yet.
type uploaded struct {
	digest [sha256.Size]byte
	add    deltalog.Add
}

// prepare uploads a new data file for every bucket the transaction changed,
// before the commit that adds it (F6), and returns the commit's actions and
// each changed bucket's new files.
//
// uploads holds the files of the batch's attempts that lost to another pod's
// commit. A bucket that comes out the same again, as most do when pods change
// different rows, keeps its file, so a lost race costs little (F37, F40).
func (db *DeltaDB) prepare(ctx context.Context, tx *Tx, uploads map[bucketID]uploaded) ([]deltalog.Action, map[bucketID][]string, error) {
	touched := make(map[bucketID]bool)
	for _, m := range tx.muts {
		n := db.schema.tables[m.table].deltaBuckets()
		touched[bucketID{table: m.table, bucket: deltatable.Bucket(m.key, n)}] = true
	}
	ids := make([]bucketID, 0, len(touched))
	for id := range touched {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		if ids[i].table != ids[j].table {
			return ids[i].table < ids[j].table
		}
		return ids[i].bucket < ids[j].bucket
	})

	adds, err := db.upload(ctx, tx, ids, uploads)
	if err != nil {
		return nil, nil, err
	}
	var actions []deltalog.Action
	files := make(map[bucketID][]string, len(ids))
	for i, id := range ids {
		files[id] = nil
		if add := adds[i]; add != nil {
			actions = append(actions, deltalog.Action{Add: add})
			files[id] = []string{add.Path}
		}
		for _, p := range db.buckets[id] {
			old := db.snap.Files[p]
			actions = append(actions, deltalog.Action{Remove: &deltalog.Remove{
				Path: p, DeletionTimestamp: tx.now.UnixMilli(), DataChange: true,
				ExtendedFileMetadata: true, PartitionValues: old.PartitionValues, Size: old.Size,
			}})
		}
	}
	return actions, files, nil
}

// upload uploads the buckets' data files, several at once (F15). It returns one
// for each bucket, or nil for an empty one.
func (db *DeltaDB) upload(ctx context.Context, tx *Tx, ids []bucketID, uploads map[bucketID]uploaded) ([]*deltalog.Add, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	adds := make([]*deltalog.Add, len(ids))
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		first error
	)
	fail := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if first == nil {
			first = err
			cancel()
		}
	}
	sem := make(chan struct{}, db.opts.UploadConcurrency)
	for i, id := range ids {
		// Encoded only once it can be uploaded, so that no more files than are
		// uploaded at once are in memory (F17).
		sem <- struct{}{}
		if ctx.Err() != nil {
			<-sem
			break
		}
		data, n, err := tx.tableData(id.table).(deltaData).encodeBucket(id.bucket)
		if err != nil {
			fail(fmt.Errorf("filestore: %s: %w", id.table, err))
		}
		digest := sha256.Sum256(data)
		mu.Lock()
		prior, ok := uploads[id]
		mu.Unlock()
		switch {
		case err != nil || n == 0:
			<-sem
			continue
		case ok && prior.digest == digest:
			adds[i] = &prior.add
			<-sem
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			add, err := deltatable.Upload(ctx, db.store, id.table, id.bucket, data, n, tx.now)
			if err != nil {
				fail(err)
				return
			}
			adds[i] = &add
			mu.Lock()
			defer mu.Unlock()
			uploads[id] = uploaded{digest: digest, add: add}
		}()
	}
	wg.Wait()
	if first == nil {
		first = ctx.Err()
	}
	return adds, first
}

// failIfFatal stops writes if an error means that this pod may not write to
// the table, or can't: it was written to by something else (F11), it is
// damaged, or a newer version's (F30).
func (db *DeltaDB) failIfFatal(err error) {
	for _, fatal := range []error{
		ErrCorrupt, ErrVersion, deltalog.ErrForeign, deltalog.ErrCorrupt, deltalog.ErrUnsupported, deltalog.ErrManaged,
	} {
		if errors.Is(err, fatal) {
			db.fail(err)
			return
		}
	}
}

// committed applies this pod's own commit, whose rows are already in
// working.
func (db *DeltaDB) committed(version int64, info deltalog.CommitInfo, actions []deltalog.Action, files map[bucketID][]string, working map[string]tableData) {
	if err := db.snap.Apply(version, info, actions); err != nil {
		// Unreachable unless something is badly wrong: stop writing (F21).
		db.fail(fmt.Errorf("apply this pod's commit %d: %w", version, err))
		return
	}
	db.setFiles(files)
	for name, d := range working {
		db.master[name] = d
	}
	db.publish(version, working)
	db.maybeCheckpoint()
}

func (db *DeltaDB) setFiles(files map[bucketID][]string) {
	for id, paths := range files {
		if len(paths) == 0 {
			delete(db.buckets, id)
		} else {
			db.buckets[id] = paths
		}
	}
}

// catchUp applies every commit after this pod's version. A commit it cannot
// apply, such as one from another writer (F11) or a newer version (F30),
// stops writes.
func (db *DeltaDB) catchUp(ctx context.Context) error {
	if db.stale() {
		// The commits after this pod's version may have been cleaned up, so
		// reading on could take a missing one for the end of the log, and
		// the next commit could write that version again (F16).
		if err := db.load(ctx); err != nil {
			db.failIfFatal(err)
			db.noteStorage(err, false)
			return err
		}
	}
	for {
		version := db.snap.Version + 1
		info, actions, err := db.log.Read(ctx, version)
		if errors.Is(err, objstore.ErrNotFound) {
			db.noteStorage(nil, false)
			db.markCurrent()
			return nil
		}
		db.noteStorage(err, false)
		if err != nil {
			db.failIfFatal(err)
			return err
		}
		if err := db.apply(ctx, version, info, actions); err != nil {
			db.noteStorage(err, false)
			db.failIfFatal(err)
			return err
		}
	}
}

// apply applies another pod's commit: it reads the new files of every
// bucket the commit changed and replaces those buckets' rows. Nothing
// changes unless the whole commit applies (F10).
func (db *DeltaDB) apply(ctx context.Context, version int64, info deltalog.CommitInfo, actions []deltalog.Action) error {
	newFiles := make(map[bucketID][]string)
	touch := func(id bucketID) {
		if _, ok := newFiles[id]; !ok {
			newFiles[id] = slices.Clone(db.buckets[id])
		}
	}
	for _, a := range actions {
		switch {
		case a.MetaData != nil:
			if err := checkDeltaMetaData(*a.MetaData, db.want); err != nil {
				return err
			}
		case a.Remove != nil:
			old, ok := db.snap.Files[a.Remove.Path]
			if !ok {
				continue // Apply below refuses it
			}
			id, err := db.fileBucket(old)
			if err != nil {
				return err
			}
			touch(id)
			newFiles[id] = slices.DeleteFunc(newFiles[id], func(p string) bool { return p == a.Remove.Path })
		}
	}
	for _, a := range actions {
		if a.Add == nil {
			continue
		}
		id, err := db.fileBucket(*a.Add)
		if err != nil {
			return err
		}
		touch(id)
		newFiles[id] = append(newFiles[id], a.Add.Path)
	}

	// Read every file of each changed bucket.
	var paths []string
	var ids []bucketID
	for id, ps := range newFiles {
		for _, p := range ps {
			paths, ids = append(paths, p), append(ids, id)
		}
	}
	rows, err := db.readFiles(ctx, paths, ids, db.master)
	if err != nil {
		return err
	}

	// Replace the changed buckets' rows: remove them all first, so a unique
	// key that moved between buckets is free before it is put again.
	working := make(map[string]tableData)
	writable := func(table string) deltaData {
		d, ok := working[table]
		if !ok {
			d = db.master[table].clone()
			working[table] = d
		}
		return d.(deltaData)
	}
	for id := range newFiles {
		d := writable(id.table)
		for _, k := range d.bucketKeys(id.bucket) {
			d.removeKey(k)
		}
	}
	for i, id := range ids {
		if err := db.putRows(writable(id.table), id, rows[i]); err != nil {
			return err
		}
	}

	if err := db.snap.Apply(version, info, actions); err != nil {
		return err
	}
	db.setFiles(newFiles)
	for name, d := range working {
		db.master[name] = d
	}
	db.publish(version, working)
	return nil
}

// WaitFor returns once this pod has applied version, catching up if it has
// not. A read that must see a write, perhaps made by another pod, waits for
// the version Update returned (F29).
func (db *DeltaDB) WaitFor(ctx context.Context, version int64) error {
	if int64(db.View().Index()) >= version {
		return nil
	}
	if err := db.lock(ctx); err != nil {
		return err
	}
	defer db.unlock()
	if err := db.catchUp(ctx); err != nil {
		return err
	}
	if int64(db.View().Index()) < version {
		return fmt.Errorf("filestore: version %d is not in the log yet", version)
	}
	return nil
}

// follow applies other pods' commits in the background, polling less often
// while the table is idle (F37).
func (db *DeltaDB) follow() {
	defer db.wg.Done()
	interval := db.opts.PollInterval
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-db.stop:
			return
		case <-timer.C:
		}
		before := db.View().Index()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		err := db.lock(ctx)
		if err == nil {
			if db.writeErr() == nil {
				err = db.catchUp(ctx)
			}
			db.unlock()
		}
		cancel()
		switch {
		case err != nil:
			db.opts.Logf("filestore: following the Delta log: %v", err)
			interval = db.opts.PollInterval
		case db.View().Index() == before:
			interval = min(2*interval, db.opts.MaxPollInterval)
		default:
			interval = db.opts.PollInterval
		}
		timer.Reset(interval)
	}
}

// maybeCheckpoint writes a checkpoint in the background after every
// CheckpointEvery versions this pod commits (F16, F38).
func (db *DeltaDB) maybeCheckpoint() {
	every := db.opts.CheckpointEvery
	if every <= 0 || db.snap.Version%every != 0 {
		return
	}
	snap := db.snap.Clone()
	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if err := db.log.WriteCheckpoint(ctx, snap, db.opts.Clock.Now(), db.opts.TombstoneRetention); err != nil {
			db.opts.Logf("filestore: checkpoint %d: %v", snap.Version, err)
		}
	}()
}

// Vacuum deletes data files no reader needs any more (F6, F16): those
// removed longer ago than the tombstone retention, and those no commit
// refers to that were uploaded longer ago than that, such as the files of a
// transaction that lost a race. A file whose name has no upload time is
// kept. It returns how many files it deleted.
func (db *DeltaDB) Vacuum(ctx context.Context) (int, error) {
	if err := db.lock(ctx); err != nil {
		return 0, err
	}
	if err := db.catchUp(ctx); err != nil {
		db.unlock()
		return 0, err
	}
	cutoff := db.opts.Clock.Now().Add(-db.opts.TombstoneRetention)
	referenced := make(map[string]bool, len(db.snap.Files)+len(db.snap.Removed))
	for p := range db.snap.Files {
		referenced[p] = true
	}
	expired := make(map[string]bool)
	for p, r := range db.snap.Removed {
		if r.DeletionTimestamp != 0 && time.UnixMilli(r.DeletionTimestamp).Before(cutoff) {
			expired[p] = true
		} else {
			referenced[p] = true
		}
	}
	db.unlock()

	// Files other pods add from here on are newer than the cutoff, and files
	// they remove are tombstoned now, so both are kept.
	names, err := db.store.List(ctx, deltatable.TableColumn+"=", "")
	if err != nil {
		db.noteStorage(err, false)
		return 0, err
	}
	deleted := 0
	for _, name := range names {
		if referenced[name] {
			continue
		}
		if !expired[name] {
			// No commit refers to it. Delete it only once no transaction can
			// still be about to commit it.
			if at, ok := deltatable.FileTime(name); !ok || !at.Before(cutoff) {
				continue
			}
		}
		if err := db.store.Delete(ctx, name); err != nil {
			db.noteStorage(err, true)
			return deleted, err
		}
		deleted++
	}
	if err := db.collectChunks(ctx); err != nil {
		db.noteStorage(err, true)
		return deleted, err
	}
	return deleted, nil
}

// CleanLog deletes the commits and checkpoints older than the log retention
// that no reader needs (F16). It returns how many files it deleted.
func (db *DeltaDB) CleanLog(ctx context.Context) (int, error) {
	n, err := db.log.Cleanup(ctx, db.opts.Clock.Now(), db.opts.LogRetention)
	if err != nil {
		db.noteStorage(err, true)
		return n, err
	}
	if err := db.collectChunks(ctx); err != nil {
		db.noteStorage(err, true)
		return n, err
	}
	return n, nil
}

// collectChunks lets a chunked object store remove old, unreferenced chunks.
// Other object stores have nothing extra to do after their objects are deleted.
func (db *DeltaDB) collectChunks(ctx context.Context) error {
	gc, ok := db.store.(interface {
		CollectGarbage(ctx context.Context, gracePeriod time.Duration) (int, error)
	})
	if !ok {
		return nil
	}
	_, err := gc.CollectGarbage(ctx, db.opts.ChunkGracePeriod)
	return err
}

// Close stops following the log and waits for background work. Writes in
// progress finish first.
func (db *DeltaDB) Close() error {
	if err := db.lock(context.Background()); err != nil {
		return err
	}
	db.mu.Lock()
	wasClosed := db.closed
	db.closed = true
	db.mu.Unlock()
	db.unlock()
	if wasClosed {
		return nil
	}
	close(db.stop)
	db.wg.Wait()
	return nil
}

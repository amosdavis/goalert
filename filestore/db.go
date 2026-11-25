package filestore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	formatName    = "FORMAT"
	lockName      = "LOCK"
	formatVersion = 1
)

// Options configure a store. The zero value is ready to use.
type Options struct {
	// Clock supplies transaction timestamps. The default is the system clock.
	Clock Clock

	// SegmentSize is the log file size at which a new file is started. The
	// default is 64 MiB.
	SegmentSize int64

	// SnapshotEvery is the number of commits after which a snapshot is
	// written in the background. The default is 10,000; a negative value
	// turns it off.
	SnapshotEvery int

	// SnapshotBytes is the amount of log written after which a snapshot is
	// written in the background. The default is 256 MiB; a negative value
	// turns it off.
	SnapshotBytes int64

	// Logf reports recovery actions and background errors. The default is
	// log.Printf.
	Logf func(format string, args ...any)

	fs           fileSystem // tests only
	skipPlatform bool       // tests only: no locking or filesystem checks
}

func (o Options) withDefaults() Options {
	if o.Clock == nil {
		o.Clock = RealClock()
	}
	if o.SegmentSize == 0 {
		o.SegmentSize = 64 << 20
	}
	if o.SnapshotEvery == 0 {
		o.SnapshotEvery = 10000
	}
	if o.SnapshotBytes == 0 {
		o.SnapshotBytes = 256 << 20
	}
	if o.Logf == nil {
		o.Logf = log.Printf
	}
	if o.fs == nil {
		o.fs = osFS{}
	}
	return o
}

// View is an immutable, consistent view of the store as of one commit.
type View struct {
	index  uint64
	time   time.Time // of the last commit, on a Delta table
	tables map[string]tableData
}

func (v *View) tableData(name string) tableData { return v.tables[name] }

// Index returns the log index of the last commit included in the view.
func (v *View) Index() uint64 { return v.index }

// DB is an open store. Its methods are safe for concurrent use.
type DB struct {
	dir     string
	schema  *Schema
	opts    Options
	fs      fileSystem
	storeID [16]byte
	lock    io.Closer
	wal     *wal

	sem    chan struct{}        // held by the one writer
	master map[string]tableData // writer-owned; never seen by readers
	view   atomic.Pointer[View]

	mu          sync.Mutex // guards the fields below
	failed      error
	closed      bool
	lastSnap    uint64
	sinceSnapN  int
	sinceSnapB  int64
	snapRunning bool

	snapMu sync.Mutex // one snapshot at a time
	snapWG sync.WaitGroup
}

// Open opens the store in dir, creating it if dir is empty or missing.
//
// Open takes an exclusive lock on dir (F11), refuses network filesystems
// (F11), restricts dir to its owner (F20), then loads the newest valid
// snapshot and replays the log after it.
func Open(dir string, schema *Schema, opts Options) (_ *DB, err error) {
	if schema.delta {
		return nil, errors.New("filestore: this schema is in use by a Delta table")
	}
	opts = opts.withDefaults()
	schema.frozen = true
	db := &DB{
		dir:    dir,
		schema: schema,
		opts:   opts,
		fs:     opts.fs,
		sem:    make(chan struct{}, 1),
	}

	if err := db.fs.MkdirAll(dir); err != nil {
		return nil, err
	}
	if !opts.skipPlatform {
		if err := checkLocalFS(dir); err != nil {
			return nil, err
		}
		if err := restrictDir(dir); err != nil {
			return nil, err
		}
		db.lock, err = lockFile(filepath.Join(dir, lockName))
		if err != nil {
			return nil, err
		}
		defer func() {
			if err != nil {
				_ = db.lock.Close()
			}
		}()
	}

	if err := db.removeTemp(); err != nil {
		return nil, err
	}
	db.storeID, err = loadOrInitFormat(db.fs, dir)
	if err != nil {
		return nil, err
	}

	snapIndex, tables, found, err := loadSnapshot(db.fs, dir, db.storeID, schema, opts.Logf)
	if err != nil {
		return nil, err
	}
	if !found {
		tables = make(map[string]tableData, len(schema.names))
		for _, name := range schema.names {
			tables[name] = schema.tables[name].newData()
		}
	}

	db.wal, err = openWAL(db.fs, dir, db.storeID, opts.SegmentSize, snapIndex, opts.Logf, func(index uint64, payload []byte) error {
		if index <= snapIndex {
			return nil // already in the snapshot
		}
		return applyPayload(tables, index, payload)
	})
	if err != nil {
		return nil, err
	}
	if first := db.wal.firstIndex(); first > snapIndex+1 {
		_ = db.wal.close()
		return nil, corruptf("the log starts at index %d, but the newest usable snapshot ends at %d", first, snapIndex)
	}
	if db.wal.lastIndex < snapIndex {
		_ = db.wal.close()
		return nil, corruptf("the log ends at index %d, before the snapshot at %d", db.wal.lastIndex, snapIndex)
	}

	db.master = tables
	db.lastSnap = snapIndex
	db.publish(db.wal.lastIndex, nil)
	return db, nil
}

// applyPayload applies one replayed log record to tables, which nothing else
// can see yet.
func applyPayload(tables map[string]tableData, index uint64, payload []byte) error {
	got, _, muts, err := decodePayload(payload)
	if err != nil {
		return err
	}
	if got != index {
		return corruptf("log record %d says it is %d", index, got)
	}
	for _, m := range muts {
		d, ok := tables[m.table]
		if !ok {
			return fmt.Errorf("%w: the log has table %q, which this version of GoAlert does not know (was it written by a newer version?)", ErrVersion, m.table)
		}
		switch m.op {
		case opPut:
			err = d.applyPut(m.key, m.val)
		case opDelete:
			err = d.applyDelete(m.key)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// removeTemp removes temporary files left by a crash.
func (db *DB) removeTemp() error {
	names, err := db.fs.ReadDir(db.dir)
	if err != nil {
		return err
	}
	for _, n := range names {
		if strings.HasSuffix(n, tmpExt) {
			if err := db.fs.Remove(filepath.Join(db.dir, n)); err != nil {
				return err
			}
		}
	}
	return nil
}

type formatFile struct {
	Format  int
	StoreID string
}

// loadOrInitFormat reads the FORMAT file, or creates it if the directory is
// empty. A non-empty directory without one is refused, so that pointing
// GoAlert at the wrong directory cannot damage it.
func loadOrInitFormat(fsys fileSystem, dir string) (id [16]byte, err error) {
	path := filepath.Join(dir, formatName)
	data, err := fsys.ReadFile(path)
	if err == nil {
		var f formatFile
		if err := json.Unmarshal(data, &f); err != nil {
			return id, corruptf("FORMAT file: %v", err)
		}
		if f.Format != formatVersion {
			return id, fmt.Errorf("%w: data directory format %d (this version of GoAlert reads %d)", ErrVersion, f.Format, formatVersion)
		}
		b, err := hex.DecodeString(f.StoreID)
		if err != nil || len(b) != len(id) {
			return id, corruptf("FORMAT file has an invalid store ID")
		}
		copy(id[:], b)
		return id, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return id, err
	}

	names, err := fsys.ReadDir(dir)
	if err != nil {
		return id, err
	}
	for _, n := range names {
		if n != lockName {
			return id, fmt.Errorf("%w: %s (found %s)", ErrNotStore, dir, n)
		}
	}
	if _, err := rand.Read(id[:]); err != nil {
		return id, err
	}
	data, err = json.Marshal(formatFile{Format: formatVersion, StoreID: hex.EncodeToString(id[:])})
	if err != nil {
		return id, err
	}
	return id, writeFileAtomic(fsys, dir, formatName, data)
}

// writeFileAtomic writes a small file through a temporary file and a rename,
// so it is either fully there or not there at all.
func writeFileAtomic(fsys fileSystem, dir, name string, data []byte) error {
	tmp := filepath.Join(dir, name+tmpExt)
	f, err := fsys.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := fsys.Rename(tmp, filepath.Join(dir, name)); err != nil {
		return err
	}
	return fsys.SyncDir(dir)
}

// publish makes the writer's tables visible to readers as of index. Only the
// tables a commit changed are cloned; the rest are shared with the previous
// view.
func (db *DB) publish(index uint64, changed map[string]tableData) {
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
	db.view.Store(&View{index: index, tables: tables})
}

// View returns an immutable view of the latest commit.
func (db *DB) View() *View { return db.view.Load() }

// writeErr returns why writes are refused, if they are.
func (db *DB) writeErr() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.failed != nil {
		return fmt.Errorf("filestore: refusing writes after an earlier error; restart to recover: %w", db.failed)
	}
	return nil
}

// Health returns an error if the store cannot accept writes (F21).
func (db *DB) Health() error { return db.writeErr() }

// Update runs fn as the only writer. If fn returns nil, its changes are
// appended to the log and fsynced before Update returns, and only then become
// visible to readers (F1). If fn returns an error, or the log write fails,
// nothing changes.
//
// If the log write fails, the store refuses all later writes until it is
// reopened (F21). The failed transaction may still be found on disk at that
// point, as with a database whose connection drops during COMMIT.
func (db *DB) Update(ctx context.Context, fn func(tx *Tx) error) error {
	select {
	case db.sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-db.sem }()

	if err := db.writeErr(); err != nil {
		return err
	}

	tx := &Tx{
		schema:  db.schema,
		base:    db.master,
		working: make(map[string]tableData),
		now:     db.opts.Clock.Now().UTC().Truncate(time.Microsecond),
	}
	err := fn(tx)
	tx.done = true
	if err != nil {
		return err
	}
	if len(tx.muts) == 0 {
		return nil
	}

	index := db.view.Load().index + 1
	payload := encodePayload(index, tx.now, tx.muts)
	if err := db.wal.append(index, payload); err != nil {
		db.mu.Lock()
		if db.failed == nil {
			db.failed = err
		}
		db.mu.Unlock()
		db.opts.Logf("filestore: %v", err)
		return err
	}

	for name, d := range tx.working {
		db.master[name] = d
	}
	db.publish(index, tx.working)
	db.afterCommit(len(payload))
	return nil
}

func (db *DB) afterCommit(n int) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.sinceSnapN++
	db.sinceSnapB += int64(n)
	if db.snapRunning || db.closed {
		return
	}
	due := (db.opts.SnapshotEvery > 0 && db.sinceSnapN >= db.opts.SnapshotEvery) ||
		(db.opts.SnapshotBytes > 0 && db.sinceSnapB >= db.opts.SnapshotBytes)
	if !due {
		return
	}
	db.snapRunning = true
	db.snapWG.Add(1)
	go func() {
		defer db.snapWG.Done()
		if err := db.Snapshot(); err != nil {
			db.opts.Logf("filestore: background snapshot: %v", err)
		}
		db.mu.Lock()
		db.snapRunning = false
		db.mu.Unlock()
	}()
}

// Snapshot writes a snapshot of the latest commit, then removes snapshots and
// log segments that are no longer needed. The newest two snapshots are kept,
// with the log needed to replay from the older one, so one damaged snapshot
// never loses data. Writers keep going while it runs.
func (db *DB) Snapshot() error {
	db.snapMu.Lock()
	defer db.snapMu.Unlock()

	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	v := db.view.Load()
	if v.index == db.lastSnap {
		db.mu.Unlock()
		return nil
	}
	db.sinceSnapN, db.sinceSnapB = 0, 0
	db.mu.Unlock()

	if err := writeSnapshot(db.fs, db.dir, db.storeID, db.schema, v, db.opts.Clock.Now()); err != nil {
		return fmt.Errorf("write snapshot: %w", err)
	}
	db.mu.Lock()
	db.lastSnap = v.index
	db.mu.Unlock()
	return db.compact()
}

func (db *DB) compact() error {
	idx, err := listSnapshots(db.fs, db.dir)
	if err != nil {
		return err
	}
	if len(idx) < 2 {
		return nil
	}
	older := idx[len(idx)-2]
	for _, i := range idx[:len(idx)-2] {
		if err := db.fs.Remove(filepath.Join(db.dir, snapshotName(i))); err != nil {
			return err
		}
	}
	if err := db.fs.SyncDir(db.dir); err != nil {
		return err
	}
	return db.wal.removeThrough(older)
}

// Backup writes a consistent copy of the latest commit to w (F9). Restore
// turns it back into a data directory.
func (db *DB) Backup(w io.Writer) error {
	return encodeSnapshot(w, db.storeID, db.schema, db.view.Load(), db.opts.Clock.Now())
}

// Close waits for the current writer and any snapshot to finish, then closes
// the store. Later writes fail with ErrClosed.
func (db *DB) Close() error {
	db.sem <- struct{}{} // wait for the writer
	db.mu.Lock()
	already := db.closed
	db.closed = true
	db.mu.Unlock()
	<-db.sem // writers waiting behind us now see ErrClosed
	if already {
		return nil
	}

	db.snapWG.Wait()
	err := db.wal.close()
	if db.lock != nil {
		if lerr := db.lock.Close(); err == nil {
			err = lerr
		}
	}
	return err
}

// Restore creates a data directory in dir, which must be empty or missing,
// from a backup written by Backup. The new store continues from the backup's
// last commit.
func Restore(dir string, schema *Schema, backup io.Reader) error {
	return restore(dir, schema, backup, osFS{}, false)
}

func restore(dir string, schema *Schema, backup io.Reader, fsys fileSystem, skipPlatform bool) error {
	data, err := io.ReadAll(backup)
	if err != nil {
		return err
	}
	if len(data) < 12+16 || string(data[:8]) != snapMagic {
		return corruptf("not a backup")
	}
	var id [16]byte
	copy(id[:], data[12:28])
	index, _, err := decodeSnapshot(data, id, schema) // validates everything
	if err != nil {
		return err
	}

	if err := fsys.MkdirAll(dir); err != nil {
		return err
	}
	var lock io.Closer
	if !skipPlatform {
		if err := checkLocalFS(dir); err != nil {
			return err
		}
		if err := restrictDir(dir); err != nil {
			return err
		}
		lock, err = lockFile(filepath.Join(dir, lockName))
		if err != nil {
			return err
		}
		defer lock.Close()
	}
	names, err := fsys.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, n := range names {
		if n != lockName {
			return fmt.Errorf("%w: restore target %s is not empty (found %s)", ErrNotStore, dir, n)
		}
	}

	if err := writeFileAtomic(fsys, dir, snapshotName(index), data); err != nil {
		return err
	}
	f, err := json.Marshal(formatFile{Format: formatVersion, StoreID: hex.EncodeToString(id[:])})
	if err != nil {
		return err
	}
	// FORMAT goes last: until it exists, the directory is not a store.
	return writeFileAtomic(fsys, dir, formatName, f)
}

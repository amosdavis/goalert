package iceberglog

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"math"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/tablelog"
)

// shards is how many manifests list the table's data files. A file's shard
// depends on its directory, that is, its GoAlert table and bucket, so a commit
// that rewrites a bucket rewrites one small manifest, and the number of
// manifests never grows (F46).
const shards = 32

// The metadata files this package writes. Their names have their time, so that
// Cleanup can tell how old they are.
const metadataPrefix = "metadata/goalert-"

// minMetadataAge is how long a metadata file that no snapshot uses is kept at
// least, since a commit in progress may be about to use it (F6).
const minMetadataAge = time.Hour

var (
	manifestName = regexp.MustCompile(`^metadata/goalert-(\d{13,})-[0-9a-f-]{36}-m(\d{2})\.avro$`)
	listName     = regexp.MustCompile(`^metadata/goalert-(\d{13,})-[0-9a-f-]{36}-list\.avro$`)
)

func shardOf(dataFile string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(path.Dir(dataFile)))
	return int(h.Sum32() % shards)
}

// uri is where Iceberg readers find an object.
func (l *Log) uri(name string) string { return l.location + "/" + name }

// name is the object at a URI.
func (l *Log) name(uri string) (string, error) {
	name, ok := strings.CutPrefix(uri, l.location+"/")
	if !ok {
		return "", fmt.Errorf("%w: the Iceberg table has the file %s, which is not under %s", tablelog.ErrForeign, uri, l.location)
	}
	return name, nil
}

func checkDataFile(name string) error {
	if err := objstore.CheckName(name); err != nil {
		return err
	}
	if path.Clean(name) != name || strings.HasPrefix(name, "../") || strings.ContainsAny(name, "%?#") {
		return fmt.Errorf("iceberglog: invalid data file name %q", name)
	}
	return nil
}

// metadataFile reads an immutable metadata file.
func (l *Log) metadataFile(ctx context.Context, uri string) ([]byte, error) {
	name, err := l.name(uri)
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	data, ok := l.files[name]
	l.mu.Unlock()
	if ok {
		return data, nil
	}
	data, err = l.cfg.Store.Get(ctx, name)
	if errors.Is(err, objstore.ErrNotFound) {
		return nil, fmt.Errorf("%w: the Iceberg table's %s is missing (F6)", tablelog.ErrCorrupt, name)
	}
	if err != nil {
		return nil, fmt.Errorf("iceberglog: read %s: %w", name, err)
	}
	l.mu.Lock()
	l.files[name] = data
	l.mu.Unlock()
	return data, nil
}

// manifests returns a snapshot's manifests by shard.
func (l *Log) manifests(ctx context.Context, snap *table.Snapshot) (map[int]iceberg.ManifestFile, error) {
	out := make(map[int]iceberg.ManifestFile)
	if snap == nil {
		return out, nil
	}
	data, err := l.metadataFile(ctx, snap.ManifestList)
	if err != nil {
		return nil, err
	}
	list, err := iceberg.ReadManifestList(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("%w: manifest list %s: %v", tablelog.ErrCorrupt, snap.ManifestList, err)
	}
	for _, m := range list {
		name, err := l.name(m.FilePath())
		if err != nil {
			return nil, err
		}
		match := manifestName.FindStringSubmatch(name)
		if match == nil {
			return nil, fmt.Errorf("%w: the Iceberg table has the manifest %s", tablelog.ErrForeign, name)
		}
		shard, _ := strconv.Atoi(match[2])
		if _, dup := out[shard]; dup || shard >= shards {
			return nil, fmt.Errorf("%w: two manifests of shard %d, or one too many", tablelog.ErrCorrupt, shard)
		}
		out[shard] = m
	}
	return out, nil
}

// entries returns the data files a manifest lists.
func (l *Log) entries(ctx context.Context, m iceberg.ManifestFile) ([]iceberg.ManifestEntry, error) {
	data, err := l.metadataFile(ctx, m.FilePath())
	if err != nil {
		return nil, err
	}
	entries, err := iceberg.ReadManifest(m, bytes.NewReader(data), true)
	if err != nil {
		return nil, fmt.Errorf("%w: manifest %s: %v", tablelog.ErrCorrupt, m.FilePath(), err)
	}
	return entries, nil
}

// each calls fn for every item, several at once, and returns the first error.
func each[T any](ctx context.Context, items []T, fn func(ctx context.Context, i int, item T) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var once sync.Once
	var first error
	sem := make(chan struct{}, 8)
	for i, item := range items {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := fn(ctx, i, item); err != nil {
				once.Do(func() { first = err; cancel() })
			}
		}()
	}
	wg.Wait()
	return first
}

// verify checks that a snapshot lists exactly the data files of GoAlert's
// state, so that what Iceberg readers see is what GoAlert has (F47).
func (l *Log) verify(ctx context.Context, h *head, s *tablelog.Snapshot) error {
	byShard, err := l.manifests(ctx, h.snapshot)
	if err != nil {
		return err
	}
	ms := slices.Collect(maps.Values(byShard))
	entries := make([][]iceberg.ManifestEntry, len(ms))
	err = each(ctx, ms, func(ctx context.Context, i int, m iceberg.ManifestFile) (err error) {
		entries[i], err = l.entries(ctx, m)
		return err
	})
	if err != nil {
		return err
	}
	n := 0
	for _, es := range entries {
		for _, e := range es {
			name, err := l.name(e.DataFile().FilePath())
			if err != nil {
				return err
			}
			add, ok := s.Files[name]
			if !ok {
				return fmt.Errorf("%w: Iceberg has the data file %s in version %d, but GoAlert does not (F47)", tablelog.ErrCorrupt, name, h.version)
			}
			if e.DataFile().FileSizeBytes() != add.Size {
				return fmt.Errorf("%w: Iceberg has the data file %s with %d bytes, not %d (F47)", tablelog.ErrCorrupt, name, e.DataFile().FileSizeBytes(), add.Size)
			}
			n++
		}
	}
	if n != len(s.Files) {
		return fmt.Errorf("%w: Iceberg has %d data files in version %d, but GoAlert has %d (F47)", tablelog.ErrCorrupt, n, h.version, len(s.Files))
	}
	return nil
}

// stage stores everything a commit needs before the catalog is asked to make
// it: the commit itself, the manifests of the shards it changes, and the list
// of all manifests. It returns the snapshot to add, and its metadata files.
func (l *Log) stage(ctx context.Context, h *head, info tablelog.CommitInfo, version int64, actions []tablelog.Action, data []byte) (*table.Snapshot, map[string][]byte, error) {
	// What the commit leaves of each file it names: nil if it removes it.
	changed := make(map[string]*tablelog.Add)
	added := make(map[string]bool)
	for _, a := range actions {
		switch {
		case a.Add != nil:
			if err := checkDataFile(a.Add.Path); err != nil {
				return nil, nil, err
			}
			changed[a.Add.Path], added[a.Add.Path] = a.Add, true
		case a.Remove != nil:
			changed[a.Remove.Path] = nil
		}
	}
	byShard := make(map[int][]string)
	for _, name := range slices.Sorted(maps.Keys(changed)) {
		byShard[shardOf(name)] = append(byShard[shardOf(name)], name)
	}
	prior, err := l.manifests(ctx, h.snapshot)
	if err != nil {
		return nil, nil, err
	}

	snap := &table.Snapshot{
		SnapshotID:     newSnapshotID(h.meta),
		SequenceNumber: h.meta.LastSequenceNumber() + 1,
		// GoAlert's time of the commit is in the commit. Iceberg's must not go
		// back, and the catalog's clock may have set the last one.
		TimestampMs: max(time.UnixMicro(info.GoAlertTime).UnixMilli(), h.meta.LastUpdatedMillis()),
	}
	if h.meta.Version() == 1 {
		snap.SequenceNumber = 0
	}
	if h.snapshot != nil {
		snap.ParentSnapshotID = &h.snapshot.SnapshotID
	}
	schemaID := h.meta.CurrentSchema().ID
	snap.SchemaID = &schemaID

	type file struct {
		name string
		data []byte
	}
	files := []file{{stagedName(version, info.GoAlertCommitID), data}}
	kept := make(map[string][]byte)
	prefix := fmt.Sprintf("%s%013d-%s", metadataPrefix, time.UnixMicro(info.GoAlertTime).UnixMilli(), info.GoAlertCommitID)

	var list []iceberg.ManifestFile
	removedFiles, addedFiles := 0, 0
	for _, shard := range slices.Sorted(maps.Keys(prior)) {
		if _, ok := byShard[shard]; !ok && prior[shard].AddedDataFiles()+prior[shard].ExistingDataFiles() > 0 {
			list = append(list, prior[shard])
		}
	}
	for _, shard := range slices.Sorted(maps.Keys(byShard)) {
		name := fmt.Sprintf("%s-m%02d.avro", prefix, shard)
		m, err := l.manifest(ctx, h, prior[shard], snap.SnapshotID, byShard[shard], changed, added)
		if err != nil {
			return nil, nil, err
		}
		addedFiles += m.added
		removedFiles += m.removed
		if m.writer == nil {
			continue
		}
		mf, err := m.writer.ToManifestFile(l.uri(name), int64(len(m.data)))
		if err != nil {
			return nil, nil, fmt.Errorf("iceberglog: write a manifest: %w", err)
		}
		list = append(list, mf)
		files = append(files, file{name, m.data})
		kept[name] = m.data
	}
	err = each(ctx, files, func(ctx context.Context, _ int, f file) error { return l.putNew(ctx, f.name, f.data) })
	if err != nil {
		return nil, nil, err
	}

	var buf bytes.Buffer
	if err := iceberg.WriteManifestList(h.meta.Version(), &buf, snap.SnapshotID, snap.ParentSnapshotID, &snap.SequenceNumber, 0, list); err != nil {
		return nil, nil, fmt.Errorf("iceberglog: write the manifest list: %w", err)
	}
	name := prefix + "-list.avro"
	if err := l.putNew(ctx, name, buf.Bytes()); err != nil {
		return nil, nil, err
	}
	kept[name] = buf.Bytes()
	snap.ManifestList = l.uri(name)

	// Keep what the next commit will read, of the manifests that stay.
	l.mu.Lock()
	for _, m := range list {
		if name, err := l.name(m.FilePath()); err == nil && l.files[name] != nil {
			kept[name] = l.files[name]
		}
	}
	l.mu.Unlock()

	op := table.OpAppend
	if removedFiles > 0 {
		op = table.OpOverwrite
	}
	var totalFiles, totalRows int64
	for _, m := range list {
		totalFiles += int64(m.AddedDataFiles()) + int64(m.ExistingDataFiles())
		totalRows += m.AddedRows() + m.ExistingRows()
	}
	snap.Summary = &table.Summary{Operation: op, Properties: iceberg.Properties{
		sumVersion:  strconv.FormatInt(version, 10),
		sumCommitID: info.GoAlertCommitID,
		sumStoreID:  info.GoAlertStoreID,
		sumWriterID: info.GoAlertWriterID,
		sumFormat:   strconv.Itoa(info.GoAlertFormat),

		"added-data-files":   strconv.Itoa(addedFiles),
		"deleted-data-files": strconv.Itoa(removedFiles),
		"total-data-files":   strconv.FormatInt(totalFiles, 10),
		"total-records":      strconv.FormatInt(totalRows, 10),
	}}
	return snap, kept, nil
}

type builtManifest struct {
	writer         *iceberg.ManifestWriter // nil if it lists nothing
	data           []byte
	added, removed int
}

// manifest rewrites one shard's manifest.
func (l *Log) manifest(ctx context.Context, h *head, prior iceberg.ManifestFile, snapshotID int64, names []string, changed map[string]*tablelog.Add, added map[string]bool) (builtManifest, error) {
	var out builtManifest
	var buf bytes.Buffer
	w, err := iceberg.NewManifestWriter(h.meta.Version(), &buf, h.meta.PartitionSpec(), h.meta.CurrentSchema(), snapshotID)
	if err != nil {
		return out, fmt.Errorf("iceberglog: write a manifest: %w", err)
	}
	var old []iceberg.ManifestEntry
	if prior != nil {
		if old, err = l.entries(ctx, prior); err != nil {
			return out, err
		}
	}
	n := 0
	found := make(map[string]bool)
	for _, e := range old {
		name, err := l.name(e.DataFile().FilePath())
		if err != nil {
			return out, err
		}
		add, touched := changed[name]
		switch {
		case !touched:
			err = w.Existing(e)
		case add == nil:
			err = w.Delete(e)
			out.removed++
		default:
			found[name] = true
			continue // added again, below
		}
		if err != nil {
			return out, fmt.Errorf("iceberglog: write a manifest: %w", err)
		}
		found[name] = true
		n++
	}
	for _, name := range names {
		add := changed[name]
		if add == nil {
			if !found[name] && !added[name] {
				return out, fmt.Errorf("%w: the commit removes %s, which Iceberg does not have (F47)", tablelog.ErrCorrupt, name)
			}
			continue
		}
		var stats struct {
			NumRecords int64 `json:"numRecords"`
		}
		if add.Stats != "" {
			if err := json.Unmarshal([]byte(add.Stats), &stats); err != nil {
				return out, fmt.Errorf("iceberglog: the statistics of %s: %w", name, err)
			}
		}
		b, err := iceberg.NewDataFileBuilder(h.meta.PartitionSpec(), iceberg.EntryContentData, l.uri(name), iceberg.ParquetFile,
			nil, nil, nil, stats.NumRecords, add.Size)
		if err != nil {
			return out, fmt.Errorf("iceberglog: data file %s: %w", name, err)
		}
		if err := w.Add(iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &snapshotID, b.Build()).Build()); err != nil {
			return out, fmt.Errorf("iceberglog: write a manifest: %w", err)
		}
		out.added++
		n++
	}
	if n == 0 {
		return out, nil
	}
	if err := w.Close(); err != nil {
		return out, fmt.Errorf("iceberglog: write a manifest: %w", err)
	}
	out.writer, out.data = w, buf.Bytes()
	return out, nil
}

func newSnapshotID(meta table.Metadata) int64 {
	for {
		var b [8]byte
		_, _ = rand.Read(b[:])
		id := int64(binary.BigEndian.Uint64(b[:]) & math.MaxInt64)
		if id > 0 && meta.SnapshotByID(id) == nil {
			return id
		}
	}
}

// expired returns the IDs of all snapshots but the newest few, and those kept
// for another reason.
func expired(snaps []table.Snapshot, keep map[int64]bool, newest int) []int64 {
	snaps = slices.Clone(snaps)
	version := func(s table.Snapshot) int64 {
		if s.Summary == nil {
			return -1
		}
		v, err := strconv.ParseInt(s.Summary.Properties[sumVersion], 10, 64)
		if err != nil {
			return -1
		}
		return v
	}
	slices.SortFunc(snaps, func(a, b table.Snapshot) int { return cmp.Compare(version(b), version(a)) })
	var ids []int64
	for i, s := range snaps {
		if i >= newest && !keep[s.SnapshotID] {
			ids = append(ids, s.SnapshotID)
		}
	}
	slices.Sort(ids)
	return ids
}

// cleanMetadata deletes this package's metadata files that no snapshot uses
// and that are older than cutoff.
func (l *Log) cleanMetadata(ctx context.Context, h *head, cutoff time.Time) (int, error) {
	names, err := l.cfg.Store.List(ctx, metadataPrefix, "")
	if err != nil {
		return 0, err
	}
	var old []string
	for _, name := range names {
		match := manifestName.FindStringSubmatch(name)
		if match == nil {
			match = listName.FindStringSubmatch(name)
		}
		if match == nil {
			continue
		}
		ms, err := strconv.ParseInt(match[1], 10, 64)
		if err == nil && time.UnixMilli(ms).Before(cutoff) {
			old = append(old, name)
		}
	}
	if len(old) == 0 {
		return 0, nil
	}

	// When anything is unclear, nothing is deleted.
	used := make(map[string]bool)
	var mu sync.Mutex
	snaps := h.meta.Snapshots()
	err = each(ctx, snaps, func(ctx context.Context, _ int, s table.Snapshot) error {
		ms, err := l.manifests(ctx, &s)
		if err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		name, err := l.name(s.ManifestList)
		if err != nil {
			return err
		}
		used[name] = true
		for _, m := range ms {
			if name, err = l.name(m.FilePath()); err != nil {
				return err
			}
			used[name] = true
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, name := range old {
		if used[name] {
			continue
		}
		if err := l.cfg.Store.Delete(ctx, name); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

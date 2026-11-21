package deltatable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/objstore"
)

// The Delta table's partition columns: the GoAlert table, and a hash bucket
// of the row's key, which keeps data files small (F39).
const (
	TableColumn  = "tbl"
	BucketColumn = "bucket"
)

// PartitionColumns lists the Delta table's partition columns.
func PartitionColumns() []string { return []string{TableColumn, BucketColumn} }

// Column is one GoAlert table's column in the Delta table. *RowType[V]
// implements it.
type Column interface {
	Name() string
	DeltaField() json.RawMessage
}

// SchemaString returns the Delta schemaString for a table holding the given
// GoAlert tables: the partition columns, then one struct column per GoAlert
// table, sorted by name.
func SchemaString(cols ...Column) (string, error) {
	sorted := append([]Column(nil), cols...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name() < sorted[j].Name() })
	fields := []json.RawMessage{
		json.RawMessage(`{"name":"tbl","type":"string","nullable":false,"metadata":{}}`),
		json.RawMessage(`{"name":"bucket","type":"integer","nullable":false,"metadata":{}}`),
	}
	seen := map[string]bool{TableColumn: true, BucketColumn: true}
	for _, c := range sorted {
		if seen[c.Name()] {
			return "", fmt.Errorf("deltatable: column %q defined twice", c.Name())
		}
		seen[c.Name()] = true
		fields = append(fields, c.DeltaField())
	}
	data, err := json.Marshal(struct {
		Type   string            `json:"type"`
		Fields []json.RawMessage `json:"fields"`
	}{Type: "struct", Fields: fields})
	return string(data), err
}

// Bucket returns the bucket of a row key, out of n. It is FNV-1a, which is
// the same in every process and release: a seeded hash such as maphash would
// put a row in different buckets on different pods (F25).
func Bucket(key string, n int) int {
	if n <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(n))
}

// DataFilePath returns a new, unique data file path in a table's bucket,
// in the Hive-style layout other Delta readers expect.
func DataFilePath(table string, bucket int) string {
	return fmt.Sprintf("%s=%s/%s=%d/part-%s.snappy.parquet", TableColumn, table, BucketColumn, bucket, uuid.NewString())
}

// WriteFile uploads rows as a new data file in a table's bucket and returns
// the add action that puts it in the table. The file is not part of the
// table until a commit includes that action, so the upload always happens
// first (F6).
//
// An error means the table is unchanged, so it never matches
// objstore.ErrUnknown: only a commit can have an unknown outcome (F32).
func WriteFile[V any](ctx context.Context, store objstore.Store, rt *RowType[V], bucket int, rows []V, at time.Time) (deltalog.Add, error) {
	data, err := rt.Encode(rows)
	if err != nil {
		return deltalog.Add{}, err
	}
	path, err := upload(ctx, store, rt.Name(), bucket, data)
	if err != nil {
		return deltalog.Add{}, err
	}
	stats, err := json.Marshal(struct {
		NumRecords int `json:"numRecords"`
	}{len(rows)})
	if err != nil {
		return deltalog.Add{}, err
	}
	return deltalog.Add{
		Path:             path,
		PartitionValues:  map[string]string{TableColumn: rt.Name(), BucketColumn: strconv.Itoa(bucket)},
		Size:             int64(len(data)),
		ModificationTime: at.UnixMilli(),
		DataChange:       true,
		Stats:            string(stats),
	}, nil
}

// How often, and how patiently, a data file upload is tried.
var (
	uploadAttempts = 5
	uploadBackoff  = 100 * time.Millisecond
)

// upload stores data as a new data file in a table's bucket and returns its
// path. Every attempt uses a new name (F36), and retries back off with
// jitter (F21). An attempt whose outcome was unclear may have left a file
// behind; no commit references it, so VACUUM removes it later (F16).
func upload(ctx context.Context, store objstore.Store, table string, bucket int, data []byte) (string, error) {
	delay := uploadBackoff
	var err error
	for attempt := range uploadAttempts {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return "", fmt.Errorf("deltatable: upload data file: %w (after %v)", ctx.Err(), err)
			case <-time.After(delay/2 + rand.N(delay)):
			}
			delay *= 2
		}
		path := DataFilePath(table, bucket)
		err = store.PutIfAbsent(ctx, path, data)
		if err == nil {
			return path, nil
		}
		if !errors.Is(err, objstore.ErrUnknown) && !errors.Is(err, objstore.ErrExists) {
			return "", fmt.Errorf("deltatable: upload data file: %w", err)
		}
	}
	// %v, not %w: the table is unchanged whatever happened to the uploads.
	return "", fmt.Errorf("deltatable: upload data file: gave up after %d attempts: %v", uploadAttempts, err)
}

// ReadFile reads the rows of a data file.
func ReadFile[V any](ctx context.Context, store objstore.Store, rt *RowType[V], path string) ([]V, error) {
	data, err := store.Get(ctx, path)
	if err != nil {
		return nil, err
	}
	return rt.Decode(data)
}

package filestore

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Snapshot layout
//
//	magic "GOALSNAP" | version u32 | store ID [16] | index u64 | created unix nanos i64 | table count u32
//	per table: name length u16 | name | row count u64 | rows
//	per row:   key length u32 | key | value length u32 | value
//	crc32c of everything above u32
//
// A snapshot holds the full state as of a log index. All integers are
// little-endian.
const (
	snapMagic   = "GOALSNAP"
	snapVersion = 1
	snapExt     = ".snap"
	tmpExt      = ".tmp"
	damagedExt  = ".damaged"
)

func snapshotName(index uint64) string { return fmt.Sprintf("%020d%s", index, snapExt) }

// encodeSnapshot writes v to w in the snapshot format.
func encodeSnapshot(w io.Writer, storeID [16]byte, schema *Schema, v *View, created time.Time) error {
	h := crc32.New(castagnoli)
	bw := bufio.NewWriterSize(io.MultiWriter(w, h), 1<<20)

	var hdr []byte
	hdr = append(hdr, snapMagic...)
	hdr = binary.LittleEndian.AppendUint32(hdr, snapVersion)
	hdr = append(hdr, storeID[:]...)
	hdr = binary.LittleEndian.AppendUint64(hdr, v.index)
	hdr = binary.LittleEndian.AppendUint64(hdr, uint64(created.UnixNano()))
	hdr = binary.LittleEndian.AppendUint32(hdr, uint32(len(schema.names)))
	if _, err := bw.Write(hdr); err != nil {
		return err
	}

	var buf []byte
	for _, name := range schema.names {
		d := v.tables[name]
		buf = buf[:0]
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(name)))
		buf = append(buf, name...)
		buf = binary.LittleEndian.AppendUint64(buf, uint64(d.length()))
		if _, err := bw.Write(buf); err != nil {
			return err
		}
		err := d.encodeRows(func(key string, raw []byte) error {
			buf = buf[:0]
			buf = binary.LittleEndian.AppendUint32(buf, uint32(len(key)))
			buf = append(buf, key...)
			buf = binary.LittleEndian.AppendUint32(buf, uint32(len(raw)))
			buf = append(buf, raw...)
			_, err := bw.Write(buf)
			return err
		})
		if err != nil {
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	var crc [4]byte
	binary.LittleEndian.PutUint32(crc[:], h.Sum32())
	_, err := w.Write(crc[:])
	return err
}

// writeSnapshot writes v to a new snapshot file atomically: to a temporary
// file first, which is fsynced, renamed into place, and the rename made
// durable.
func writeSnapshot(fs fileSystem, dir string, storeID [16]byte, schema *Schema, v *View, created time.Time) error {
	final := filepath.Join(dir, snapshotName(v.index))
	tmp := final + tmpExt
	_ = fs.Remove(tmp) // left over from a crash

	f, err := fs.Create(tmp)
	if err != nil {
		return err
	}
	if err := encodeSnapshot(f, storeID, schema, v, created); err != nil {
		_ = f.Close()
		_ = fs.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = fs.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = fs.Remove(tmp)
		return err
	}
	if err := fs.Rename(tmp, final); err != nil {
		_ = fs.Remove(tmp)
		return err
	}
	return fs.SyncDir(dir)
}

// reader decodes the fixed-width fields of the snapshot format with bounds
// checks.
type reader struct {
	b   []byte
	off int
	err error
}

func (r *reader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || len(r.b)-r.off < n {
		r.err = corruptf("snapshot is truncated")
		return nil
	}
	b := r.b[r.off : r.off+n]
	r.off += n
	return b
}

func (r *reader) u16() uint16 {
	if b := r.take(2); b != nil {
		return binary.LittleEndian.Uint16(b)
	}
	return 0
}

func (r *reader) u32() uint32 {
	if b := r.take(4); b != nil {
		return binary.LittleEndian.Uint32(b)
	}
	return 0
}

func (r *reader) u64() uint64 {
	if b := r.take(8); b != nil {
		return binary.LittleEndian.Uint64(b)
	}
	return 0
}

// decodeSnapshot validates a snapshot and rebuilds its tables.
func decodeSnapshot(data []byte, storeID [16]byte, schema *Schema) (index uint64, tables map[string]*tableData, err error) {
	if len(data) < 8+4 || string(data[:8]) != snapMagic {
		return 0, nil, corruptf("not a snapshot")
	}
	if v := binary.LittleEndian.Uint32(data[8:12]); v != snapVersion {
		return 0, nil, fmt.Errorf("%w: snapshot format version %d (this version of GoAlert reads %d)", ErrVersion, v, snapVersion)
	}
	if len(data) < 4 {
		return 0, nil, corruptf("snapshot is truncated")
	}
	body, sum := data[:len(data)-4], binary.LittleEndian.Uint32(data[len(data)-4:])
	if crc32.Checksum(body, castagnoli) != sum {
		return 0, nil, corruptf("snapshot checksum mismatch")
	}

	r := &reader{b: body, off: 12}
	if id := r.take(16); r.err == nil && !bytes.Equal(id, storeID[:]) {
		return 0, nil, corruptf("snapshot belongs to a different data directory")
	}
	index = r.u64()
	_ = r.u64() // created
	count := r.u32()
	if r.err != nil {
		return 0, nil, r.err
	}

	tables = make(map[string]*tableData, len(schema.names))
	for i := uint32(0); i < count; i++ {
		name := string(r.take(int(r.u16())))
		rows := r.u64()
		if r.err != nil {
			return 0, nil, r.err
		}
		def, ok := schema.tables[name]
		if !ok {
			return 0, nil, fmt.Errorf("%w: snapshot has table %q, which this version of GoAlert does not know (was it written by a newer version?)", ErrVersion, name)
		}
		if _, dup := tables[name]; dup {
			return 0, nil, corruptf("snapshot has table %q twice", name)
		}
		d := def.newData()
		for j := uint64(0); j < rows; j++ {
			key := string(r.take(int(r.u32())))
			raw := r.take(int(r.u32()))
			if r.err != nil {
				return 0, nil, r.err
			}
			if err := d.applyPut(key, raw); err != nil {
				return 0, nil, fmt.Errorf("snapshot table %s: %w", name, err)
			}
		}
		tables[name] = d
	}
	if r.off != len(body) {
		return 0, nil, corruptf("snapshot has %d unexpected trailing bytes", len(body)-r.off)
	}
	for _, name := range schema.names {
		if _, ok := tables[name]; !ok {
			tables[name] = schema.tables[name].newData() // a table added since the snapshot was written
		}
	}
	return index, tables, nil
}

// listSnapshots returns the indexes of the snapshot files in dir, ascending.
func listSnapshots(fs fileSystem, dir string) ([]uint64, error) {
	names, err := fs.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var idx []uint64
	for _, n := range names {
		if !strings.HasSuffix(n, snapExt) {
			continue
		}
		i, err := strconv.ParseUint(strings.TrimSuffix(n, snapExt), 10, 64)
		if err != nil || snapshotName(i) != n {
			return nil, corruptf("unexpected snapshot file name %q", n)
		}
		idx = append(idx, i)
	}
	sort.Slice(idx, func(a, b int) bool { return idx[a] < idx[b] })
	return idx, nil
}

// loadSnapshot loads the newest snapshot that is valid. If a newer snapshot
// is damaged it falls back to an older one; the caller checks that the log
// still covers everything after the snapshot it got, so falling back never
// loses data. An unknown format version stops startup instead.
func loadSnapshot(fs fileSystem, dir string, storeID [16]byte, schema *Schema, logf func(string, ...any)) (index uint64, tables map[string]*tableData, found bool, err error) {
	idx, err := listSnapshots(fs, dir)
	if err != nil {
		return 0, nil, false, err
	}
	for i := len(idx) - 1; i >= 0; i-- {
		name := snapshotName(idx[i])
		data, err := fs.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return 0, nil, false, err
		}
		index, tables, err := decodeSnapshot(data, storeID, schema)
		if err == nil && index != idx[i] {
			err = corruptf("snapshot %s holds index %d", name, index)
		}
		if err != nil {
			if !errorsIsCorrupt(err) {
				return 0, nil, false, fmt.Errorf("%s: %w", name, err)
			}
			// Set the damaged file aside, so that compaction never counts it
			// as one of the snapshots worth keeping. It stays for inspection.
			logf("filestore: setting aside damaged snapshot %s as %s%s: %v", name, name, damagedExt, err)
			path := filepath.Join(dir, name)
			if err := fs.Rename(path, path+damagedExt); err != nil {
				return 0, nil, false, err
			}
			if err := fs.SyncDir(dir); err != nil {
				return 0, nil, false, err
			}
			continue
		}
		return index, tables, true, nil
	}
	return 0, nil, false, nil
}

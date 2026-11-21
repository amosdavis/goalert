package filestore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Log layout
//
// The log is a sequence of segment files named by the index of their first
// record (00000000000000000001.wal). Each segment starts with a header:
//
//	magic "GOALWAL\x00" | version u32 | store ID [16] | first index u64 | crc32c u32
//
// followed by records:
//
//	payload length u32 | crc32c(length bytes + payload) u32 | payload
//
// A payload starts with its index (u64), so a valid record found after a
// damaged one proves the damage is not just an unfinished write at the end
// (F6). All integers are little-endian.
const (
	walMagic      = "GOALWAL\x00"
	walVersion    = 1
	walHeaderSize = 8 + 4 + 16 + 8 + 4
	walExt        = ".wal"

	recordHeaderSize = 8

	// minRecordPayload is the size of a payload's fixed fields: index u64,
	// commit time i64 and mutation count u32.
	minRecordPayload = 20
	maxRecordPayload = 256 << 20
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type segment struct {
	name       string // base name
	firstIndex uint64
	lastIndex  uint64 // firstIndex-1 when the segment has no records
	size       int64
}

type wal struct {
	fs      fileSystem
	dir     string
	storeID [16]byte
	segSize int64

	mu        sync.Mutex // guards everything below
	segs      []segment  // in order; the last one is active
	active    file
	lastIndex uint64
	failed    error
}

func segmentName(first uint64) string { return fmt.Sprintf("%020d%s", first, walExt) }

func encodeWALHeader(storeID [16]byte, first uint64) []byte {
	b := make([]byte, walHeaderSize)
	copy(b[0:8], walMagic)
	binary.LittleEndian.PutUint32(b[8:12], walVersion)
	copy(b[12:28], storeID[:])
	binary.LittleEndian.PutUint64(b[28:36], first)
	binary.LittleEndian.PutUint32(b[36:40], crc32.Checksum(b[:36], castagnoli))
	return b
}

// parseWALHeader validates a segment header and returns the segment's first
// index.
func parseWALHeader(b []byte, storeID [16]byte) (uint64, error) {
	if len(b) < walHeaderSize {
		return 0, errShortHeader
	}
	if string(b[0:8]) != walMagic {
		return 0, corruptf("bad log segment header")
	}
	if v := binary.LittleEndian.Uint32(b[8:12]); v != walVersion {
		return 0, fmt.Errorf("%w: log format version %d (this version of GoAlert reads %d)", ErrVersion, v, walVersion)
	}
	if crc32.Checksum(b[:36], castagnoli) != binary.LittleEndian.Uint32(b[36:40]) {
		return 0, corruptf("bad log segment header checksum")
	}
	if !bytes.Equal(b[12:28], storeID[:]) {
		return 0, errForeignSegment
	}
	return binary.LittleEndian.Uint64(b[28:36]), nil
}

// errForeignSegment is a complete, valid header from another data directory.
// It is never mistaken for a torn header and removed.
var errForeignSegment = corruptf("log segment belongs to a different data directory")

func recordChecksum(lenBytes, payload []byte) uint32 {
	return crc32.Update(crc32.Checksum(lenBytes, castagnoli), castagnoli, payload)
}

func encodeRecord(payload []byte) []byte {
	b := make([]byte, recordHeaderSize+len(payload))
	binary.LittleEndian.PutUint32(b[0:4], uint32(len(payload)))
	copy(b[recordHeaderSize:], payload)
	binary.LittleEndian.PutUint32(b[4:8], recordChecksum(b[0:4], payload))
	return b
}

// parseRecord parses the record at data[off:]. It returns the payload and the
// record's total size, or ok=false if there is no complete, valid record
// there.
func parseRecord(data []byte, off int) (payload []byte, size int, ok bool) {
	if off < 0 || len(data)-off < recordHeaderSize+minRecordPayload {
		return nil, 0, false
	}
	n := binary.LittleEndian.Uint32(data[off:])
	if n < minRecordPayload || n > maxRecordPayload || int64(n) > int64(len(data)-off-recordHeaderSize) {
		return nil, 0, false
	}
	end := off + recordHeaderSize + int(n)
	payload = data[off+recordHeaderSize : end]
	if recordChecksum(data[off:off+4], payload) != binary.LittleEndian.Uint32(data[off+4:]) {
		return nil, 0, false
	}
	return payload, end - off, true
}

func payloadIndex(payload []byte) uint64 { return binary.LittleEndian.Uint64(payload) }

// laterRecordExists reports whether any valid record with an index of at
// least minIndex starts at or after data[from:].
func laterRecordExists(data []byte, from int, minIndex uint64) bool {
	for p := from; p+recordHeaderSize+minRecordPayload <= len(data); p++ {
		// Cheap filters first: a plausible length and index.
		n := binary.LittleEndian.Uint32(data[p:])
		if n < minRecordPayload || int64(n) > int64(len(data)-p-recordHeaderSize) {
			continue
		}
		idx := binary.LittleEndian.Uint64(data[p+recordHeaderSize:])
		if idx < minIndex || idx-minIndex > 1<<32 {
			continue
		}
		if _, _, ok := parseRecord(data, p); ok {
			return true
		}
	}
	return false
}

// openWAL validates every log segment, calls replay for each record in
// order, and opens the last segment for appending. covered is the index of
// the snapshot the caller loaded (0 if none). If there are no segments, it
// creates one that starts at covered+1.
//
// Damage at the end of the last segment that is not followed by any valid
// record is a write cut short by a crash. Records are fsynced before they are
// acknowledged, so that write was never acknowledged, and it is truncated.
// Gaps and damage in old segments are tolerated only when everything they
// could hold is already in the snapshot: compaction deletes old segments, and
// a crash can leave those deletions partly done. Any other damage is an error
// (F6).
func openWAL(fs fileSystem, dir string, storeID [16]byte, segSize int64, covered uint64, logf func(string, ...any), replay func(index uint64, payload []byte) error) (*wal, error) {
	names, err := fs.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var segs []segment
	for _, n := range names {
		if !strings.HasSuffix(n, walExt) {
			continue
		}
		first, err := strconv.ParseUint(strings.TrimSuffix(n, walExt), 10, 64)
		if err != nil || segmentName(first) != n {
			return nil, corruptf("unexpected log file name %q", n)
		}
		segs = append(segs, segment{name: n, firstIndex: first})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].firstIndex < segs[j].firstIndex })

	for i := 0; i < len(segs); i++ {
		s := &segs[i]
		isLast := i == len(segs)-1
		path := filepath.Join(dir, s.name)
		data, err := fs.ReadFile(path)
		if err != nil {
			return nil, err
		}

		first, err := parseWALHeader(data, storeID)
		torn := err != nil && !errors.Is(err, ErrVersion) && !errors.Is(err, errForeignSegment)
		if torn && isLast && len(data) <= walHeaderSize {
			// A crash while starting a new segment: the header is short or
			// fails its checksum, and the file holds no records, so nothing
			// is lost by removing it.
			logf("filestore: removing incomplete new log segment %s", s.name)
			if err := fs.Remove(path); err != nil {
				return nil, err
			}
			if err := fs.SyncDir(dir); err != nil {
				return nil, err
			}
			segs = segs[:i]
			break
		}
		if err != nil {
			if errors.Is(err, errShortHeader) {
				err = corruptf("log segment %s is truncated", s.name)
			}
			return nil, fmt.Errorf("%s: %w", s.name, err)
		}
		if first != s.firstIndex {
			return nil, corruptf("log segment %s says it starts at index %d", s.name, first)
		}
		if i > 0 && first != segs[i-1].lastIndex+1 {
			if first > covered+1 {
				return nil, corruptf("log segment %s starts at index %d, but the previous one ends at %d", s.name, first, segs[i-1].lastIndex)
			}
			logf("filestore: ignoring a gap in old log segments before %s; the snapshot covers it", s.name)
		}

		next := first
		off := walHeaderSize
		for off < len(data) {
			payload, n, ok := parseRecord(data, off)
			if ok && payloadIndex(payload) != next {
				return nil, corruptf("log segment %s: record at offset %d has index %d, expected %d", s.name, off, payloadIndex(payload), next)
			}
			if !ok {
				if !isLast {
					if segs[i+1].firstIndex > covered+1 {
						return nil, corruptf("log segment %s is damaged at offset %d", s.name, off)
					}
					logf("filestore: ignoring damage in old log segment %s; the snapshot covers it", s.name)
					next = segs[i+1].firstIndex
					break
				}
				if laterRecordExists(data, off+1, next) {
					return nil, corruptf("log segment %s is damaged at offset %d, and valid records follow", s.name, off)
				}
				logf("filestore: dropping %d bytes of an unfinished write at the end of log segment %s (it was never acknowledged)", len(data)-off, s.name)
				if err := fs.TruncateFile(path, int64(off)); err != nil {
					return nil, err
				}
				data = data[:off]
				break
			}
			if err := replay(next, payload); err != nil {
				return nil, fmt.Errorf("replay log index %d: %w", next, err)
			}
			next++
			off += n
		}
		s.lastIndex = next - 1
		s.size = int64(len(data))
	}

	w := &wal{fs: fs, dir: dir, storeID: storeID, segSize: segSize, segs: segs}
	if len(segs) == 0 {
		w.lastIndex = covered
		if err := w.rotateLocked(covered + 1); err != nil {
			return nil, fmt.Errorf("create log segment: %w", err)
		}
		return w, nil
	}

	last := segs[len(segs)-1]
	w.lastIndex = last.lastIndex
	w.active, err = fs.OpenAppend(filepath.Join(dir, last.name))
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (w *wal) firstIndex() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.segs[0].firstIndex
}

func (w *wal) fail(err error) error {
	w.failed = err
	return err
}

// rotateLocked starts a new segment whose first record will be first.
func (w *wal) rotateLocked(first uint64) error {
	path := filepath.Join(w.dir, segmentName(first))
	f, err := w.fs.Create(path)
	if err != nil {
		return err
	}
	if _, err := f.Write(encodeWALHeader(w.storeID, first)); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := w.fs.SyncDir(w.dir); err != nil {
		_ = f.Close()
		return err
	}
	if w.active != nil {
		if err := w.active.Close(); err != nil {
			_ = f.Close()
			return err
		}
	}
	w.active = f
	w.segs = append(w.segs, segment{name: segmentName(first), firstIndex: first, lastIndex: first - 1, size: walHeaderSize})
	return nil
}

// append writes one record and fsyncs it. After any write or fsync error the
// log refuses further appends (F21): once fsync has failed, the OS may have
// dropped the data, so the only safe recovery is to reopen and re-read the
// log.
func (w *wal) append(index uint64, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failed != nil {
		return w.failed
	}
	if index != w.lastIndex+1 {
		return fmt.Errorf("filestore: internal error: appending index %d after %d", index, w.lastIndex)
	}
	if len(payload) > maxRecordPayload {
		return fmt.Errorf("filestore: transaction too large (%d bytes, limit %d)", len(payload), maxRecordPayload)
	}

	if last := w.segs[len(w.segs)-1]; last.size >= w.segSize && last.lastIndex >= last.firstIndex {
		if err := w.rotateLocked(index); err != nil {
			return w.fail(fmt.Errorf("start new log segment: %w", err))
		}
	}
	last := &w.segs[len(w.segs)-1]

	rec := encodeRecord(payload)
	if _, err := w.active.Write(rec); err != nil {
		// Remove any part of the record that reached the file. If that fails
		// too, recovery drops the unfinished record on the next start.
		_ = w.active.Truncate(last.size)
		return w.fail(fmt.Errorf("write log: %w", err))
	}
	if err := w.active.Sync(); err != nil {
		return w.fail(fmt.Errorf("fsync log: %w", err))
	}
	last.size += int64(len(rec))
	last.lastIndex = index
	w.lastIndex = index
	return nil
}

// removeThrough deletes the segments whose records all have an index of at
// most index. The active segment is never removed.
func (w *wal) removeThrough(index uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for n < len(w.segs)-1 && w.segs[n].lastIndex <= index {
		n++
	}
	if n == 0 {
		return nil
	}
	for _, s := range w.segs[:n] {
		if err := w.fs.Remove(filepath.Join(w.dir, s.name)); err != nil {
			return err
		}
	}
	w.segs = append([]segment(nil), w.segs[n:]...)
	return w.fs.SyncDir(w.dir)
}

func (w *wal) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.active == nil {
		return nil
	}
	err := w.active.Close()
	w.active = nil
	if w.failed == nil {
		w.failed = ErrClosed
	}
	return err
}

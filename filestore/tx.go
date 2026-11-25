package filestore

import (
	"encoding/binary"
	"time"
)

const (
	opPut    byte = 1
	opDelete byte = 2
)

// A mutation is one row change. A transaction's mutations are written to the
// log as one record.
type mutation struct {
	op    byte
	table string
	key   string
	val   []byte // the row's JSON, for opPut
}

// Tx is a write transaction. It sees its own changes. It is valid only inside
// the function passed to Update, on that goroutine.
type Tx struct {
	schema  *Schema
	base    map[string]*tableData // the writer's current tables
	working map[string]*tableData // clones of the tables this transaction changed
	muts    []mutation
	now     time.Time
	done    bool
	failed  error // set by Fail
}

func (tx *Tx) tableData(name string) *tableData {
	if d, ok := tx.working[name]; ok {
		return d
	}
	return tx.base[name]
}

func (tx *Tx) writable(name string) *tableData {
	if d, ok := tx.working[name]; ok {
		return d
	}
	base, ok := tx.base[name]
	if !ok {
		panic("filestore: table " + name + " is not in this store's schema")
	}
	d := base.clone()
	tx.working[name] = d
	return d
}

func (tx *Tx) check() error {
	if tx.done {
		return ErrTxDone
	}
	return nil
}

// Now returns the transaction's timestamp: a single time for the whole
// transaction, like now() in Postgres, in UTC with microsecond precision.
// Store it in rows instead of reading a clock later, so that replay sees the
// same values (F25).
func (tx *Tx) Now() time.Time { return tx.now }

// NextID returns the next value of a sequence, starting at 1. Unlike a
// Postgres sequence it is part of the transaction, so a transaction that
// fails does not use up a value.
func (tx *Tx) NextID(sequence string) (int64, error) {
	seq := tx.schema.seq
	r, _ := seq.Get(tx, sequence)
	r.Name = sequence
	r.Value++
	if err := seq.Put(tx, r); err != nil {
		return 0, err
	}
	return r.Value, nil
}

// encodePayload encodes a log record payload:
//
//	index u64 | commit time unix nanos i64 | mutation count u32 | mutations
//	mutation: op u8 | table length u16 | table | key length u32 | key [| value length u32 | value]
func encodePayload(index uint64, at time.Time, muts []mutation) []byte {
	size := minRecordPayload
	for _, m := range muts {
		size += 1 + 2 + len(m.table) + 4 + len(m.key)
		if m.op == opPut {
			size += 4 + len(m.val)
		}
	}
	b := make([]byte, 0, size)
	b = binary.LittleEndian.AppendUint64(b, index)
	b = binary.LittleEndian.AppendUint64(b, uint64(at.UnixNano()))
	b = binary.LittleEndian.AppendUint32(b, uint32(len(muts)))
	for _, m := range muts {
		b = append(b, m.op)
		b = binary.LittleEndian.AppendUint16(b, uint16(len(m.table)))
		b = append(b, m.table...)
		b = binary.LittleEndian.AppendUint32(b, uint32(len(m.key)))
		b = append(b, m.key...)
		if m.op == opPut {
			b = binary.LittleEndian.AppendUint32(b, uint32(len(m.val)))
			b = append(b, m.val...)
		}
	}
	return b
}

func decodePayload(p []byte) (index uint64, at time.Time, muts []mutation, err error) {
	r := &reader{b: p}
	index = r.u64()
	at = time.Unix(0, int64(r.u64())).UTC()
	n := r.u32()
	if r.err != nil {
		return 0, time.Time{}, nil, r.err
	}
	if uint64(n) > uint64(len(p)) { // each mutation takes at least one byte
		return 0, time.Time{}, nil, corruptf("log record claims %d changes", n)
	}
	muts = make([]mutation, 0, n)
	for i := uint32(0); i < n; i++ {
		var m mutation
		if b := r.take(1); b != nil {
			m.op = b[0]
		}
		m.table = string(r.take(int(r.u16())))
		m.key = string(r.take(int(r.u32())))
		switch m.op {
		case opPut:
			m.val = r.take(int(r.u32()))
		case opDelete:
		default:
			if r.err == nil {
				r.err = corruptf("log record has unknown operation %d", m.op)
			}
		}
		if r.err != nil {
			return 0, time.Time{}, nil, r.err
		}
		muts = append(muts, m)
	}
	if r.off != len(p) {
		return 0, time.Time{}, nil, corruptf("log record has %d unexpected trailing bytes", len(p)-r.off)
	}
	return index, at, muts, nil
}

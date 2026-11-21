package filestore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/google/btree"
	"github.com/target/goalert/deltatable"
)

const (
	btreeDegree = 32

	// maxKeyLen bounds primary and index keys.
	maxKeyLen = 4096
)

// Schema is the set of tables in a store. Define every table, with its
// indexes, before passing the schema to Open.
type Schema struct {
	tables   map[string]tableDef
	names    []string // sorted
	seq      *Table[seqRow]
	frozen   bool
	delta    bool  // set up for a Delta table (delta.go)
	deltaErr error // why that failed
}

// seqRow holds the last value handed out by a sequence.
type seqRow struct {
	Name  string `parquet:"name"`
	Value int64  `parquet:"value"`
}

// NewSchema returns an empty schema.
func NewSchema() *Schema {
	s := &Schema{tables: make(map[string]tableDef)}
	s.seq = newTable(s, "_seq", func(r seqRow) string { return r.Name })
	return s
}

type tableDef interface {
	newData() tableData
	deltaDef // delta.go
}

// tableData is one table's rows and indexes. A tableData that has been
// published to readers is never modified again; writers work on a clone.
type tableData interface {
	clone() tableData
	applyPut(key string, raw []byte) error
	applyDelete(key string) error
	length() int
	encodeRows(fn func(key string, raw []byte) error) error
}

// Reader is a consistent view of the store: a *View, or the *Tx passed to
// Update.
type Reader interface {
	tableData(name string) tableData
}

// Table is a typed table of rows of type V, stored as JSON in the local log
// or as Parquet in a Delta table. Each row's key comes from the row itself.
//
// Rows returned by reads are shared with the store and with other readers.
// Treat them as read-only: copy a row, change the copy, and Put it.
type Table[V any] struct {
	schema  *Schema
	name    string
	keyFn   func(V) string
	indexes []*Index[V]
	uniques []*Unique[V]

	// On a Delta table (delta.go): how many buckets the rows are spread
	// over, the Parquet row type, and the index of rows by bucket.
	buckets  int
	rowType  *deltatable.RowType[V]
	bucketIx *Index[V]
}

// Index finds rows by a secondary key. A row can have any number of keys in
// an index, and many rows can share a key.
type Index[V any] struct {
	t    *Table[V]
	name string
	pos  int
	keys func(V) []string
}

// Unique is an index whose keys belong to at most one row. Put fails with a
// *UniqueError instead of breaking that rule (F8).
type Unique[V any] struct {
	t    *Table[V]
	name string
	pos  int
	key  func(V) (string, bool)
}

// NewTable defines a table. Names starting with "_" are reserved.
func NewTable[V any](s *Schema, name string, key func(V) string) *Table[V] {
	if name == "" || strings.HasPrefix(name, "_") {
		panic(fmt.Sprintf("filestore: invalid table name %q", name))
	}
	return newTable(s, name, key)
}

func newTable[V any](s *Schema, name string, key func(V) string) *Table[V] {
	s.checkOpen()
	if _, ok := s.tables[name]; ok {
		panic(fmt.Sprintf("filestore: table %q defined twice", name))
	}
	if len(name) > 255 {
		panic(fmt.Sprintf("filestore: table name %q is too long", name))
	}
	t := &Table[V]{schema: s, name: name, keyFn: key}
	s.tables[name] = t
	s.names = append(s.names, name)
	sort.Strings(s.names)
	return t
}

func (s *Schema) checkOpen() {
	if s.frozen {
		panic("filestore: schema changed after Open")
	}
}

// Name returns the table's name.
func (t *Table[V]) Name() string { return t.name }

// Index adds an index. keys returns the row's keys in the index.
func (t *Table[V]) Index(name string, keys func(V) []string) *Index[V] {
	t.schema.checkOpen()
	ix := &Index[V]{t: t, name: name, pos: len(t.indexes), keys: keys}
	t.indexes = append(t.indexes, ix)
	return ix
}

// Unique adds a unique index. key returns the row's key in the index, and
// false if the row has none.
func (t *Table[V]) Unique(name string, key func(V) (string, bool)) *Unique[V] {
	t.schema.checkOpen()
	u := &Unique[V]{t: t, name: name, pos: len(t.uniques), key: key}
	t.uniques = append(t.uniques, u)
	return u
}

type item[V any] struct {
	key string
	val V
}

type uniqEntry struct{ key, pk string }

func (t *Table[V]) newData() tableData {
	d := &typedTable[V]{
		def:  t,
		rows: btree.NewG(btreeDegree, func(a, b item[V]) bool { return a.key < b.key }),
		idx:  make([]*btree.BTreeG[string], len(t.indexes)),
		uniq: make([]*btree.BTreeG[uniqEntry], len(t.uniques)),
	}
	for i := range d.idx {
		d.idx[i] = btree.NewG(btreeDegree, func(a, b string) bool { return a < b })
	}
	for i := range d.uniq {
		d.uniq[i] = btree.NewG(btreeDegree, func(a, b uniqEntry) bool { return a.key < b.key })
	}
	return d
}

// decode decodes a row, refusing unknown fields: a field this version does
// not know was written by a newer version, and dropping it would lose data
// (F19).
func (t *Table[V]) decode(raw []byte) (V, error) {
	var v V
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	err := dec.Decode(&v)
	if err == nil && dec.More() {
		err = fmt.Errorf("trailing data after row")
	}
	if err != nil && strings.HasPrefix(err.Error(), "json: unknown field") {
		return v, fmt.Errorf("%w: table %s: %v (was it written by a newer version?)", ErrVersion, t.name, err)
	}
	if err != nil {
		return v, corruptf("table %s: decode row: %v", t.name, err)
	}
	return v, nil
}

func checkKey(table, kind, key string) error {
	if key == "" {
		return fmt.Errorf("filestore: %s: empty %s key", table, kind)
	}
	if len(key) > maxKeyLen {
		return fmt.Errorf("filestore: %s: %s key is longer than %d bytes", table, kind, maxKeyLen)
	}
	if strings.IndexByte(key, 0) >= 0 {
		return fmt.Errorf("filestore: %s: %s key contains a zero byte", table, kind)
	}
	return nil
}

func (t *Table[V]) data(r Reader) *typedTable[V] {
	d, ok := r.tableData(t.name).(*typedTable[V])
	if !ok {
		panic(fmt.Sprintf("filestore: table %q is not in this store's schema", t.name))
	}
	return d
}

// Get returns the row with the given key.
func (t *Table[V]) Get(r Reader, key string) (V, bool) {
	it, ok := t.data(r).rows.Get(item[V]{key: key})
	return it.val, ok
}

// Len returns the number of rows.
func (t *Table[V]) Len(r Reader) int { return t.data(r).rows.Len() }

// Ascend calls fn for each row in key order until fn returns false.
func (t *Table[V]) Ascend(r Reader, fn func(V) bool) {
	t.data(r).rows.Ascend(func(it item[V]) bool { return fn(it.val) })
}

// AscendFrom calls fn for each row with a key of at least from, in key order,
// until fn returns false.
func (t *Table[V]) AscendFrom(r Reader, from string, fn func(V) bool) {
	t.data(r).rows.AscendGreaterOrEqual(item[V]{key: from}, func(it item[V]) bool { return fn(it.val) })
}

// Descend calls fn for each row in reverse key order until fn returns false.
func (t *Table[V]) Descend(r Reader, fn func(V) bool) {
	t.data(r).rows.Descend(func(it item[V]) bool { return fn(it.val) })
}

// Put inserts or replaces a row.
func (t *Table[V]) Put(tx *Tx, v V) error {
	if err := tx.check(); err != nil {
		return err
	}
	v2, raw, err := t.normalize(v)
	if err != nil {
		return err
	}
	key := t.keyFn(v2)
	if err := checkKey(t.name, "primary", key); err != nil {
		return err
	}
	d := tx.writable(t.name).(*typedTable[V])
	if err := d.put(key, v2); err != nil {
		return err
	}
	tx.muts = append(tx.muts, mutation{op: opPut, table: t.name, key: key, val: raw})
	return nil
}

// normalize returns v exactly as the store will read it back, so the live
// state and a rebuilt one are always the same (F25): through Parquet on a
// Delta table, through JSON (also returned, for the log) otherwise. It also
// keeps the caller's value from being shared with the store.
func (t *Table[V]) normalize(v V) (V, []byte, error) {
	if t.rowType != nil {
		v2, err := t.rowType.Normalize(v)
		if err != nil {
			return v, nil, fmt.Errorf("filestore: %s: encode row: %w", t.name, err)
		}
		return v2, nil, nil
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return v, nil, fmt.Errorf("filestore: %s: encode row: %w", t.name, err)
	}
	v2, err := t.decode(raw)
	if err != nil {
		return v, nil, fmt.Errorf("filestore: %s: row does not decode the way it encodes: %w", t.name, err)
	}
	return v2, raw, nil
}

// Delete removes the row with the given key and reports whether it existed.
func (t *Table[V]) Delete(tx *Tx, key string) (bool, error) {
	if err := tx.check(); err != nil {
		return false, err
	}
	if _, ok := t.Get(tx, key); !ok {
		return false, nil
	}
	d := tx.writable(t.name).(*typedTable[V])
	d.delete(key)
	tx.muts = append(tx.muts, mutation{op: opDelete, table: t.name, key: key})
	return true, nil
}

// Ascend calls fn, in primary key order, for each row that has key in the
// index, until fn returns false.
func (ix *Index[V]) Ascend(r Reader, key string, fn func(V) bool) {
	d := ix.t.data(r)
	prefix := key + "\x00"
	d.idx[ix.pos].AscendGreaterOrEqual(prefix, func(e string) bool {
		if !strings.HasPrefix(e, prefix) {
			return false
		}
		it, ok := d.rows.Get(item[V]{key: e[len(prefix):]})
		if !ok {
			panic(fmt.Sprintf("filestore: %s.%s: index entry without a row", ix.t.name, ix.name))
		}
		return fn(it.val)
	})
}

// Count returns the number of rows that have key in the index.
func (ix *Index[V]) Count(r Reader, key string) int {
	n := 0
	ix.Ascend(r, key, func(V) bool { n++; return true })
	return n
}

// Get returns the row that has key in the unique index.
func (u *Unique[V]) Get(r Reader, key string) (V, bool) {
	d := u.t.data(r)
	e, ok := d.uniq[u.pos].Get(uniqEntry{key: key})
	if !ok {
		var zero V
		return zero, false
	}
	it, ok := d.rows.Get(item[V]{key: e.pk})
	if !ok {
		panic(fmt.Sprintf("filestore: %s.%s: unique entry without a row", u.t.name, u.name))
	}
	return it.val, true
}

// typedTable holds a table's rows and indexes as copy-on-write B-trees, so
// cloning is O(1) and a clone handed to readers is never affected by later
// writes (F10).
type typedTable[V any] struct {
	def  *Table[V]
	rows *btree.BTreeG[item[V]]
	idx  []*btree.BTreeG[string] // entries are index key + "\x00" + primary key
	uniq []*btree.BTreeG[uniqEntry]
}

func (d *typedTable[V]) clone() tableData {
	c := &typedTable[V]{
		def:  d.def,
		rows: d.rows.Clone(),
		idx:  make([]*btree.BTreeG[string], len(d.idx)),
		uniq: make([]*btree.BTreeG[uniqEntry], len(d.uniq)),
	}
	for i, t := range d.idx {
		c.idx[i] = t.Clone()
	}
	for i, t := range d.uniq {
		c.uniq[i] = t.Clone()
	}
	return c
}

func (d *typedTable[V]) length() int { return d.rows.Len() }

// put validates every index key and unique constraint before changing
// anything, so a failed put leaves the table as it was.
func (d *typedTable[V]) put(key string, v V) error {
	ikeys := make([][]string, len(d.def.indexes))
	for i, ix := range d.def.indexes {
		ikeys[i] = ix.keys(v)
		for _, k := range ikeys[i] {
			if err := checkKey(d.def.name, ix.name, k); err != nil {
				return err
			}
		}
	}
	ukeys := make([]string, len(d.def.uniques))
	has := make([]bool, len(d.def.uniques))
	for i, u := range d.def.uniques {
		k, ok := u.key(v)
		if !ok {
			continue
		}
		if err := checkKey(d.def.name, u.name, k); err != nil {
			return err
		}
		if e, found := d.uniq[i].Get(uniqEntry{key: k}); found && e.pk != key {
			return &UniqueError{Table: d.def.name, Index: u.name, Key: k}
		}
		ukeys[i], has[i] = k, true
	}

	if old, ok := d.rows.Get(item[V]{key: key}); ok {
		d.removeIndexes(key, old.val)
	}
	for i, keys := range ikeys {
		for _, k := range keys {
			d.idx[i].ReplaceOrInsert(k + "\x00" + key)
		}
	}
	for i := range ukeys {
		if has[i] {
			d.uniq[i].ReplaceOrInsert(uniqEntry{key: ukeys[i], pk: key})
		}
	}
	d.rows.ReplaceOrInsert(item[V]{key: key, val: v})
	return nil
}

func (d *typedTable[V]) removeIndexes(key string, old V) {
	for i, ix := range d.def.indexes {
		for _, k := range ix.keys(old) {
			d.idx[i].Delete(k + "\x00" + key)
		}
	}
	for i, u := range d.def.uniques {
		k, ok := u.key(old)
		if !ok {
			continue
		}
		if e, found := d.uniq[i].Get(uniqEntry{key: k}); found && e.pk == key {
			d.uniq[i].Delete(uniqEntry{key: k})
		}
	}
}

func (d *typedTable[V]) delete(key string) bool {
	old, ok := d.rows.Get(item[V]{key: key})
	if !ok {
		return false
	}
	d.removeIndexes(key, old.val)
	d.rows.Delete(item[V]{key: key})
	return true
}

func (d *typedTable[V]) applyPut(key string, raw []byte) error {
	v, err := d.def.decode(raw)
	if err != nil {
		return err
	}
	if got := d.def.keyFn(v); got != key {
		return corruptf("table %s: row stored under key %q has key %q", d.def.name, key, got)
	}
	if err := d.put(key, v); err != nil {
		return corruptf("table %s: %v", d.def.name, err)
	}
	return nil
}

func (d *typedTable[V]) applyDelete(key string) error {
	if !d.delete(key) {
		return corruptf("table %s: delete of missing row %q", d.def.name, key)
	}
	return nil
}

func (d *typedTable[V]) encodeRows(fn func(key string, raw []byte) error) error {
	var err error
	d.rows.Ascend(func(it item[V]) bool {
		var raw []byte
		raw, err = json.Marshal(it.val)
		if err == nil {
			err = fn(it.key, raw)
		}
		return err == nil
	})
	return err
}

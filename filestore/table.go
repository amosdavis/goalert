package filestore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
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
	tables   map[string]*table
	names    []string // sorted
	seq      *Table[seqRow]
	frozen   bool
	delta    bool          // set up for a Delta table (delta.go)
	deltaErr error         // why that failed
	fks      []*foreignKey // foreign keys, in declaration order (foreignkey.go)
}

// seqRow holds the last value handed out by a sequence.
type seqRow struct {
	Name  string `parquet:"name"`
	Value int64  `parquet:"value"`
}

// NewSchema returns an empty schema.
func NewSchema() *Schema {
	s := &Schema{tables: make(map[string]*table)}
	s.seq = newTable(s, "_seq", func(r seqRow) string { return r.Name })
	return s
}

// Reader is a consistent view of the store: a *View, or the *Tx passed to
// Update.
type Reader interface {
	tableData(name string) *tableData
}

// Table is a typed table of rows of type V, stored as JSON in the local log
// or as Parquet in a Delta table. Each row's key comes from the row itself.
//
// What a row refers to, through a pointer or a slice, is shared with the store
// and with other readers. Treat it as read-only.
type Table[V any] struct{ t *table }

// Index finds rows by a secondary key. A row can have any number of keys in
// an index, and many rows can share a key.
type Index[V any] struct{ ix *index }

// Unique is an index whose keys belong to at most one row. Put fails with a
// *UniqueError instead of breaking that rule (F8).
type Unique[V any] struct{ u *unique }

// table is a Table without the type of its rows. Rows are pointers to values
// of that type, which never change once they are in the store.
//
// Everything but Table, Index and Unique works with this, and not only for
// simplicity: generic code is compiled again for each type of row, in each
// package that uses a table of it, and there are many of both.
type table struct {
	schema  *Schema
	name    string
	typ     reflect.Type
	key     func(row any) string
	indexes []*index
	uniques []*unique

	// On a Delta table (delta.go): how many buckets the rows are spread
	// over, and the Parquet row type.
	buckets int
	rowType *deltatable.Rows
}

type index struct {
	t    *table
	name string
	pos  int
	keys func(row any) []string
}

type unique struct {
	t    *table
	name string
	pos  int
	key  func(row any) (string, bool)
}

// NewTable defines a table. Names starting with "_" are reserved.
func NewTable[V any](s *Schema, name string, key func(V) string) *Table[V] {
	if name == "" || strings.HasPrefix(name, "_") {
		panic(fmt.Sprintf("filestore: invalid table name %q", name))
	}
	return newTable(s, name, key)
}

func newTable[V any](s *Schema, name string, key func(V) string) *Table[V] {
	return &Table[V]{s.define(name, reflect.TypeFor[V](), func(row any) string { return key(*row.(*V)) })}
}

func (s *Schema) define(name string, typ reflect.Type, key func(row any) string) *table {
	s.checkOpen()
	if _, ok := s.tables[name]; ok {
		panic(fmt.Sprintf("filestore: table %q defined twice", name))
	}
	if len(name) > 255 {
		panic(fmt.Sprintf("filestore: table name %q is too long", name))
	}
	t := &table{schema: s, name: name, typ: typ, key: key}
	s.tables[name] = t
	s.names = append(s.names, name)
	sort.Strings(s.names)
	return t
}

// TableNames returns the names of the tables that were defined, sorted.
func (s *Schema) TableNames() []string {
	return slices.DeleteFunc(slices.Clone(s.names), func(name string) bool { return strings.HasPrefix(name, "_") })
}

// UniqueNames returns the names of a table's unique indexes.
func (s *Schema) UniqueNames(table string) []string {
	t, ok := s.tables[table]
	if !ok {
		return nil
	}
	var names []string
	for _, u := range t.uniques {
		names = append(names, u.name)
	}
	return names
}

func (s *Schema) checkOpen() {
	if s.frozen {
		panic("filestore: schema changed after Open")
	}
}

// Name returns the table's name.
func (t *Table[V]) Name() string { return t.t.name }

// Index adds an index. keys returns the row's keys in the index.
func (t *Table[V]) Index(name string, keys func(V) []string) *Index[V] {
	return &Index[V]{t.t.addIndex(name, func(row any) []string { return keys(*row.(*V)) })}
}

func (t *table) addIndex(name string, keys func(row any) []string) *index {
	t.schema.checkOpen()
	ix := &index{t: t, name: name, pos: len(t.indexes), keys: keys}
	t.indexes = append(t.indexes, ix)
	return ix
}

// Unique adds a unique index. key returns the row's key in the index, and
// false if the row has none.
func (t *Table[V]) Unique(name string, key func(V) (string, bool)) *Unique[V] {
	return &Unique[V]{t.t.addUnique(name, func(row any) (string, bool) { return key(*row.(*V)) })}
}

func (t *table) addUnique(name string, key func(row any) (string, bool)) *unique {
	t.schema.checkOpen()
	u := &unique{t: t, name: name, pos: len(t.uniques), key: key}
	t.uniques = append(t.uniques, u)
	return u
}

// decode decodes a row, refusing unknown fields: a field this version does
// not know was written by a newer version, and dropping it would lose data
// (F19).
func (t *table) decode(raw []byte) (row any, err error) {
	row = reflect.New(t.typ).Interface()
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	err = dec.Decode(row)
	if err == nil && dec.More() {
		err = fmt.Errorf("trailing data after row")
	}
	if err != nil && strings.HasPrefix(err.Error(), "json: unknown field") {
		return nil, fmt.Errorf("%w: table %s: %v (was it written by a newer version?)", ErrVersion, t.name, err)
	}
	if err != nil {
		return nil, corruptf("table %s: decode row: %v", t.name, err)
	}
	return row, nil
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

func (t *table) data(r Reader) *tableData {
	d := r.tableData(t.name)
	if d == nil || d.def.typ != t.typ {
		panic(fmt.Sprintf("filestore: table %q is not in this store's schema", t.name))
	}
	return d
}

// Get returns the row with the given key.
func (t *Table[V]) Get(r Reader, key string) (V, bool) {
	row, ok := t.t.data(r).get(key)
	if !ok {
		var zero V
		return zero, false
	}
	return *row.(*V), true
}

// Len returns the number of rows.
func (t *Table[V]) Len(r Reader) int { return t.t.data(r).rows.Len() }

// Ascend calls fn for each row in key order until fn returns false.
func (t *Table[V]) Ascend(r Reader, fn func(V) bool) {
	t.t.data(r).rows.Ascend(func(it item) bool { return fn(*it.row.(*V)) })
}

// AscendFrom calls fn for each row with a key of at least from, in key order,
// until fn returns false.
func (t *Table[V]) AscendFrom(r Reader, from string, fn func(V) bool) {
	t.t.data(r).rows.AscendGreaterOrEqual(item{key: from}, func(it item) bool { return fn(*it.row.(*V)) })
}

// Descend calls fn for each row in reverse key order until fn returns false.
func (t *Table[V]) Descend(r Reader, fn func(V) bool) {
	t.t.data(r).rows.Descend(func(it item) bool { return fn(*it.row.(*V)) })
}

// Put inserts or replaces a row.
func (t *Table[V]) Put(tx *Tx, v V) error { return t.t.put(tx, &v, true) }

// Delete removes the row with the given key and reports whether it existed.
func (t *Table[V]) Delete(tx *Tx, key string) (bool, error) { return t.t.delete(tx, key) }

// put is Put, which checks the row's references to other rows.
func (t *table) put(tx *Tx, row any, checkRefs bool) error {
	if err := tx.check(); err != nil {
		return err
	}
	row, raw, err := t.normalize(row)
	if err != nil {
		return err
	}
	key := t.key(row)
	if err := checkKey(t.name, "primary", key); err != nil {
		return err
	}
	if checkRefs {
		if err := t.schema.checkReferences(tx, t.name, row); err != nil {
			return err
		}
	}
	if err := tx.writable(t.name).put(key, row); err != nil {
		return err
	}
	tx.muts = append(tx.muts, mutation{op: opPut, table: t.name, key: key, val: raw})
	return nil
}

// normalize returns a copy of row exactly as the store will read it back, so
// the live state and a rebuilt one are always the same (F25): through Parquet
// on a Delta table, through JSON (also returned, for the log) otherwise. It
// also keeps the caller's value from being shared with the store.
func (t *table) normalize(row any) (any, []byte, error) {
	if t.rowType != nil {
		row, err := t.rowType.Normalize(row)
		if err != nil {
			return nil, nil, fmt.Errorf("filestore: %s: encode row: %w", t.name, err)
		}
		return row, nil, nil
	}
	raw, err := json.Marshal(row)
	if err != nil {
		return nil, nil, fmt.Errorf("filestore: %s: encode row: %w", t.name, err)
	}
	row, err = t.decode(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("filestore: %s: row does not decode the way it encodes: %w", t.name, err)
	}
	return row, raw, nil
}

func (t *table) delete(tx *Tx, key string) (bool, error) {
	if err := tx.check(); err != nil {
		return false, err
	}
	if _, ok := t.data(tx).get(key); !ok {
		return false, nil
	}
	if err := t.schema.checkRestricted(tx, t.name, key); err != nil {
		return false, err
	}
	tx.writable(t.name).delete(key)
	tx.muts = append(tx.muts, mutation{op: opDelete, table: t.name, key: key})
	// The row goes first, so a cycle of cascades ends when it comes back to
	// a row already deleted. An error leaves the transaction to fail whole.
	if err := t.schema.cascade(tx, t.name, key); err != nil {
		return true, err
	}
	return true, nil
}

// Ascend calls fn, in primary key order, for each row that has key in the
// index, until fn returns false.
func (ix *Index[V]) Ascend(r Reader, key string, fn func(V) bool) {
	d := ix.ix.t.data(r)
	ix.ix.ascend(d, key, func(pk string) bool { return fn(*d.mustGet(pk, ix.ix.name).(*V)) })
}

// Count returns the number of rows that have key in the index.
func (ix *Index[V]) Count(r Reader, key string) int {
	n := 0
	ix.ix.ascend(ix.ix.t.data(r), key, func(string) bool { n++; return true })
	return n
}

// ascend calls fn, in order, for the primary key of each row that has key in
// the index, until fn returns false.
func (ix *index) ascend(d *tableData, key string, fn func(pk string) bool) {
	prefix := key + "\x00"
	d.idx[ix.pos].AscendGreaterOrEqual(prefix, func(e string) bool {
		return strings.HasPrefix(e, prefix) && fn(e[len(prefix):])
	})
}

// Get returns the row that has key in the unique index.
func (u *Unique[V]) Get(r Reader, key string) (V, bool) {
	d := u.u.t.data(r)
	e, ok := d.uniq[u.u.pos].Get(uniqEntry{key: key})
	if !ok {
		var zero V
		return zero, false
	}
	return *d.mustGet(e.pk, u.u.name).(*V), true
}

type item struct {
	key string
	row any // a pointer to a row
}

type uniqEntry struct{ key, pk string }

// tableData is one table's rows and indexes, as copy-on-write B-trees, so
// cloning is O(1) and a clone handed to readers is never affected by later
// writes (F10). One that has been published to readers is never modified
// again; writers work on a clone.
type tableData struct {
	def  *table
	rows *btree.BTreeG[item]
	idx  []*btree.BTreeG[string] // entries are index key + "\x00" + primary key
	uniq []*btree.BTreeG[uniqEntry]
}

func (t *table) newData() *tableData {
	d := &tableData{
		def:  t,
		rows: btree.NewG(btreeDegree, func(a, b item) bool { return a.key < b.key }),
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

func (d *tableData) clone() *tableData {
	c := &tableData{
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

func (d *tableData) length() int { return d.rows.Len() }

func (d *tableData) get(key string) (row any, ok bool) {
	it, ok := d.rows.Get(item{key: key})
	return it.row, ok
}

// mustGet returns a row that an index says there is.
func (d *tableData) mustGet(key, index string) any {
	row, ok := d.get(key)
	if !ok {
		panic(fmt.Sprintf("filestore: %s.%s: index entry without a row", d.def.name, index))
	}
	return row
}

// put validates every index key and unique constraint before changing
// anything, so a failed put leaves the table as it was.
func (d *tableData) put(key string, row any) error {
	ikeys := make([][]string, len(d.def.indexes))
	for i, ix := range d.def.indexes {
		ikeys[i] = ix.keys(row)
		for _, k := range ikeys[i] {
			if err := checkKey(d.def.name, ix.name, k); err != nil {
				return err
			}
		}
	}
	ukeys := make([]string, len(d.def.uniques))
	has := make([]bool, len(d.def.uniques))
	for i, u := range d.def.uniques {
		k, ok := u.key(row)
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

	if old, ok := d.get(key); ok {
		d.removeIndexes(key, old)
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
	d.rows.ReplaceOrInsert(item{key: key, row: row})
	return nil
}

func (d *tableData) removeIndexes(key string, old any) {
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

func (d *tableData) delete(key string) bool {
	old, ok := d.get(key)
	if !ok {
		return false
	}
	d.removeIndexes(key, old)
	d.rows.Delete(item{key: key})
	return true
}

func (d *tableData) applyPut(key string, raw []byte) error {
	row, err := d.def.decode(raw)
	if err != nil {
		return err
	}
	if got := d.def.key(row); got != key {
		return corruptf("table %s: row stored under key %q has key %q", d.def.name, key, got)
	}
	if err := d.put(key, row); err != nil {
		return corruptf("table %s: %v", d.def.name, err)
	}
	return nil
}

func (d *tableData) applyDelete(key string) error {
	if !d.delete(key) {
		return corruptf("table %s: delete of missing row %q", d.def.name, key)
	}
	return nil
}

func (d *tableData) encodeRows(fn func(key string, raw []byte) error) error {
	var err error
	d.rows.Ascend(func(it item) bool {
		var raw []byte
		raw, err = json.Marshal(it.row)
		if err == nil {
			err = fn(it.key, raw)
		}
		return err == nil
	})
	return err
}

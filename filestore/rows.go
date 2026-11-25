package filestore

import (
	"fmt"
	"reflect"
	"strings"
)

// Rows is access to a table for code that does not know the type of its rows,
// such as tests and tools. A row is a pointer to a struct whose fields have
// parquet tags. Those that are read are shared with the store: copy a row to
// change it.
type Rows struct{ t *table }

// Rows returns access to the rows of a table, if there is one by that name.
func (s *Schema) Rows(table string) (Rows, bool) {
	t, ok := s.tables[table]
	if !ok || strings.HasPrefix(table, "_") {
		return Rows{}, false
	}
	return Rows{t}, true
}

// Schema is the schema of the store.
func (db *DeltaDB) Schema() *Schema { return db.schema }

// Type is the type that rows are pointers to.
func (r Rows) Type() reflect.Type { return r.t.typ }

func (r Rows) check(row any) error {
	if want := reflect.PointerTo(r.t.typ); reflect.TypeOf(row) != want || reflect.ValueOf(row).IsNil() {
		return fmt.Errorf("filestore: %s: a row is a %s, not a %T", r.t.name, want, row)
	}
	return nil
}

// Key returns a row's primary key.
func (r Rows) Key(row any) (string, error) {
	if err := r.check(row); err != nil {
		return "", err
	}
	return r.t.key(row), nil
}

// Get returns the row with the given key.
func (r Rows) Get(reader Reader, key string) (row any, ok bool) { return r.t.data(reader).get(key) }

// Ascend calls fn for each row in key order until fn returns false.
func (r Rows) Ascend(reader Reader, fn func(row any) bool) {
	r.t.data(reader).rows.Ascend(func(it item) bool { return fn(it.row) })
}

// Insert adds a row, see Table.Insert.
func (r Rows) Insert(tx *Tx, row any, pkey string) error {
	if err := r.check(row); err != nil {
		return err
	}
	return r.t.insert(tx, row, pkey)
}

// Put inserts or replaces a row.
func (r Rows) Put(tx *Tx, row any) error {
	if err := r.check(row); err != nil {
		return err
	}
	return r.t.put(tx, row, true)
}

// Delete removes the row with the given key and reports whether it existed.
func (r Rows) Delete(tx *Tx, key string) (bool, error) { return r.t.delete(tx, key) }

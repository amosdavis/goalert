package filestore

import "fmt"

// OnDelete is what happens to the rows that refer to a row being deleted.
type OnDelete int

const (
	// Restrict refuses to delete a row while other rows refer to it, like
	// Postgres's default NO ACTION.
	Restrict OnDelete = iota
	// Cascade deletes the rows that refer to it too (ON DELETE CASCADE).
	Cascade
	// SetNull clears their reference (ON DELETE SET NULL).
	SetNull
)

// ForeignKeyError reports a write that would break a foreign key (F7).
type ForeignKeyError struct {
	Table, Key string // the row written or deleted
	ForeignKey string
	// Missing means the row refers to a parent that doesn't exist;
	// otherwise a parent was deleted while rows still refer to it.
	Missing bool
}

func (e *ForeignKeyError) Error() string {
	if e.Missing {
		return fmt.Sprintf("filestore: %s %q: foreign key %s refers to a missing row", e.Table, e.Key, e.ForeignKey)
	}
	return fmt.Sprintf("filestore: %s %q is still referred to through foreign key %s", e.Table, e.Key, e.ForeignKey)
}

// foreignKey is a foreign key, with the typed work done by closures.
type foreignKey struct {
	name          string
	child, parent string
	onDelete      OnDelete

	// check returns an error if a child row refers to a missing parent.
	check func(r Reader, row any) error
	// children returns the keys of the child rows that refer to parentKey.
	children func(r Reader, parentKey string) []string
	// remove deletes a child row; clear removes its reference.
	remove func(tx *Tx, childKey string) error
	clear  func(tx *Tx, childKey string) error
}

// References declares a foreign key named name: child rows refer to a row of
// parent by its primary key, which ref returns, with false for a row that
// refers to none (a null reference). Writing a child row checks that its
// parent exists, and deleting a parent applies onDelete to the rows that
// refer to it, in the same transaction (F2, F7). For SetNull, clear returns a
// row without the reference; otherwise it is unused and may be nil.
//
// The checks run in transactions, not when a commit already made, by another
// pod or before a restart, is applied. That stays safe with several pods: a
// transaction made on top of a parent's deletion runs again on the new state
// and fails its check (F4).
func References[C, P any](child *Table[C], name string, parent *Table[P], ref func(C) (string, bool), onDelete OnDelete, clear func(C) C) {
	var clearRow func(row any) any
	if clear != nil {
		clearRow = func(row any) any {
			c := clear(*row.(*C))
			return &c
		}
	}
	references(child.t, name, parent.t, func(row any) (string, bool) { return ref(*row.(*C)) }, onDelete, clearRow)
}

func references(child *table, name string, parent *table, ref func(row any) (string, bool), onDelete OnDelete, clear func(row any) any) {
	s := child.schema
	s.checkOpen()
	if parent.schema != s {
		panic(fmt.Sprintf("filestore: foreign key %s: tables of different schemas", name))
	}
	if onDelete == SetNull && clear == nil {
		panic(fmt.Sprintf("filestore: foreign key %s: SetNull needs clear", name))
	}
	ix := child.addIndex("_fk_"+name, func(row any) []string {
		if k, ok := ref(row); ok {
			return []string{k}
		}
		return nil
	})
	s.fks = append(s.fks, &foreignKey{
		name: name, child: child.name, parent: parent.name, onDelete: onDelete,
		check: func(r Reader, row any) error {
			k, ok := ref(row)
			if !ok {
				return nil
			}
			if _, found := parent.data(r).get(k); !found {
				return &ForeignKeyError{Table: child.name, Key: child.key(row), ForeignKey: name, Missing: true}
			}
			return nil
		},
		children: func(r Reader, parentKey string) []string {
			var keys []string
			ix.ascend(child.data(r), parentKey, func(pk string) bool {
				keys = append(keys, pk)
				return true
			})
			return keys
		},
		remove: func(tx *Tx, childKey string) error {
			_, err := child.delete(tx, childKey)
			return err
		},
		clear: func(tx *Tx, childKey string) error {
			row, ok := child.data(tx).get(childKey)
			if !ok {
				return nil
			}
			// Clearing a reference leaves no new one to check. The row's
			// others may refer to a row that this delete takes away too, and
			// their foreign keys deal with that.
			return child.put(tx, clear(row), false)
		},
	})
}

// ForeignKey describes a foreign key that was declared.
type ForeignKey struct {
	Name, Child, Parent string
	OnDelete            OnDelete
}

// ForeignKeys returns the foreign keys that were declared, in that order.
func (s *Schema) ForeignKeys() []ForeignKey {
	out := make([]ForeignKey, len(s.fks))
	for i, fk := range s.fks {
		out[i] = ForeignKey{fk.name, fk.child, fk.parent, fk.onDelete}
	}
	return out
}

// checkReferences returns an error if a row of table refers to a missing row.
func (s *Schema) checkReferences(tx *Tx, table string, row any) error {
	for _, fk := range s.fks {
		if fk.child == table {
			if err := fk.check(tx, row); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkRestricted returns an error if a Restrict foreign key still refers
// to row key of table.
func (s *Schema) checkRestricted(tx *Tx, table, key string) error {
	for _, fk := range s.fks {
		if fk.parent == table && fk.onDelete == Restrict && len(fk.children(tx, key)) > 0 {
			return &ForeignKeyError{Table: table, Key: key, ForeignKey: fk.name}
		}
	}
	return nil
}

// cascade applies the foreign keys' actions to the rows that referred to
// row key of table, which was just deleted.
func (s *Schema) cascade(tx *Tx, table, key string) error {
	for _, fk := range s.fks {
		if fk.parent != table || fk.onDelete == Restrict {
			continue
		}
		for _, ck := range fk.children(tx, key) {
			var err error
			if fk.onDelete == Cascade {
				err = fk.remove(tx, ck)
			} else {
				err = fk.clear(tx, ck)
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

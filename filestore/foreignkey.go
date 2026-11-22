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

	// check returns an error if child row v refers to a missing parent.
	check func(r Reader, v any) error
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
	s := child.schema
	s.checkOpen()
	if parent.schema != s {
		panic(fmt.Sprintf("filestore: foreign key %s: tables of different schemas", name))
	}
	if onDelete == SetNull && clear == nil {
		panic(fmt.Sprintf("filestore: foreign key %s: SetNull needs clear", name))
	}
	ix := child.Index("_fk_"+name, func(c C) []string {
		if k, ok := ref(c); ok {
			return []string{k}
		}
		return nil
	})
	s.fks = append(s.fks, &foreignKey{
		name: name, child: child.name, parent: parent.name, onDelete: onDelete,
		check: func(r Reader, v any) error {
			c := v.(C)
			k, ok := ref(c)
			if !ok {
				return nil
			}
			if _, found := parent.Get(r, k); !found {
				return &ForeignKeyError{Table: child.name, Key: child.keyFn(c), ForeignKey: name, Missing: true}
			}
			return nil
		},
		children: func(r Reader, parentKey string) []string {
			var keys []string
			ix.Ascend(r, parentKey, func(c C) bool {
				keys = append(keys, child.keyFn(c))
				return true
			})
			return keys
		},
		remove: func(tx *Tx, childKey string) error {
			_, err := child.Delete(tx, childKey)
			return err
		},
		clear: func(tx *Tx, childKey string) error {
			c, ok := child.Get(tx, childKey)
			if !ok {
				return nil
			}
			return child.Put(tx, clear(c))
		},
	})
}

// checkReferences returns an error if row v of table refers to a missing
// row.
func (s *Schema) checkReferences(tx *Tx, table string, v any) error {
	for _, fk := range s.fks {
		if fk.child == table {
			if err := fk.check(tx, v); err != nil {
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

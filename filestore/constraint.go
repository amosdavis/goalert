package filestore

import "fmt"

// Constraint errors describe themselves in Postgres's terms (SQLSTATE,
// constraint and table names), which sqlutil.MapError understands, so the
// code that turns a broken constraint into a message for the user works the
// same on both backends (F22). For that, indexes, foreign keys and checks
// carry the names of the Postgres constraints they stand for.

// SQLSTATE codes of the constraints.
const (
	stateForeignKey = "23503"
	stateUnique     = "23505"
	stateCheck      = "23514"
)

func (e *UniqueError) SQLState() string       { return stateUnique }
func (e *UniqueError) ConstraintName() string { return e.Index }
func (e *UniqueError) TableName() string      { return e.Table }
func (e *UniqueError) Detail() string         { return fmt.Sprintf("Key (%s) already exists.", e.Key) }
func (e *UniqueError) Hint() string           { return "" }

func (e *ForeignKeyError) SQLState() string       { return stateForeignKey }
func (e *ForeignKeyError) ConstraintName() string { return e.ForeignKey }
func (e *ForeignKeyError) TableName() string      { return e.Table }
func (e *ForeignKeyError) Hint() string           { return "" }
func (e *ForeignKeyError) Detail() string {
	if e.Missing {
		return "The row it refers to is not present."
	}
	return "The row is still referenced."
}

// CheckError reports a row that breaks a CHECK constraint, or a rule that
// Postgres enforced with a trigger, such as a limit (F8).
type CheckError struct {
	Table, Constraint string
	// Message and HintText are those of the trigger, such as "limit
	// exceeded" and "max=5".
	Message, HintText string
}

func (e *CheckError) Error() string {
	msg := e.Message
	if msg == "" {
		msg = "violates check constraint"
	}
	return fmt.Sprintf("filestore: %s: %s (%s)", e.Table, msg, e.Constraint)
}

func (e *CheckError) SQLState() string       { return stateCheck }
func (e *CheckError) ConstraintName() string { return e.Constraint }
func (e *CheckError) TableName() string      { return e.Table }
func (e *CheckError) Detail() string         { return "" }
func (e *CheckError) Hint() string           { return e.HintText }

// Insert adds a row, and unlike Put refuses to replace one: it returns a
// *UniqueError for pkey, the name of the primary key constraint.
func (t *Table[V]) Insert(tx *Tx, v V, pkey string) error { return t.t.insert(tx, &v, pkey) }

func (t *table) insert(tx *Tx, row any, pkey string) error {
	key := t.key(row)
	if _, ok := t.data(tx).get(key); ok {
		return &UniqueError{Table: t.name, Index: pkey, Key: key}
	}
	return t.put(tx, row, true)
}

// Ref returns an optional reference as References takes it.
func Ref(p *string) (string, bool) {
	if p == nil {
		return "", false
	}
	return *p, true
}

// IntKey is the key of a row with an integer primary key. It is padded, so
// keys of ids from a sequence sort in the order the ids were handed out.
func IntKey(id int64) string { return fmt.Sprintf("%020d", id) }

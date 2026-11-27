// Package sqlutil describes a broken constraint the way Postgres did, by its
// SQLSTATE and constraint name. The Delta backend's constraint errors describe
// themselves that way (filestore.UniqueError and the others), so the code that
// turns one into a message for the user stays the same (F8, F22).
package sqlutil

import "github.com/pkg/errors"

// Error represents a driver-agnostic SQL error.
type Error struct {
	err            error
	Code           string
	Message        string
	Detail         string
	Hint           string
	TableName      string
	ConstraintName string
	Where          string
	ColumnName     string
	Position       int
}

func (e Error) Error() string { return e.err.Error() }

// constraintError is a broken constraint, in Postgres's terms.
type constraintError interface {
	error
	SQLState() string
	ConstraintName() string
	TableName() string
	Detail() string
	Hint() string
}

// MapError will return a Error from the given err object or nil otherwise.
func MapError(err error) *Error {
	var e Error
	if errors.As(err, &e) {
		return &e
	}

	var ce constraintError
	if errors.As(err, &ce) {
		return &Error{
			err:            err,
			Code:           ce.SQLState(),
			Message:        ce.Error(),
			Detail:         ce.Detail(),
			Hint:           ce.Hint(),
			TableName:      ce.TableName(),
			ConstraintName: ce.ConstraintName(),
		}
	}

	return nil
}

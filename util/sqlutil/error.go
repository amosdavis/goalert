package sqlutil

import (
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pkg/errors"
)

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

// constraintError is a broken constraint on a backend other than Postgres,
// in Postgres's terms.
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
	var pgxErr *pgconn.PgError

	if errors.As(err, &pgxErr) {
		return &Error{
			err:            err,
			Code:           pgxErr.Code,
			Message:        pgxErr.Message,
			Detail:         pgxErr.Detail,
			Hint:           pgxErr.Hint,
			Where:          pgxErr.Where,
			TableName:      pgxErr.TableName,
			ConstraintName: pgxErr.ConstraintName,
			ColumnName:     pgxErr.ColumnName,
			Position:       int(pgxErr.Position),
		}
	}

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

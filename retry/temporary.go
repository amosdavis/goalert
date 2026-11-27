package retry

import (
	"context"
	"net"

	"github.com/pkg/errors"
)

type clientErr interface {
	ClientError() bool
}

type tempErr interface {
	Temporary() bool
}

// TemporaryError returns an error that will always be classified as temporary.
func TemporaryError(err error) error {
	return tempErrWrap{error: err}
}

type tempErrWrap struct {
	error
}

func (e tempErrWrap) Temporary() bool { return true }

// IsTemporaryError will determine if an error is temporary, and thus
// the action can/should be retried.
func IsTemporaryError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var cliErr clientErr
	if errors.As(err, &cliErr) && cliErr.ClientError() {
		return false
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	var tempErr tempErr
	if errors.As(err, &tempErr) && tempErr.Temporary() {
		return true
	}

	return false
}

// DoTempFunc is a simplified version of DoFunc that just returns an error value.
type DoTempFunc func(int) error

// DoTemporaryError will retry as long as the error returned from fn is
// temporary as defined by IsTemporaryError.
func DoTemporaryError(fn func(attempt int) error, opts ...Option) error {
	return Do(func(n int) (bool, error) {
		err := fn(n)
		return IsTemporaryError(err), err
	}, opts...)
}

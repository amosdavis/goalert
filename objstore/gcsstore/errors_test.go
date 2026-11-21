package gcsstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/target/goalert/objstore"
	"google.golang.org/api/googleapi"
)

func TestErrors(t *testing.T) {
	gerr := func(code int) error { return &googleapi.Error{Code: code} }

	assert.ErrorIs(t, writeErr("x", gerr(412)), objstore.ErrExists)
	for _, code := range []int{401, 403} {
		err := writeErr("x", gerr(code))
		assert.ErrorIs(t, err, objstore.ErrDenied, "refused credentials are reported as such (F33)")
		assert.NotErrorIs(t, err, objstore.ErrUnknown, "a refused write did not happen")
		assert.ErrorIs(t, opErr("read", "x", gerr(code)), objstore.ErrDenied)
	}
	assert.NotErrorIs(t, writeErr("x", gerr(400)), objstore.ErrUnknown)
	assert.NotErrorIs(t, writeErr("x", gerr(400)), objstore.ErrDenied)
	assert.ErrorIs(t, writeErr("x", gerr(503)), objstore.ErrUnknown, "a server error may have applied the write")
	assert.NotErrorIs(t, opErr("read", "x", gerr(503)), objstore.ErrDenied)
}

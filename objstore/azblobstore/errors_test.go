package azblobstore

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/stretchr/testify/assert"
	"github.com/target/goalert/objstore"
)

func TestErrors(t *testing.T) {
	rerr := func(status int, code bloberror.Code) error {
		return &azcore.ResponseError{StatusCode: status, ErrorCode: string(code)}
	}

	assert.ErrorIs(t, writeErr("x", rerr(409, bloberror.BlobAlreadyExists)), objstore.ErrExists)
	assert.ErrorIs(t, writeErr("x", rerr(412, bloberror.ConditionNotMet)), objstore.ErrExists)
	for _, e := range []error{
		rerr(403, bloberror.AuthorizationFailure),
		rerr(403, bloberror.AuthenticationFailed),
		rerr(403, bloberror.AuthorizationPermissionMismatch),
		rerr(401, bloberror.InvalidAuthenticationInfo),
	} {
		err := writeErr("x", e)
		assert.ErrorIs(t, err, objstore.ErrDenied, "refused credentials are reported as such (F33)")
		assert.NotErrorIs(t, err, objstore.ErrUnknown, "a refused write did not happen")
		assert.ErrorIs(t, opErr("read", "x", e), objstore.ErrDenied)
	}
	assert.ErrorIs(t, writeErr("x", rerr(500, bloberror.InternalError)), objstore.ErrUnknown)
	assert.ErrorIs(t, writeErr("x", rerr(408, bloberror.OperationTimedOut)), objstore.ErrUnknown)
}

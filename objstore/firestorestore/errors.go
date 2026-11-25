package firestorestore

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/target/goalert/objstore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// errExists means a create found the document already there.
var errExists = errors.New("firestorestore: document already exists")

// rejected reports whether a code means the server did not apply a write.
// Other codes, and errors that are not the server's, leave that open.
func rejected(code codes.Code) bool {
	switch code {
	case codes.InvalidArgument, codes.NotFound, codes.AlreadyExists, codes.PermissionDenied, codes.Unauthenticated,
		codes.ResourceExhausted, codes.FailedPrecondition, codes.Aborted, codes.OutOfRange, codes.Unimplemented:
		return true
	}
	return false
}

func denied(err error) bool {
	code := status.Code(err)
	return code == codes.PermissionDenied || code == codes.Unauthenticated
}

// attempts notes what happened to the attempts of one write.
type attempts struct {
	// unsure means an attempt may have been applied.
	unsure atomic.Bool
}

type attemptsKey struct{}

// trackAttempts sees every attempt of a call, including those the client
// library retries.
func trackAttempts(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	if a, ok := ctx.Value(attemptsKey{}).(*attempts); ok && err != nil && !rejected(status.Code(err)) {
		a.unsure.Store(true)
	}
	return err
}

// write does a write and classifies its error. It repeats one that ran into
// another write to the same document (Aborted), which was not applied.
func write(ctx context.Context, op string, fn func(ctx context.Context) error) error {
	a := &attempts{}
	actx := context.WithValue(ctx, attemptsKey{}, a)
	delay := 10 * time.Millisecond
	var err error
	for attempt := 1; ; attempt++ {
		err = fn(actx)
		if status.Code(err) != codes.Aborted || attempt == 8 {
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(delay/2 + rand.N(delay)):
			delay *= 2
			continue
		}
		break
	}
	if err == nil {
		return nil
	}
	code := status.Code(err)
	err = fmt.Errorf("firestorestore: %s: %w", op, err)
	switch {
	case code == codes.AlreadyExists:
		// Perhaps made by an earlier attempt, which callers check for.
		return fmt.Errorf("%w: %w", errExists, err)
	case a.unsure.Load() || !rejected(code):
		return objstore.Unknown(err)
	case denied(err):
		return objstore.Denied(err)
	}
	return err
}

// opErr wraps an error from an operation other than a write.
func opErr(op string, err error) error {
	err = fmt.Errorf("firestorestore: %s: %w", op, err)
	if denied(err) {
		return objstore.Denied(err)
	}
	return err
}

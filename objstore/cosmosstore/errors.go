package cosmosstore

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/target/goalert/objstore"
)

func statusCode(err error) int {
	var rerr *azcore.ResponseError
	if errors.As(err, &rerr) {
		return rerr.StatusCode
	}
	return 0
}

// notFound reports whether an item does not exist. Cosmos DB answers 404 for
// other reasons too, such as a container that was deleted, or a region that
// lags behind, but those carry a substatus (the error code the SDK gives
// them), and must not be taken for a missing commit (F16, F44).
func notFound(err error) bool {
	var rerr *azcore.ResponseError
	if !errors.As(err, &rerr) || rerr.StatusCode != http.StatusNotFound {
		return false
	}
	if rerr.RawResponse == nil {
		return false
	}
	sub := rerr.RawResponse.Header.Get("x-ms-substatus")
	return sub == "" || sub == "0"
}

// denied reports whether Cosmos DB refused the credentials or a permission
// (F33). It also answers 403 when a partition is full or the account is out of
// quota (substatus 1014 and others), but a write refused that way did not
// happen either, and needs an operator just the same.
func denied(err error) bool {
	status := statusCode(err)
	return status == http.StatusUnauthorized || status == http.StatusForbidden
}

// opErr wraps an error from an operation other than a write.
func opErr(op string, err error) error {
	err = fmt.Errorf("cosmosstore: %s: %w", op, err)
	if denied(err) {
		return objstore.Denied(err)
	}
	return err
}

// writeErr classifies an error from a write. A request the server rejected
// outright did not happen. Anything else (a timeout, a dropped connection, a
// server error) may have.
func writeErr(op string, err error) error {
	if err == nil {
		return nil
	}
	err = fmt.Errorf("cosmosstore: %s: %w", op, err)
	if denied(err) {
		return objstore.Denied(err)
	}
	if status := statusCode(err); status >= 400 && status < 500 && status != http.StatusRequestTimeout {
		return err
	}
	return objstore.Unknown(err)
}

package nonce

import (
	"context"
	"time"

	"github.com/target/goalert/util/log"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// Store allows generating and consuming nonce values.
type Store struct {
	logger   *log.Logger
	shutdown chan context.Context

	delta *deltaStore // delta.go
}

func (s *Store) loop() {
	defer close(s.shutdown)
	t := time.NewTicker(time.Hour * 24)
	defer t.Stop()
	ctx := s.logger.BackgroundContext()
	for {
		select {
		case <-t.C:
			err := s.delta.cleanup(ctx)
			if err != nil {
				log.Log(ctx, errors.Wrap(err, "cleanup old nonce values"))
			}
		case <-s.shutdown:
			return
		}
	}
}

// Shutdown allows gracefully shutting down the nonce store.
func (s *Store) Shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.shutdown <- ctx

	// wait for it to complete
	<-s.shutdown
	return nil
}

// New will generate a new cryptographically random nonce value.
func (s *Store) New() [16]byte { return uuid.New() }

// Consume will record the use of a nonce value.
//
// An error is returned if it is not possible to validate the nonce value.
// Otherwise true/false is returned to indicate if the id is valid.
//
// The first call to Consume for a given ID will return true, subsequent calls
// for the same ID will return false.
func (s *Store) Consume(ctx context.Context, id [16]byte) (bool, error) {
	return s.delta.consume(ctx, uuid.UUID(id))
}

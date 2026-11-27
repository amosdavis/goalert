package engine

import (
	"context"
	"time"

	"github.com/target/goalert/validation/validate"

	"github.com/google/uuid"
)

type callback struct {
	ID              string
	AlertID         int
	ServiceID       string
	ContactMethodID uuid.UUID
	CreatedAt       time.Time
}

// findCallback finds the message that a callback is about.
func (p *Engine) findCallback(ctx context.Context, id string) (*callback, error) {
	err := validate.UUID("CallbackID", id)
	if err != nil {
		return nil, err
	}
	return p.delta.findCallback(ctx, id)
}

func (c callback) Normalize() (*callback, error) {
	if c.ID == "" {
		c.ID = uuid.New().String()
	}
	err := validate.Many(
		validate.UUID("ID", c.ID),
	)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

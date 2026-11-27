package alertlog

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/search"
	"github.com/target/goalert/validation/validate"
)

// SearchOptions contains criteria for filtering alert logs.
type SearchOptions struct {
	// FilterAlertIDs restricts the log entries belonging to specific alertIDs only.
	FilterAlertIDs []int `json:"f"`

	// FilterServiceID restricts the log entries to those alerts that belong to a specific service.
	FilterServiceID *uuid.UUID `json:"i,omitempty"`

	// Limit restricts the maximum number of rows returned. Default is 15.
	Limit int `json:"-"`

	After SearchCursor `json:"a,omitempty"`

	Since *time.Time `json:"s,omitempty"`
}

type SearchCursor struct {
	ID int `json:"i,omitempty"`
}

type renderData SearchOptions

func (opts renderData) Normalize() (*renderData, error) {
	if opts.Limit == 0 {
		opts.Limit = search.DefaultMaxResults
	}

	err := validate.Many(
		validate.Range("FilterAlertIDs", len(opts.FilterAlertIDs), 0, 50),
		validate.Range("Limit", opts.Limit, 0, search.MaxResults),
	)
	if err != nil {
		return nil, err
	}

	return &opts, nil
}

// Search will return a list of matching log entries
func (s *Store) Search(ctx context.Context, opts *SearchOptions) ([]Entry, error) {
	if opts == nil {
		opts = &SearchOptions{}
	}

	err := permission.LimitCheckAny(ctx, permission.System, permission.Admin, permission.User)
	if err != nil {
		return nil, err
	}

	data, err := (*renderData)(opts).Normalize()
	if err != nil {
		return nil, err
	}

	return s.delta.search(ctx, data)
}

package alert

import (
	"context"
	"strconv"
	"time"

	"github.com/target/goalert/permission"
	"github.com/target/goalert/search"
	"github.com/target/goalert/validation/validate"
)

// SortMode indicates the mode of sorting for alerts.
type SortMode int

const (
	// SortModeStatusID will sort by status priority (unacked, then acked, then closed) followed by ID (newest/highest first)
	SortModeStatusID SortMode = iota

	// SortModeDateID will sort alerts by date newest first, falling back to ID (newest/highest first)
	SortModeDateID

	// SortModeDateIDReverse will sort alerts by date oldest first, falling back to ID (oldest/lowest first)
	SortModeDateIDReverse
)

// SearchOptions contains criteria for filtering and sorting alerts.
type SearchOptions struct {
	// Search is matched case-insensitive against the alert summary, id and service name.
	Search string `json:"s,omitempty"`

	// Status, if specified, will restrict alerts to those with a matching status.
	Status []Status `json:"t,omitempty"`

	// ServiceFilter, if specified, will restrict alerts to those with a matching ServiceID on IDs, if valid.
	ServiceFilter IDFilter `json:"v,omitempty"`

	After SearchCursor `json:"a,omitempty"`

	// Omit specifies a list of alert IDs to exclude from the results.
	Omit []int `json:"o,omitempty"`

	// NotifiedUserID will include all alerts the specified user has been
	// notified for to the results.
	NotifiedUserID string `json:"e,omitempty"`

	// Limit restricts the maximum number of rows returned. Default is 50.
	// Note: Limit is applied AFTER AfterID is taken into account.
	Limit int `json:"-"`

	// Sort allows customizing the sort method.
	Sort SortMode `json:"z,omitempty"`

	// NotBefore will omit any alerts created any time before the provided time.
	NotBefore time.Time `json:"n,omitempty"`

	// Before will only include alerts that were created before the provided time.
	Before time.Time `json:"b,omitempty"`
}

type IDFilter struct {
	Valid bool     `json:"v,omitempty"`
	IDs   []string `json:"i,omitempty"`
}

type SearchCursor struct {
	ID      int       `json:"i,omitempty"`
	Status  Status    `json:"s,omitempty"`
	Created time.Time `json:"c,omitempty"`
}

type renderData SearchOptions

func (opts renderData) Normalize() (*renderData, error) {
	if opts.Limit == 0 {
		opts.Limit = search.DefaultMaxResults
	}

	err := validate.Many(
		validate.Search("Search", opts.Search),
		validate.Range("Limit", opts.Limit, 0, 1001),
		validate.Range("Status", len(opts.Status), 0, 3),
		validate.ManyUUID("Services", opts.ServiceFilter.IDs, 50),
		validate.Range("Omit", len(opts.Omit), 0, 50),
		validate.OneOf("Sort", opts.Sort, SortModeStatusID, SortModeDateID, SortModeDateIDReverse),
	)
	if opts.After.Status != "" {
		err = validate.Many(err, validate.OneOf("After.Status", opts.After.Status, StatusTriggered, StatusActive, StatusClosed))
	}
	if err != nil {
		return nil, err
	}

	for i, stat := range opts.Status {
		err = validate.OneOf("Status["+strconv.Itoa(i)+"]", stat, StatusTriggered, StatusActive, StatusClosed)
		if err != nil {
			return nil, err
		}
	}

	return &opts, err
}

func (s *Store) Search(ctx context.Context, opts *SearchOptions) ([]Alert, error) {
	err := permission.LimitCheckAny(ctx, permission.System, permission.User)
	if err != nil {
		return nil, err
	}
	if opts == nil {
		opts = new(SearchOptions)
	}

	data, err := (*renderData)(opts).Normalize()
	if err != nil {
		return nil, err
	}

	return s.delta.search(ctx, data)
}

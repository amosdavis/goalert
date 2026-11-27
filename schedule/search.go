package schedule

import (
	"context"

	"github.com/target/goalert/permission"
	"github.com/target/goalert/search"
	"github.com/target/goalert/validation/validate"
)

// SearchOptions allow filtering and paginating the list of schedules.
type SearchOptions struct {
	Search string `json:"s,omitempty"`

	// FavoritesUserID specifies the UserID whose favorite services want to be displayed.
	FavoritesUserID string `json:"u,omitempty"`

	// FavoritesOnly controls filtering the results to those marked as favorites by FavoritesUserID.
	FavoritesOnly bool `json:"g,omitempty"`

	// FavoritesFirst indicates that services marked as favorite (by FavoritesUserID) should be returned first (before any non-favorites).
	FavoritesFirst bool `json:"f,omitempty"`

	After SearchCursor `json:"a,omitempty"`

	// Omit specifies a list of schedule IDs to exclude from the results.
	Omit []string `json:"o,omitempty"`

	// Only specifies a list of schedule IDs to limit the results to.
	Only []string `json:"n,omitempty"`

	Limit int `json:"-"`
}

// SearchCursor is used to indicate a position in a paginated list.
type SearchCursor struct {
	Name       string `json:"n,omitempty"`
	IsFavorite bool   `json:"f"`
}

type renderData SearchOptions

func (opts renderData) Normalize() (*renderData, error) {
	if opts.Limit == 0 {
		opts.Limit = search.DefaultMaxResults
	}
	err := validate.Many(
		validate.Search("Search", opts.Search),
		validate.Range("Limit", opts.Limit, 0, search.MaxResults),
		validate.ManyUUID("Omit", opts.Omit, 50),
		validate.ManyUUID("Only", opts.Only, 50),
	)
	if opts.After.Name != "" {
		err = validate.Many(err, validate.IDName("After.Name", opts.After.Name))
	}
	if opts.FavoritesOnly || opts.FavoritesFirst || opts.FavoritesUserID != "" {
		err = validate.Many(err, validate.UUID("FavoritesUserID", opts.FavoritesUserID))
	}
	if err != nil {
		return nil, err
	}

	return &opts, err
}

func (store *Store) Search(ctx context.Context, opts *SearchOptions) ([]Schedule, error) {
	if opts == nil {
		opts = &SearchOptions{}
	}
	userCheck := permission.User
	if opts.FavoritesUserID != "" {
		userCheck = permission.MatchUser(opts.FavoritesUserID)
	}
	err := permission.LimitCheckAny(ctx, permission.System, userCheck)
	if err != nil {
		return nil, err
	}
	data, err := (*renderData)(opts).Normalize()
	if err != nil {
		return nil, err
	}
	return store.delta.search(ctx, (*SearchOptions)(data))
}

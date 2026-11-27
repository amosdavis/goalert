package service

import (
	"context"
	"strings"

	"github.com/target/goalert/permission"
	"github.com/target/goalert/search"
	"github.com/target/goalert/validation/validate"
)

// SearchOptions contains criteria for filtering and sorting services.
type SearchOptions struct {
	// Search is matched case-insensitive against the service name and description.
	Search string `json:"s,omitempty"`

	// FavoritesUserID specifies the UserID whose favorite services want to be displayed.
	FavoritesUserID string `json:"u,omitempty"`

	// FavoritesOnly controls filtering the results to those marked as favorites by FavoritesUserID.
	FavoritesOnly bool `json:"o,omitempty"`

	// Omit specifies a list of service IDs to exclude from the results.
	Omit []string `json:"m,omitempty"`

	// Only lookup the service IDs present in the request.
	Only []string `json:"n,omitempty"`

	// FavoritesFirst indicates that services marked as favorite (by FavoritesUserID) should be returned first (before any non-favorites).
	FavoritesFirst bool `json:"f,omitempty"`

	// Limit will limit the number of results.
	Limit int `json:"-"`

	After SearchCursor `json:"a,omitempty"`
}

type SearchCursor struct {
	Name       string `json:"n"`
	IsFavorite bool   `json:"f"`
}

type renderData SearchOptions

func (opts renderData) IntegrationKey() string {
	if !strings.Contains(opts.Search, "token=") {
		return ""
	}
	return opts.Search[6:42]
}

func (opts renderData) LabelKey() string {
	searchStr := opts.Search
	if strings.Contains(opts.Search, "token=") {
		// strip token string
		searchStr = opts.Search[42:]
		searchStr = strings.TrimSpace(searchStr)
	}
	idx := strings.IndexByte(searchStr, '=')
	if idx == -1 {
		return ""
	}
	return strings.TrimSuffix(searchStr[:idx], "!") // if `!=`` is used
}
func (opts renderData) LabelValue() string {
	searchStr := opts.Search
	if strings.Contains(opts.Search, "token=") {
		// strip token string
		searchStr = opts.Search[42:]
		searchStr = strings.TrimSpace(searchStr)
	}
	idx := strings.IndexByte(searchStr, '=')
	if idx == -1 {
		return ""
	}
	val := searchStr[idx+1:]
	if val == "" {
		return "*"
	}
	return val
}
func (opts renderData) LabelNegate() bool {
	idx := strings.IndexByte(opts.Search, '=')
	if idx < 1 {
		return false
	}

	return opts.Search[idx-1] == '!'
}

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
	if opts.IntegrationKey() != "" {
		err = validate.Search("IntegrationKey", opts.IntegrationKey())
	}
	if opts.LabelKey() != "" {
		err = validate.Search("LabelKey", opts.LabelKey())
		if opts.LabelValue() != "*" {
			err = validate.Many(err,
				validate.LabelValue("LabelValue", opts.LabelValue()),
			)
		}
	}
	if err != nil {
		return nil, err
	}

	return &opts, nil
}

// Search will return a list of matching services and the total number of matches available.
func (s *Store) Search(ctx context.Context, opts *SearchOptions) ([]Service, error) {
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
	return s.delta.search(ctx, data)
}

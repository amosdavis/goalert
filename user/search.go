package user

import (
	"context"
	"strings"

	"github.com/target/goalert/notification/nfydest"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/search"
	"github.com/target/goalert/validation"
	"github.com/target/goalert/validation/validate"
)

// SearchOptions allow filtering and paginating the list of users.
type SearchOptions struct {
	Search string       `json:"s,omitempty"`
	After  SearchCursor `json:"a,omitempty"`

	// Omit specifies a list of user IDs to exclude from the results.
	Omit []string `json:"o,omitempty"`

	// Only specifies a list of user IDs to limit the results to.
	Only []string `json:"n,omitempty"`

	Limit int `json:"-"`

	DestArgs map[string]string `json:"da,omitempty"`
	DestType string            `json:"dt,omitempty"`

	// FavoritesUserID specifies the UserID whose favorite users want to be displayed.
	FavoritesUserID string `json:"u,omitempty"`

	// FavoritesOnly controls filtering the results to those marked as favorites by FavoritesUserID.
	FavoritesOnly bool `json:"g,omitempty"`

	// FavoritesFirst indicates the user marked as favorite (by FavoritesUserID) should be returned first (before any non-favorites).
	FavoritesFirst bool `json:"f,omitempty"`
}

func (so *SearchOptions) FromNotifyOptions(ctx context.Context, opts nfydest.SearchOptions) error {
	so.Search = opts.Search
	so.Omit = opts.Omit
	so.Limit = opts.Limit
	if opts.Cursor != "" {
		err := search.ParseCursor(opts.Cursor, &so.After)
		if err != nil {
			return err
		}
	}
	so.FavoritesFirst = true
	so.FavoritesUserID = permission.UserID(ctx)
	return nil
}

// SearchCursor is used to indicate a position in a paginated list.
type SearchCursor struct {
	Name       string `json:"n,omitempty"`
	IsFavorite bool   `json:"f,omitempty"`
}

type renderData SearchOptions

func (opts renderData) SearchString() string {
	if opts.Email() != "" {
		return ""
	}

	return opts.Search
}

func (opts renderData) Email() string {
	if !strings.HasPrefix(opts.Search, "email=") {
		return ""
	}
	return strings.TrimPrefix(opts.Search, "email=")
}

func (opts renderData) Normalize() (*renderData, error) {
	if opts.Limit == 0 {
		opts.Limit = search.DefaultMaxResults
	}

	if opts.DestType != "" && opts.DestArgs == nil {
		return nil, validation.NewGenericError("DestArgs must be set when DestType is set")
	}

	err := validate.Many(
		validate.Search("Search", opts.Search),
		validate.Range("Limit", opts.Limit, 0, search.MaxResults),
		validate.ManyUUID("Omit", opts.Omit, 50),
		validate.ManyUUID("Only", opts.Only, 50),
	)
	if opts.After.Name != "" {
		err = validate.Many(err, validate.Name("After.Name", opts.After.Name))
	}
	if opts.FavoritesOnly || opts.FavoritesFirst || opts.FavoritesUserID != "" {
		err = validate.Many(err, validate.UUID("FavoritesUserID", opts.FavoritesUserID))
	}
	if err != nil {
		return nil, err
	}

	return &opts, err
}

func (u User) Cursor() (string, error) {
	return search.Cursor(SearchCursor{
		Name:       u.Name,
		IsFavorite: u.isUserFavorite,
	})
}

func (u User) AsField() nfydest.FieldValue {
	return nfydest.FieldValue{
		Value:      u.ID,
		Label:      u.Name,
		IsFavorite: u.isUserFavorite,
	}
}

// Search performs a paginated search of users with the given options.
func (s *Store) Search(ctx context.Context, opts *SearchOptions) ([]User, error) {
	err := permission.LimitCheckAny(ctx, permission.User)
	userCheck := permission.User
	if err != nil {
		return nil, err
	}
	if opts == nil {
		opts = &SearchOptions{}
	}
	data, err := (*renderData)(opts).Normalize()
	if err != nil {
		return nil, err
	}
	if opts.FavoritesUserID != "" {
		userCheck = permission.MatchUser(opts.FavoritesUserID)
	}
	err = permission.LimitCheckAny(ctx, permission.System, userCheck)
	if err != nil {
		return nil, err
	}
	return s.delta.search(ctx, data)
}

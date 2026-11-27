package integrationkey

import (
	"context"

	"github.com/target/goalert/permission"
	"github.com/target/goalert/search"
	"github.com/target/goalert/validation/validate"
)

// InKeySearchOptions allow filtering and paginating the list of rotations.
type InKeySearchOptions struct {
	Search string `json:"s,omitempty"`
	After  string `json:"a,omitempty"`

	// Omit specifies a list of key ids to exclude from the results.
	Omit []string `json:"o,omitempty"`

	Limit int `json:"-"`
}

type intKeyRenderData InKeySearchOptions

func (opts intKeyRenderData) Normalize() (*intKeyRenderData, error) {
	if opts.Limit == 0 {
		opts.Limit = search.DefaultMaxResults
	}

	err := validate.Many(
		validate.Search("Search", opts.Search),
		validate.Range("Limit", opts.Limit, 0, search.MaxResults),
		validate.Range("Omit", len(opts.Omit), 0, 50),
	)

	if err != nil {
		return nil, err
	}

	return &opts, err
}

func (s *Store) Search(ctx context.Context, opts *InKeySearchOptions) ([]IntegrationKey, error) {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return nil, err
	}
	if opts == nil {
		opts = &InKeySearchOptions{}
	}
	data, err := (*intKeyRenderData)(opts).Normalize()
	if err != nil {
		return nil, err
	}
	return s.delta.search(ctx, InKeySearchOptions(*data))
}

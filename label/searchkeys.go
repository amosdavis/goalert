package label

import (
	"context"
	"strconv"

	"github.com/target/goalert/permission"
	"github.com/target/goalert/search"
	"github.com/target/goalert/validation/validate"
)

// KeySearchOptions allow filtering and paginating the list of rotations.
type KeySearchOptions struct {
	Search string `json:"s,omitempty"`
	After  string `json:"a,omitempty"`

	// Omit specifies a list of key names to exclude from the results.
	Omit []string `json:"o,omitempty"`

	Limit int `json:"-"`
}

type keyRenderData KeySearchOptions

func (opts keyRenderData) Normalize() (*keyRenderData, error) {
	if opts.Limit == 0 {
		opts.Limit = search.DefaultMaxResults
	}

	err := validate.Many(
		validate.Search("Search", opts.Search),
		validate.Range("Limit", opts.Limit, 0, search.MaxResults),
		validate.Range("Omit", len(opts.Omit), 0, 50),
	)

	if opts.After != "" {
		err = validate.Many(err, validate.LabelKey("After", opts.After))
	}

	if err != nil {
		return nil, err
	}
	for i, key := range opts.Omit {
		err = validate.Many(err,
			validate.LabelKey("Omit["+strconv.Itoa(i)+"]", key),
		)
	}

	return &opts, err
}

func (s *Store) SearchKeys(ctx context.Context, opts *KeySearchOptions) ([]string, error) {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return nil, err
	}
	if opts == nil {
		opts = &KeySearchOptions{}
	}
	data, err := (*keyRenderData)(opts).Normalize()
	if err != nil {
		return nil, err
	}
	return s.delta.searchKeys(ctx, KeySearchOptions(*data))
}

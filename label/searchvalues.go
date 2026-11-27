package label

import (
	"context"
	"strconv"

	"github.com/target/goalert/permission"
	"github.com/target/goalert/search"
	"github.com/target/goalert/validation/validate"
)

// ValueSearchOptions allow filtering and paginating the list of rotations.
type ValueSearchOptions struct {
	Key string `json:"k"`

	KeySearchOptions
}

type valueRenderData ValueSearchOptions

func (opts valueRenderData) Normalize() (*valueRenderData, error) {
	if opts.Limit == 0 {
		opts.Limit = search.DefaultMaxResults
	}

	err := validate.Many(
		validate.Search("Search", opts.Search),
		validate.Range("Limit", opts.Limit, 0, search.MaxResults),
		validate.Range("Omit", len(opts.Omit), 0, 50),
		validate.Search("Key", opts.Key),
	)

	if opts.After != "" {
		err = validate.Many(err, validate.LabelValue("After", opts.After))
	}

	if err != nil {
		return nil, err
	}
	for i, value := range opts.Omit {
		err = validate.Many(err,
			validate.LabelKey("Omit["+strconv.Itoa(i)+"]", value),
		)
	}

	return &opts, err
}

func (s *Store) SearchValues(ctx context.Context, opts *ValueSearchOptions) ([]string, error) {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return nil, err
	}
	if opts == nil {
		opts = &ValueSearchOptions{}
	}
	data, err := (*valueRenderData)(opts).Normalize()
	if err != nil {
		return nil, err
	}
	return s.delta.searchValues(ctx, ValueSearchOptions(*data))
}

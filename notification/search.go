package notification

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/validation"
	"github.com/target/goalert/validation/validate"
)

type MessageLog struct {
	ID           string
	CreatedAt    time.Time
	LastStatusAt time.Time
	MessageType  gadb.EnumOutgoingMessagesType

	LastStatus    State
	StatusDetails string
	SrcValue      string

	AlertID       int
	ProviderMsgID *ProviderMessageID

	UserID   string
	UserName string

	ContactMethodID uuid.UUID

	ChannelID uuid.UUID

	ServiceID   string
	ServiceName string

	SentAt     *time.Time
	RetryCount int
}

// SearchOptions allow filtering and paginating the list of messages.
type SearchOptions struct {
	Search string       `json:"s,omitempty"`
	After  SearchCursor `json:"a,omitempty"`

	CreatedAfter  time.Time `json:"ca,omitempty"`
	CreatedBefore time.Time `json:"cb,omitempty"`

	// Omit specifies a list of message IDs to exclude from the results
	Omit []string `json:"o,omitempty"`

	Limit int `json:"-"`
}

// SearchCursor is used to indicate a position in a paginated list.
type SearchCursor struct {
	ID        string    `json:"i,omitempty"`
	CreatedAt time.Time `json:"n,omitempty"`
}

type renderData struct {
	SearchOptions

	TimeSeries         bool
	TimeSeriesOrigin   time.Time
	TimeSeriesInterval time.Duration
}

func (opts renderData) Normalize() (*renderData, error) {
	if opts.Limit == 0 {
		opts.Limit = 50
	}

	err := validate.Many(
		validate.Search("Search", opts.Search),
		// should be 1 more than the expected limit
		validate.Range("Limit", opts.Limit, 0, 101),
		validate.ManyUUID("Omit", opts.Omit, 50),
	)
	if err != nil {
		return nil, err
	}

	if opts.TimeSeries {
		opts.TimeSeriesInterval = opts.TimeSeriesInterval.Truncate(time.Second)
		if opts.CreatedBefore.IsZero() {
			return nil, validation.NewFieldError("CreatedBefore", "required for time series queries")
		}
		if opts.CreatedAfter.IsZero() {
			return nil, validation.NewFieldError("CreatedAfter", "required for time series queries")
		}

		diff := opts.CreatedBefore.Sub(opts.CreatedAfter)
		minInterval := diff/1000 + 1
		minInterval = minInterval.Truncate(time.Minute)
		err = validate.Duration("TimeSeriesInterval", opts.TimeSeriesInterval, minInterval, time.Hour*24*365)
	}
	if err != nil {
		return nil, err
	}

	return &opts, err
}

type TimeSeriesOpts struct {
	SearchOptions
	TimeSeriesOrigin   time.Time
	TimeSeriesInterval time.Duration
}
type TimeSeriesBucket struct {
	Start time.Time
	End   time.Time
	Count int
}

// TimeSeries returns a list of time series buckets for the given search options.
func (s *Store) TimeSeries(ctx context.Context, opts TimeSeriesOpts) ([]TimeSeriesBucket, error) {
	err := permission.LimitCheckAny(ctx, permission.System, permission.Admin)
	if err != nil {
		return nil, err
	}

	data := &renderData{
		SearchOptions:      opts.SearchOptions,
		TimeSeries:         true,
		TimeSeriesOrigin:   opts.TimeSeriesOrigin,
		TimeSeriesInterval: opts.TimeSeriesInterval,
	}
	data, err = data.Normalize()
	if err != nil {
		return nil, err
	}

	return s.delta.timeSeries(ctx, data)
}

func (s *Store) Search(ctx context.Context, opts *SearchOptions) ([]MessageLog, error) {
	if opts == nil {
		opts = &SearchOptions{}
	}

	err := permission.LimitCheckAny(ctx, permission.System, permission.Admin)
	if err != nil {
		return nil, err
	}

	data := &renderData{SearchOptions: *opts}
	data, err = data.Normalize()
	if err != nil {
		return nil, err
	}

	return s.delta.search(ctx, data)
}

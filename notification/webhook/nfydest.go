package webhook

import (
	"context"
	"net/url"

	"github.com/target/goalert/config"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/notification/nfydest"
	"github.com/target/goalert/validation"
	"github.com/target/goalert/validation/validate"
)

const (
	DestTypeWebhook = "builtin-webhook"
	FieldWebhookURL = "webhook_url"
	FallbackIconURL = "builtin://webhook"
)

func NewWebhookDest(url string) gadb.DestV1 {
	return gadb.NewDestV1(DestTypeWebhook, FieldWebhookURL, url)
}

var _ (nfydest.Provider) = (*Sender)(nil)

func (Sender) ID() string { return DestTypeWebhook }

func (Sender) TypeInfo(ctx context.Context) (*nfydest.TypeInfo, error) {
	cfg := config.FromContext(ctx)
	return &nfydest.TypeInfo{
		Type:                       DestTypeWebhook,
		Name:                       "Webhook",
		Enabled:                    cfg.Webhook.Enable,
		SupportsUserVerification:   true,
		SupportsOnCallNotify:       true,
		SupportsStatusUpdates:      true,
		SupportsAlertNotifications: true,
		StatusUpdatesRequired:      true,
		RequiredFields: []nfydest.FieldConfig{{
			FieldID:            FieldWebhookURL,
			Label:              "Webhook URL",
			PlaceholderText:    "https://example.com",
			InputType:          "url",
			Hint:               "Webhook Documentation",
			HintURL:            "/docs#webhooks",
			SupportsValidation: true,
		}},
	}, nil
}

func (s *Sender) ValidateField(ctx context.Context, fieldID, value string) error {
	cfg := config.FromContext(ctx)
	switch fieldID {
	case FieldWebhookURL:
		err := validate.AbsoluteURL(FieldWebhookURL, value)
		if err != nil {
			return err
		}
		if !cfg.ValidWebhookURL(value) {
			return validation.NewGenericError("url is not allowed by administator")
		}

		return nil
	}

	return validation.NewGenericError("unknown field ID")
}

func (s *Sender) DisplayInfo(ctx context.Context, args map[string]string) (*nfydest.DisplayInfo, error) {
	if args == nil {
		args = make(map[string]string)
	}

	u, err := url.Parse(args[FieldWebhookURL])
	if err != nil {
		return nil, validation.WrapError(err)
	}
	return &nfydest.DisplayInfo{
		IconURL:     FallbackIconURL,
		IconAltText: "Webhook",
		Text:        u.Hostname(),
	}, nil
}

package app

import (
	"context"

	"github.com/target/goalert/notification/twilio"
)

func (app *App) initTwilio(ctx context.Context) error {
	app.twilioConfig = &twilio.Config{
		BaseURL: app.cfg.TwilioBaseURL,
		CMStore: app.ContactMethodStore,
		Client:  app.httpClient,
	}
	app.twilioSMS = app.stores.NewTwilioSMS(app.twilioConfig)
	app.twilioVoice = twilio.NewVoice(app.twilioConfig)
	return nil
}

package app

import (
	"context"

	"github.com/target/goalert/graphql2/graphqlapp"
)

func (app *App) initGraphQL(ctx context.Context) error {
	db := app.stores.DB
	app.graphql2 = &graphqlapp.App{
		DB:                  db,
		AuthBasicStore:      app.AuthBasicStore,
		UserStore:           app.UserStore,
		CMStore:             app.ContactMethodStore,
		NRStore:             app.NotificationRuleStore,
		NCStore:             app.NCStore,
		AlertStore:          app.AlertStore,
		AlertLogStore:       app.AlertLogStore,
		ServiceStore:        app.ServiceStore,
		FavoriteStore:       app.FavoriteStore,
		PolicyStore:         app.EscalationStore,
		ScheduleStore:       app.ScheduleStore,
		RotationStore:       app.RotationStore,
		OnCallStore:         app.OnCallStore,
		TimeZoneStore:       app.TimeZoneStore,
		IntKeyStore:         app.IntegrationKeyStore,
		LabelStore:          app.LabelStore,
		RuleStore:           app.ScheduleRuleStore,
		OverrideStore:       app.OverrideStore,
		ConfigStore:         app.ConfigStore,
		LimitStore:          app.LimitStore,
		NotificationStore:   app.NotificationStore,
		NoticeStore:         app.NoticeStore,
		Twilio:              app.twilioConfig,
		AuthHandler:         app.AuthHandler,
		NotificationManager: app.notificationManager,
		APIKeyStore:         app.APIKeyStore,
		DestReg:             app.DestRegistry,
		ReEncryptAll: func(ctx context.Context) error {
			return app.storage.Tables.ReEncryptAll(ctx, db, app.cfg.EncryptionKeys)
		},
	}

	return nil
}

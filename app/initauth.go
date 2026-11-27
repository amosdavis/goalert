package app

import (
	"context"

	"github.com/pkg/errors"
	"github.com/target/goalert/auth"
	"github.com/target/goalert/auth/basic"
	"github.com/target/goalert/auth/oidc"
)

func (app *App) initAuth(ctx context.Context) error {
	cfgAuth := auth.HandlerConfig{
		UserStore:      app.UserStore,
		SessionKeyring: app.SessionKeyring,
		IntKeyStore:    app.IntegrationKeyStore,
		APIKeyring:     app.APIKeyring,
		APIKeyStore:    app.APIKeyStore,
	}
	app.AuthHandler = app.stores.NewAuthHandler(cfgAuth)

	cfg := oidc.Config{
		Keyring:    app.OAuthKeyring,
		NonceStore: app.NonceStore,
	}
	oidcProvider, err := oidc.NewProvider(ctx, cfg)
	if err != nil {
		return errors.Wrap(err, "init OIDC auth provider")
	}
	if err := app.AuthHandler.AddIdentityProvider("oidc", oidcProvider); err != nil {
		return err
	}

	basicProvider, err := basic.NewProvider(ctx, app.AuthBasicStore)
	if err != nil {
		return errors.Wrap(err, "init basic auth provider")
	}
	return app.AuthHandler.AddIdentityProvider("basic", basicProvider)
}

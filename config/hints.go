package config

// Hints contains information helpful for configuring GoAlert and various integrations.
type Hints struct {
	OIDC struct {
		RedirectURL string
	}
	Twilio struct {
		MessageWebhookURL string
		VoiceWebhookURL   string
	}
}

// Hints returns available hints for the current configuration.
func (cfg Config) Hints() Hints {
	var h Hints

	h.OIDC.RedirectURL = cfg.CallbackURL("/api/v2/identity/providers/oidc/callback")
	h.Twilio.MessageWebhookURL = cfg.CallbackURL("/api/v2/twilio/message")
	h.Twilio.VoiceWebhookURL = cfg.CallbackURL("/api/v2/twilio/call")

	return h
}

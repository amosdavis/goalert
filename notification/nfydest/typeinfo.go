package nfydest

type TypeInfo struct {
	Type string

	Name        string
	IconURL     string
	IconAltText string
	Enabled     bool

	RequiredFields []FieldConfig

	UserDisclaimer string

	// Message type info
	SupportsStatusUpdates      bool
	SupportsAlertNotifications bool
	SupportsUserVerification   bool
	SupportsOnCallNotify       bool

	UserVerificationRequired bool
	StatusUpdatesRequired    bool
}

func (t TypeInfo) IsContactMethod() bool {
	return t.SupportsAlertNotifications && t.SupportsUserVerification
}

func (t TypeInfo) IsEPTarget() bool {
	return t.SupportsAlertNotifications && !t.UserVerificationRequired
}

func (t TypeInfo) IsSchedOnCallNotify() bool {
	return t.SupportsOnCallNotify && !t.UserVerificationRequired
}

type FieldConfig struct {
	FieldID            string
	Label              string
	Hint               string
	HintURL            string
	PlaceholderText    string
	Prefix             string
	InputType          string
	SupportsSearch     bool
	SupportsValidation bool
}

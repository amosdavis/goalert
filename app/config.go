package app

import (
	"crypto/tls"
	"log/slog"
	"time"

	"github.com/target/goalert/config"
	"github.com/target/goalert/expflag"
	"github.com/target/goalert/keyring"
	"github.com/target/goalert/util/log"
)

type Config struct {
	LegacyLogger *log.Logger

	Logger *slog.Logger

	ExpFlags expflag.FlagSet

	ListenAddr  string
	Verbose     bool
	JSON        bool
	LogRequests bool
	APIOnly     bool
	LogEngine   bool

	PublicURL string

	TLSListenAddr string
	TLSConfig     *tls.Config

	HTTPPrefix string

	MaxReqBodyBytes   int64
	MaxReqHeaderBytes int

	DisableHTTPSRedirect bool

	EnableSecureHeaders bool

	TwilioBaseURL string

	// StorageObjects and StorageLog are where GoAlert's data is
	// (storagebackend.Config).
	StorageObjects string
	StorageLog     string
	// PodName, if set, names this pod in the commits it writes.
	PodName string

	StatusAddr string

	EngineCycleTime time.Duration

	EncryptionKeys keyring.Keys

	StubNotifiers bool

	UIDir string

	// InitialConfig will be pushed into the config store
	// if specified before the engine is started.
	InitialConfig *config.Config
}

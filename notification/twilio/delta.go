package twilio

import (
	"time"

	"github.com/target/goalert/filestore"
	"github.com/target/goalert/txn"
)

// errorRow is a twilio_sms_errors or twilio_voice_errors row on the Delta
// backend.
type errorRow struct {
	ID           int64     `parquet:"id"`
	PhoneNumber  string    `parquet:"phone_number"`
	ErrorMessage string    `parquet:"error_message"`
	Outgoing     bool      `parquet:"outgoing"`
	OccurredAt   time.Time `parquet:"occurred_at,timestamp(microsecond:utc)"`
}

// DeltaTables are this package's tables on the Delta backend.
type DeltaTables struct{}

// DefineDeltaTables adds the error tables to the Delta backend's schema. SMS
// reply callbacks are gone with reply codes (F60).
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	for _, name := range []string{"twilio_sms_errors", "twilio_voice_errors"} {
		filestore.NewTable(s, name, func(r errorRow) string { return filestore.IntKey(r.ID) })
	}
	return &DeltaTables{}
}

type dbSMS struct{}

// NewDeltaSMS returns an SMS sender on the Delta backend. The lookups are kept
// in the signature for the stores' shared constructor, but reply-code storage
// is gone, so it needs none of them.
func NewDeltaSMS(_ txn.DB, _ *DeltaTables, c *Config, _ AlertOpenFunc, _ ServiceNameFunc) *SMS {
	return newSMS(&dbSMS{}, c)
}

// AlertOpenFunc reports whether, in r, an alert exists and is not closed.
type AlertOpenFunc func(r filestore.Reader, alertID int64) bool

// ServiceNameFunc returns, in r, a service's name, and false if there is no
// such service.
type ServiceNameFunc func(r filestore.Reader, serviceID string) (name string, ok bool)

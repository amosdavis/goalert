package twilio

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/target/goalert/config"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/notification"
	"github.com/target/goalert/notification/nfydest"
	"github.com/target/goalert/retry"
	"github.com/target/goalert/util/log"

	"github.com/pkg/errors"
)

func NewSMSDest(number string) gadb.DestV1 {
	return gadb.NewDestV1(DestTypeTwilioSMS, FieldPhoneNumber, number)
}

// SMS implements a notification.Sender for Twilio SMS.
type SMS struct {
	b *dbSMS
	c *Config
	r notification.Receiver

	limit *replyLimiter
}

var (
	_ notification.ReceiverSetter = &SMS{}
	_ nfydest.MessageSender       = &SMS{}
	_ nfydest.MessageStatuser     = &SMS{}
)

func newSMS(b *dbSMS, c *Config) *SMS {
	return &SMS{
		b: b,
		c: c,

		limit: newReplyLimiter(),
	}
}

// SetReceiver sets the notification.Receiver for incoming messages and status updates.
func (s *SMS) SetReceiver(r notification.Receiver) { s.r = r }

// Status provides the current status of a message.
func (s *SMS) MessageStatus(ctx context.Context, externalID string) (*notification.Status, error) {
	msg, err := s.c.GetSMS(ctx, externalID)
	if err != nil {
		return nil, wrapStatusErr(err)
	}

	return msg.messageStatus(), nil
}

// Send implements the notification.Sender interface.
func (s *SMS) SendMessage(ctx context.Context, msg notification.Message) (*notification.SentMessage, error) {
	cfg := config.FromContext(ctx)
	if !cfg.Twilio.Enable {
		return nil, errors.New("Twilio provider is disabled")
	}
	if msg.DestType() != DestTypeTwilioSMS {
		return nil, errors.Errorf("unsupported destination type %s; expected SMS", msg.DestType())
	}
	destNumber := msg.DestArg(FieldPhoneNumber)
	if destNumber == cfg.Twilio.FromNumber {
		return nil, errors.New("refusing to send outgoing SMS to FromNumber")
	}

	ctx = log.WithFields(ctx, log.Fields{
		"Phone": destNumber,
		"Type":  "TwilioSMS",
	})

	var message string
	var err error
	switch t := msg.(type) {
	case notification.AlertStatus:
		message, err = renderAlertStatusMessage(cfg.ApplicationName(), t)
	case notification.AlertBundle:
		var link string
		if canContainURL(ctx, destNumber) {
			link = cfg.CallbackURL(fmt.Sprintf("/services/%s/alerts", t.ServiceID))
		}

		message, err = renderAlertBundleMessage(cfg.ApplicationName(), t, link, 0)
	case notification.Alert:
		var link string
		if canContainURL(ctx, destNumber) {
			link = cfg.CallbackURL(fmt.Sprintf("/alerts/%d", t.AlertID))
		}

		message, err = renderAlertMessage(cfg.ApplicationName(), t, link, 0)
	case notification.Test:
		message = fmt.Sprintf("%s: Test message.", cfg.ApplicationName())
	case notification.Verification:
		message = fmt.Sprintf("%s: Verification code: %s", cfg.ApplicationName(), t.Code)
	default:
		return nil, errors.Errorf("unhandled message type %T", t)
	}
	if err != nil {
		return nil, errors.Wrap(err, "render message")
	}

	opts := &SMSOptions{
		ValidityPeriod: time.Second * 30,
		CallbackParams: make(url.Values),
	}
	opts.CallbackParams.Set(msgParamID, msg.MsgID())
	// Actually send notification to end user & receive Message Status
	resp, err := s.c.SendSMS(ctx, destNumber, message, opts)
	if err != nil {
		return nil, errors.Wrap(err, "send message")
	}

	// If the message was sent successfully, reset reply limits.
	s.limit.Reset(destNumber)

	return resp.sentMessage(), nil
}

func (s *SMS) ServeStatusCallback(w http.ResponseWriter, req *http.Request) {
	if disabled(w, req) {
		return
	}
	ctx := req.Context()
	status := MessageStatus(req.FormValue("MessageStatus"))
	sid := validSID(req.FormValue("MessageSid"))
	number := validPhone(req.FormValue("To"))
	if status == "" || sid == "" || number == "" {
		http.Error(w, "", http.StatusBadRequest)
		return
	}

	ctx = log.WithFields(ctx, log.Fields{
		"Status": status,
		"SID":    sid,
		"Phone":  number,
		"Type":   "TwilioSMS",
	})
	msg := Message{SID: sid, Status: status, From: validPhone(req.FormValue("From"))}

	log.Debugf(ctx, "Got Twilio SMS status callback.")

	err := s.r.SetMessageStatus(ctx, sid, msg.messageStatus())
	if err != nil {
		// log and continue
		log.Log(ctx, err)
	}
}

// isStopMessage checks the body of the message against single-word matches
// i.e. "stop" will unsubscribe, however "please stop" will not.
func isStopMessage(body string) bool {
	switch strings.ToLower(body) {
	case "stop", "stopall", "unsubscribe", "cancel", "end", "quit":
		return true
	}

	return false
}

// isStartMessage checks the body of the message against single-word matches
// i.e. "start" will resubscribe, however "please start" will not.
func isStartMessage(body string) bool {
	switch strings.ToLower(body) {
	case "start", "yes", "unstop":
		return true
	}

	return false
}

func (s *SMS) ServeMessage(w http.ResponseWriter, req *http.Request) {
	if disabled(w, req) {
		return
	}
	ctx := req.Context()
	cfg := config.FromContext(ctx)
	from := validPhone(req.FormValue("From"))
	if from == "" || from == cfg.Twilio.FromNumber {
		http.Error(w, "", http.StatusBadRequest)
		return
	}

	ctx = log.WithFields(ctx, log.Fields{
		"Number": from,
		"Type":   "TwilioSMS",
	})

	respond := func(isPassive bool, msg string) {
		if !isPassive {
			// always reset if an action was taken
			s.limit.Reset(from)
		}

		if s.limit.ShouldDrop(from) {
			log.Debugf(ctx, "SMS passive reply limit reached for %s, not replying.", from)
			return
		}

		if isPassive {
			valid, err := s.r.IsKnownDest(ctx, gadb.NewDestV1(DestTypeTwilioSMS, FieldPhoneNumber, from))
			if err != nil {
				log.Log(ctx, fmt.Errorf("check if known SMS number: %w", err))
			} else if !valid {
				// don't respond if the number is not known
				return
			}
			s.limit.RecordPassiveReply(from)
		}
		smsFrom := req.FormValue("To")
		if cfg.Twilio.MessagingServiceSID != "" {
			smsFrom = cfg.Twilio.MessagingServiceSID
		}
		_, err := s.c.SendSMS(ctx, from, msg, &SMSOptions{FromNumber: smsFrom})
		if err != nil {
			log.Log(ctx, errors.Wrap(err, "send response"))
		}
	}
	retryOpts := []retry.Option{
		retry.Log(ctx),
		retry.Limit(10),
		retry.FibBackoff(time.Second),
	}

	// handle start and stop codes from user
	body := req.FormValue("Body")
	if isStartMessage(body) {
		err := retry.DoTemporaryError(func(int) error { return s.r.Start(ctx, NewSMSDest(from)) }, retryOpts...)
		if err != nil {
			log.Log(ctx, fmt.Errorf("process START message: %w", err))
		}
		return
	}
	if isStopMessage(body) {
		err := retry.DoTemporaryError(func(int) error { return s.r.Stop(ctx, NewSMSDest(from)) }, retryOpts...)
		if err != nil {
			log.Log(ctx, fmt.Errorf("process STOP message: %w", err))
		}
		return
	}

	respond(true, "Replies are not supported. Visit the dashboard to manage alerts.")
}

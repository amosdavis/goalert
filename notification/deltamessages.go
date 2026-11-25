package notification

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
)

// messageBuckets is how many data files outgoing_messages and
// message_status_history are spread over. Both grow without bound and change
// with every message, so small files keep a change cheap (F39).
const messageBuckets = 64

const (
	statusBundled        = string(gadb.EnumOutgoingMessagesStatusBundled)
	statusFailed         = string(gadb.EnumOutgoingMessagesStatusFailed)
	statusPending        = string(gadb.EnumOutgoingMessagesStatusPending)
	statusQueuedRemotely = string(gadb.EnumOutgoingMessagesStatusQueuedRemotely)
	statusSending        = string(gadb.EnumOutgoingMessagesStatusSending)

	typeAlert        = string(gadb.EnumOutgoingMessagesTypeAlertNotification)
	typeAlertBundle  = string(gadb.EnumOutgoingMessagesTypeAlertNotificationBundle)
	typeAlertStatus  = string(gadb.EnumOutgoingMessagesTypeAlertStatusUpdate)
	typeStatusBundle = string(gadb.EnumOutgoingMessagesTypeAlertStatusUpdateBundle)
	typeTest         = string(gadb.EnumOutgoingMessagesTypeTestNotification)
	typeVerification = string(gadb.EnumOutgoingMessagesTypeVerificationMessage)
)

// The values of enum_outgoing_messages_type and
// enum_outgoing_messages_status.
var (
	messageTypes = []string{
		typeAlert, typeAlertBundle, typeAlertStatus, typeStatusBundle,
		string(gadb.EnumOutgoingMessagesTypeScheduleOnCallNotification),
		string(gadb.EnumOutgoingMessagesTypeSignalMessage),
		typeTest, typeVerification,
	}
	messageStatuses = []string{
		statusBundled,
		string(gadb.EnumOutgoingMessagesStatusDelivered),
		statusFailed, statusPending, statusQueuedRemotely,
		string(gadb.EnumOutgoingMessagesStatusRead),
		statusSending,
		string(gadb.EnumOutgoingMessagesStatusSent),
	}
)

// MessageRow is an outgoing_messages row on the Delta backend. Either UserID
// and ContactMethodID, or ChannelID, are set.
type MessageRow struct {
	ID                     string     `parquet:"id"`
	MessageType            string     `parquet:"message_type"`
	UserID                 *string    `parquet:"user_id,optional"`
	ContactMethodID        *string    `parquet:"contact_method_id,optional"`
	ChannelID              *string    `parquet:"channel_id,optional"`
	AlertID                *int64     `parquet:"alert_id,optional"`
	AlertLogID             *int64     `parquet:"alert_log_id,optional"`
	ServiceID              *string    `parquet:"service_id,optional"`
	EscalationPolicyID     *string    `parquet:"escalation_policy_id,optional"`
	ScheduleID             *string    `parquet:"schedule_id,optional"`
	CycleID                *string    `parquet:"cycle_id,optional"`
	UserVerificationCodeID *string    `parquet:"user_verification_code_id,optional"`
	StatusAlertIDs         []int64    `parquet:"status_alert_ids,optional,list"`
	LastStatus             string     `parquet:"last_status"`
	LastStatusAt           *time.Time `parquet:"last_status_at,optional,timestamp(microsecond:utc)"`
	StatusDetails          string     `parquet:"status_details"`
	ProviderMsgID          *string    `parquet:"provider_msg_id,optional"` // see ProviderMsgIDColumn
	ProviderSeq            int32      `parquet:"provider_seq"`
	SrcValue               *string    `parquet:"src_value,optional"`
	RetryCount             int32      `parquet:"retry_count"`
	NextRetryAt            *time.Time `parquet:"next_retry_at,optional,timestamp(microsecond:utc)"`
	FiredAt                *time.Time `parquet:"fired_at,optional,timestamp(microsecond:utc)"`
	SendingDeadline        *time.Time `parquet:"sending_deadline,optional,timestamp(microsecond:utc)"`
	SentAt                 *time.Time `parquet:"sent_at,optional,timestamp(microsecond:utc)"`
	CreatedAt              time.Time  `parquet:"created_at,timestamp(microsecond:utc)"`
}

// StatusHistoryRow is a message_status_history row on the Delta backend.
type StatusHistoryRow struct {
	ID            int64     `parquet:"id"`
	MessageID     string    `parquet:"message_id"`
	Status        string    `parquet:"status"`
	StatusDetails string    `parquet:"status_details"`
	Timestamp     time.Time `parquet:"timestamp,timestamp(microsecond:utc)"`
}

// ProviderMsgIDColumn is id as the provider_msg_id column holds it: nil for
// an empty one.
func ProviderMsgIDColumn(id ProviderMessageID) *string {
	v, err := id.Value()
	if err != nil {
		return nil // Value never fails
	}
	s, ok := v.(string)
	if !ok {
		return nil
	}
	return &s
}

// destID is the contact method or channel the message goes to.
func (m MessageRow) destID() string {
	switch {
	case m.ContactMethodID != nil:
		return *m.ContactMethodID
	case m.ChannelID != nil:
		return *m.ChannelID
	}
	return ""
}

// unfinished reports whether the engine still has something to do with the
// message: send it, find out what became of it, or try it again.
func (m MessageRow) unfinished() bool {
	switch m.LastStatus {
	case statusPending, statusSending, statusQueuedRemotely:
		return true
	case statusFailed:
		return m.NextRetryAt != nil || m.CycleID != nil
	}
	return false
}

// check returns an error if the row breaks a CHECK constraint of
// outgoing_messages, tried in the order of their names, or has an unknown
// type or status.
func (m MessageRow) check() error {
	if !slices.Contains(messageTypes, m.MessageType) {
		return fmt.Errorf("outgoing_messages: unknown message type %q", m.MessageType)
	}
	if !slices.Contains(messageStatuses, m.LastStatus) {
		return fmt.Errorf("outgoing_messages: unknown status %q", m.LastStatus)
	}
	toUser := m.UserID != nil && m.ContactMethodID != nil && m.ChannelID == nil
	toChannel := m.ChannelID != nil && m.ContactMethodID == nil && m.UserID == nil
	processed := true
	switch m.LastStatus {
	case statusPending, statusSending, statusFailed, statusBundled:
		processed = false
	}
	for _, c := range []struct {
		name string
		ok   bool
	}{
		{"om_alert_svc_ep_ids", m.MessageType != typeAlert || m.AlertID != nil && m.ServiceID != nil && m.EscalationPolicyID != nil},
		{"om_no_status_bundles", m.MessageType != typeStatusBundle || m.LastStatus != statusPending},
		{"om_pending_no_fired_no_sent", m.LastStatus != statusPending || m.FiredAt == nil && m.SentAt == nil},
		{"om_processed_no_fired_sent", !processed || m.FiredAt == nil && m.SentAt != nil},
		{"om_sending_deadline_reqd", m.LastStatus != statusSending || m.SendingDeadline != nil},
		{"om_sending_fired_no_sent", m.LastStatus != statusSending || m.FiredAt != nil && m.SentAt == nil},
		{"om_status_alert_ids", m.MessageType != typeStatusBundle || m.StatusAlertIDs != nil},
		{"om_status_update_log_id", m.MessageType != typeAlertStatus || m.AlertLogID != nil},
		{"om_user_cm_or_channel", toUser || toChannel},
		{"verify_needs_id", m.MessageType != typeVerification || m.UserVerificationCodeID != nil},
	} {
		if !c.ok {
			return &filestore.CheckError{Table: "outgoing_messages", Constraint: c.name}
		}
	}
	return nil
}

// outgoingMessage is the part of the row that outgoingMessageToSendResult
// needs, as the Postgres queries return it.
func (m MessageRow) outgoingMessage() (gadb.OutgoingMessage, error) {
	id, err := uuid.Parse(m.ID)
	if err != nil {
		return gadb.OutgoingMessage{}, fmt.Errorf("outgoing_messages %s: %w", m.ID, err)
	}
	msg := gadb.OutgoingMessage{
		ID:            id,
		MessageType:   gadb.EnumOutgoingMessagesType(m.MessageType),
		LastStatus:    gadb.EnumOutgoingMessagesStatus(m.LastStatus),
		StatusDetails: m.StatusDetails,
		ProviderSeq:   m.ProviderSeq,
		CreatedAt:     m.CreatedAt,
	}
	if m.ProviderMsgID != nil {
		if msg.ProviderMsgID, err = gadb.ParseProviderMessageID(*m.ProviderMsgID); err != nil {
			return gadb.OutgoingMessage{}, err
		}
	}
	if m.NextRetryAt != nil {
		msg.NextRetryAt = sql.NullTime{Time: *m.NextRetryAt, Valid: true}
	}
	if m.LastStatusAt != nil {
		msg.LastStatusAt = sql.NullTime{Time: *m.LastStatusAt, Valid: true}
	}
	if m.SrcValue != nil {
		msg.SrcValue = sql.NullString{String: *m.SrcValue, Valid: true}
	}
	return msg, nil
}

// maxTimeSteps bounds how many keys of a timeIndex a range is looked up by;
// a longer range reads the whole table.
const maxTimeSteps = 1000

// timeIndex finds messages by a range of the time in one of their columns.
// Rows are found by equal keys only, so a key is the step of time, such as an
// hour, that a row's time falls in.
type timeIndex struct {
	rows *filestore.Table[MessageRow]
	ix   *filestore.Index[MessageRow]
	step time.Duration
	at   func(MessageRow) (time.Time, bool)
}

func newTimeIndex(rows *filestore.Table[MessageRow], name string, step time.Duration, at func(MessageRow) (time.Time, bool)) timeIndex {
	x := timeIndex{rows: rows, step: step, at: at}
	x.ix = rows.Index(name, func(m MessageRow) []string {
		v, ok := at(m)
		if !ok {
			return nil
		}
		return []string{timeKey(v, step)}
	})
	return x
}

func timeKey(t time.Time, step time.Duration) string {
	return strconv.FormatInt(t.Truncate(step).Unix(), 10)
}

// ascend calls fn, until it returns false, for each message whose time is at
// or after from and before to.
func (x timeIndex) ascend(r filestore.Reader, from, to time.Time, fn func(MessageRow) bool) {
	more := true
	visit := func(m MessageRow) bool {
		if v, ok := x.at(m); ok && !v.Before(from) && v.Before(to) {
			more = fn(m)
		}
		return more
	}
	if to.Sub(from) > maxTimeSteps*x.step {
		x.rows.Ascend(r, visit)
		return
	}
	for t := from.Truncate(x.step); more && t.Before(to); t = t.Add(x.step) {
		x.ix.Ascend(r, timeKey(t, x.step), visit)
	}
}

// PendingTypeKey is the key in MessagesPending of the messages of a type.
func PendingTypeKey(typ gadb.EnumOutgoingMessagesType) string { return string(typ) }

// PendingDestKey is the key in MessagesPending of the messages of a type to a
// contact method or channel.
func PendingDestKey(typ gadb.EnumOutgoingMessagesType, destID string) string {
	return string(typ) + "/" + destID
}

func methodTypeKey(cmID, typ string) string { return cmID + "/" + typ }

// defineMessages defines outgoing_messages, keyed by id
// (outgoing_messages_pkey), and message_status_history, keyed by id
// (message_status_history_pkey).
//
// idx_om_alert_log_id, idx_om_cm_sent, idx_om_ep_sent, idx_om_service_sent,
// idx_om_user_sent and idx_om_vcode_id exist on Postgres for the foreign keys
// on their columns, and here each foreign key brings an index of its own.
func (t *DeltaTables) defineMessages(s *filestore.Schema) {
	t.Messages = filestore.NewTable(s, "outgoing_messages", func(m MessageRow) string { return m.ID }).Buckets(messageBuckets)
	t.MessagesUnfinished = t.Messages.Index("idx_om_last_status_sent", func(m MessageRow) []string {
		if !m.unfinished() {
			return nil
		}
		return []string{m.LastStatus}
	})
	t.MessagesPending = t.Messages.Index("om_pending", func(m MessageRow) []string {
		if m.LastStatus != statusPending {
			return nil
		}
		typ := gadb.EnumOutgoingMessagesType(m.MessageType)
		return []string{PendingTypeKey(typ), PendingDestKey(typ, m.destID())}
	})
	t.MessagesByAlert = t.Messages.Index("idx_om_alert_sent", func(m MessageRow) []string {
		if m.AlertID == nil {
			return nil
		}
		return []string{filestore.IntKey(*m.AlertID)}
	})
	t.MessagesByCycle = t.Messages.Index("idx_outgoing_messages_notif_cycle", func(m MessageRow) []string {
		if m.CycleID == nil {
			return nil
		}
		return []string{*m.CycleID}
	})
	t.MessagesByProviderMsgID = t.Messages.Unique("idx_outgoing_messages_provider_msg_id", func(m MessageRow) (string, bool) {
		return filestore.Ref(m.ProviderMsgID)
	})
	t.byMethodType = t.Messages.Index("om_cm_time_test_verify_idx", func(m MessageRow) []string {
		if m.ContactMethodID == nil {
			return nil
		}
		return []string{methodTypeKey(*m.ContactMethodID, m.MessageType)}
	})
	t.byCreated = newTimeIndex(t.Messages, "om_created_day", 24*time.Hour, func(m MessageRow) (time.Time, bool) {
		return m.CreatedAt, true
	})
	t.bySent = newTimeIndex(t.Messages, "om_sent_hour", time.Hour, func(m MessageRow) (time.Time, bool) {
		if m.SentAt == nil {
			return time.Time{}, false
		}
		return *m.SentAt, true
	})

	t.History = filestore.NewTable(s, "message_status_history", func(h StatusHistoryRow) string { return filestore.IntKey(h.ID) }).Buckets(messageBuckets)
	t.historyByMessage = t.History.Index("message_status_history_message_id_idx", func(h StatusHistoryRow) []string { return []string{h.MessageID} })
	filestore.References(t.History, "message_status_history_message_id_fkey", t.Messages,
		func(h StatusHistoryRow) (string, bool) { return h.MessageID, true }, filestore.Cascade, nil)
}

// MessagesSent calls fn, until it returns false, for each message in r whose
// sent_at is at or after from and before to.
func (t *DeltaTables) MessagesSent(r filestore.Reader, from, to time.Time, fn func(MessageRow) bool) {
	t.bySent.ascend(r, from, to, fn)
}

// PendingForAlert returns, in r, the pending messages created before a time
// for an alert, and the pending alert bundles for its service.
func (t *DeltaTables) PendingForAlert(r filestore.Reader, alertID int64, serviceID string, before time.Time) []MessageRow {
	var rows []MessageRow
	add := func(m MessageRow) {
		if m.LastStatus == statusPending && m.CreatedAt.Before(before) {
			rows = append(rows, m)
		}
	}
	forAlert := func(m MessageRow) bool { return m.AlertID != nil && *m.AlertID == alertID }
	t.MessagesByAlert.Ascend(r, filestore.IntKey(alertID), func(m MessageRow) bool {
		add(m)
		return true
	})
	t.MessagesPending.Ascend(r, PendingTypeKey(MessageTypeAlertBundle), func(m MessageRow) bool {
		if m.ServiceID != nil && *m.ServiceID == serviceID && !forAlert(m) {
			add(m)
		}
		return true
	})
	return rows
}

// InsertMessage adds a message, with its first message_status_history row
// (trg_insert_message_status_history). The columns left empty get their
// defaults: a new id, created_at and last_status_at now, and last_status
// pending. It returns the message as added.
func (t *DeltaTables) InsertMessage(tx *filestore.Tx, m MessageRow) (MessageRow, error) {
	now := tx.Now()
	if m.ID == "" {
		m.ID = uuid.NewString()
	}
	if m.LastStatus == "" {
		m.LastStatus = statusPending
	}
	if m.LastStatusAt == nil {
		m.LastStatusAt = &now
	}
	if m.CreatedAt.IsZero() {
		m.CreatedAt = now
	}
	if err := m.check(); err != nil {
		return m, err
	}
	if err := t.Messages.Insert(tx, m, "outgoing_messages_pkey"); err != nil {
		return m, err
	}
	return m, t.recordStatus(tx, m)
}

// SetMessageStatus saves a changed message and records its status in
// message_status_history. It stands for an UPDATE that sets last_status, even
// to what it was (trg_update_message_status_history).
//
// A message is claimed this way: the transaction that finds it pending sets
// it to sending, and only the pod whose transaction committed sends it (F3).
func (t *DeltaTables) SetMessageStatus(tx *filestore.Tx, m MessageRow) error {
	if err := t.putMessage(tx, m); err != nil {
		return err
	}
	return t.recordStatus(tx, m)
}

// UpdateMessage saves a changed message. It stands for an UPDATE that doesn't
// set last_status, which is not recorded, so it refuses to change the status.
func (t *DeltaTables) UpdateMessage(tx *filestore.Tx, m MessageRow) error {
	if old, ok := t.Messages.Get(tx, m.ID); ok && old.LastStatus != m.LastStatus {
		return fmt.Errorf("outgoing_messages %s: UpdateMessage can't change the status from %s to %s", m.ID, old.LastStatus, m.LastStatus)
	}
	return t.putMessage(tx, m)
}

// putMessage saves a message that exists, so that a message deleted since the
// caller read it doesn't come back.
func (t *DeltaTables) putMessage(tx *filestore.Tx, m MessageRow) error {
	if _, ok := t.Messages.Get(tx, m.ID); !ok {
		return fmt.Errorf("outgoing_messages %s: update: %w", m.ID, sql.ErrNoRows)
	}
	if err := m.check(); err != nil {
		return err
	}
	return t.Messages.Put(tx, m)
}

// recordStatus is fn_insert_message_status_history.
func (t *DeltaTables) recordStatus(tx *filestore.Tx, m MessageRow) error {
	if m.LastStatusAt == nil {
		return errors.New("message_status_history: timestamp must not be null: the message has no last_status_at")
	}
	id, err := tx.NextID("message_status_history_id_seq")
	if err != nil {
		return err
	}
	return t.History.Insert(tx, StatusHistoryRow{
		ID:            id,
		MessageID:     m.ID,
		Status:        m.LastStatus,
		StatusDetails: m.StatusDetails,
		Timestamp:     *m.LastStatusAt,
	}, "message_status_history_pkey")
}

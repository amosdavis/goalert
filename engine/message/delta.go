package message

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/target/goalert/alert/alertlog"
	"github.com/target/goalert/config"
	"github.com/target/goalert/deltastores"
	"github.com/target/goalert/engine/enginetables"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/notification"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/retry"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util/log"
)

const (
	// sendTimeout is how long sending a message may take, with its retries.
	sendTimeout = 5 * time.Second
	// claimTime is how long a claim lasts. It is longer than committing the claim
	// and sending the message take together, so that no second send starts while
	// one is in flight (F3). A claim that was committed too late to be used is not.
	claimTime = 30 * time.Second
	// retryDelay is the time between a send that may have failed and the next.
	retryDelay = 15 * time.Second
	maxRetries = 3
	// remoteTimeout is how long a provider has to report what became of a message.
	remoteTimeout = 24 * time.Hour
	// maxRemote is how many messages a provider is asked about in a cycle.
	maxRemote = 10
)

// finishDelay is how long to wait until what came of a message is recorded
// again, at first.
var finishDelay = 100 * time.Millisecond

// DeltaDB sends messages on the Delta backend. A message is claimed by a commit
// before it is sent (F3), and what came of it is recorded by another.
type DeltaDB struct {
	db  *filestore.DeltaDB
	t   *deltastores.Tables
	log *alertlog.Store

	// unrecorded has what came of messages that could not be recorded yet, by
	// their ids, so that they are not sent again.
	mu         sync.Mutex
	unrecorded map[string]outcome
}

// NewDeltaDB returns a message sender on the Delta backend. log is the Delta
// backend's too.
func NewDeltaDB(db *filestore.DeltaDB, t *deltastores.Tables, log *alertlog.Store) *DeltaDB {
	return &DeltaDB{db: db, t: t, log: log, unrecorded: make(map[string]outcome)}
}

// Name returns the name of the module.
func (db *DeltaDB) Name() string { return "Engine.Message" }

// FollowUp does what comes with sending a message, in the transaction that
// records what came of it.
type FollowUp func(ctx context.Context, tx *filestore.Tx) error

// DeltaSendFunc sends a message. It returns what came of it, and what follows
// from that, which may be nil.
type DeltaSendFunc func(context.Context, *Message) (*notification.SendResult, FollowUp, error)

// claim is a message that this pod may send. The time tells it from an earlier
// or later claim of the same message (F54).
type claim struct {
	Message
	at time.Time
}

// outcome is what came of a claim.
type outcome struct {
	claim
	res    *notification.SendResult
	follow FollowUp
	// unclear is whether it may have been recorded already.
	unclear bool
}

// remote is a message whose provider has not reported what became of it.
type remote struct {
	id       string
	provider notification.ProviderMessageID
}

// Batch is what a cycle has to do once it is committed.
type Batch struct {
	// Retried is how many messages were queued again, since it is not known that
	// they were sent.
	Retried int

	claims   []claim
	remotes  []remote
	recorded []outcome
}

// UpdateAll looks for what to send, and claims it. b counts only if the
// transaction commits.
func (db *DeltaDB) UpdateAll(ctx context.Context, tx *filestore.Tx, b *Batch) error {
	if err := permission.LimitCheckAny(ctx, permission.System); err != nil {
		return err
	}
	*b = Batch{}
	u := &update{DeltaDB: db, tx: tx, now: tx.Now(), b: b, dests: make(map[notification.DestID]dest)}
	db.mu.Lock()
	for _, o := range db.unrecorded {
		b.recorded = append(b.recorded, o)
	}
	db.mu.Unlock()
	var todo []todo
	for _, o := range b.recorded {
		todo = append(todo, u.do(o.ID, func() error { return db.record(ctx, tx, o) }))
	}
	u.run(ctx, "record what came of messages", todo)

	u.clearStatusUpdates(ctx)
	u.failUnsendable(ctx)
	u.retry(ctx)
	u.claim(ctx)
	u.checkRemote(ctx)
	return nil
}

// update is one run of UpdateAll.
type update struct {
	*DeltaDB
	tx    *filestore.Tx
	now   time.Time
	b     *Batch
	dests map[notification.DestID]dest
}

// todo is something to be done with one message.
type todo struct {
	id string
	fn func() error
}

func (u *update) do(id string, fn func() error) todo { return todo{id, fn} }

// run does things with messages, one by one: a message for which that fails
// stays as it is, and does not keep the others from being handled (F53).
func (u *update) run(ctx context.Context, what string, todos []todo) {
	err := enginetables.Each(u.tx, todos, func(t todo) error { return errors.Wrapf(t.fn(), "message %s", t.id) })
	if err != nil {
		log.Log(ctx, errors.Wrap(err, what))
	}
}

// unfinished are the messages with a status that still have something to be
// done with.
func (u *update) unfinished(status Status) []notification.MessageRow {
	var out []notification.MessageRow
	u.t.Notification.MessagesUnfinished.Ascend(u.tx, string(status), func(m notification.MessageRow) bool {
		out = append(out, m)
		return true
	})
	return out
}

// fail marks a message as failed, for good.
func (u *update) fail(m notification.MessageRow, details string) error {
	m.LastStatus, m.LastStatusAt, m.StatusDetails = string(StatusFailed), &u.now, details
	m.CycleID, m.NextRetryAt = nil, nil
	return u.t.Notification.SetMessageStatus(u.tx, m)
}

// clearStatusUpdates drops the status updates of contact methods that don't want
// them any more.
func (u *update) clearStatusUpdates(ctx context.Context) {
	var todo []todo
	for _, m := range append(u.unfinished(StatusPending), u.unfinished(StatusFailed)...) {
		if m.MessageType != string(gadb.EnumOutgoingMessagesTypeAlertStatusUpdate) || m.ContactMethodID == nil ||
			Status(m.LastStatus) == StatusFailed && m.NextRetryAt == nil {
			continue
		}
		if cm, ok := u.t.ContactMethod.Methods.Get(u.tx, *m.ContactMethodID); ok && !cm.EnableStatusUpdates {
			todo = append(todo, u.do(m.ID, func() error {
				_, err := u.t.Notification.Messages.Delete(u.tx, m.ID)
				return err
			}))
		}
	}
	u.run(ctx, "clear disabled status updates", todo)
}

// failUnsendable fails the messages whose contact method can't be used.
func (u *update) failUnsendable(ctx context.Context) {
	twilio := config.FromContext(ctx).Twilio.Enable
	var todo []todo
	for _, m := range u.unfinished(StatusPending) {
		if m.ContactMethodID == nil {
			continue
		}
		cm, ok := u.t.ContactMethod.Methods.Get(u.tx, *m.ContactMethodID)
		phone := cm.Type == string(gadb.EnumUserContactMethodTypeSMS) || cm.Type == string(gadb.EnumUserContactMethodTypeVOICE)
		var details string
		switch {
		case !ok:
			continue
		case phone && !twilio:
			details = "SMS/Voice support not enabled by administrator"
		case cm.Disabled && m.MessageType != string(gadb.EnumOutgoingMessagesTypeVerificationMessage):
			details = "contact method disabled"
		default:
			continue
		}
		todo = append(todo, u.do(m.ID, func() error {
			if err := u.fail(m, details); err != nil || m.AlertID == nil {
				return err
			}
			ctx := permission.UserSourceContext(ctx, cm.UserID, permission.RoleUser, &permission.SourceInfo{
				Type: permission.SourceTypeContactMethod,
				ID:   cm.ID,
			})
			return u.log.LogTx(ctx, deltatxn.Of(u.tx), int(*m.AlertID), alertlog.TypeNotificationSent, alertlog.NotificationMetaData{MessageID: m.ID})
		}))
	}
	u.run(ctx, "fail messages that can't be sent", todo)
}

// retry queues the messages again whose sending failed, or may have.
func (u *update) retry(ctx context.Context) {
	// Whether a message whose claim has expired was sent is not known. It is sent
	// again: a duplicate page is better than a lost one (F3).
	var expired, failed []todo
	for _, m := range u.unfinished(StatusSending) {
		if m.SendingDeadline == nil || !m.SendingDeadline.After(u.now) {
			expired = append(expired, u.do(m.ID, func() error {
				return tempFail(u.tx, u.t.Notification, m, nil, "send deadline expired")
			}))
		}
	}
	u.run(ctx, "fail messages whose claim has expired", expired)

	for _, m := range u.unfinished(StatusFailed) {
		switch {
		case m.RetryCount >= maxRetries:
			if m.CycleID != nil || m.NextRetryAt != nil {
				m.NextRetryAt, m.CycleID = nil, nil
				failed = append(failed, u.do(m.ID, func() error { return u.t.Notification.UpdateMessage(u.tx, m) }))
			}
		case m.NextRetryAt != nil && u.now.After(*m.NextRetryAt):
			m.LastStatus, m.StatusDetails = string(StatusPending), ""
			m.NextRetryAt, m.FiredAt, m.SentAt, m.SendingDeadline = nil, nil, nil, nil
			m.ProviderMsgID, m.ProviderSeq = nil, 0
			m.RetryCount++
			failed = append(failed, u.do(m.ID, func() error {
				if err := u.t.Notification.SetMessageStatus(u.tx, m); err != nil {
					return err
				}
				u.b.Retried++
				return nil
			}))
		}
	}
	u.run(ctx, "retry messages that failed", failed)
}

// dest is a destination, and whose it is.
type dest struct {
	gadb.DestV1
	userID   string
	disabled bool
}

// message is a row as the queue takes it. One without a destination, which was
// deleted, is not ok.
func (u *update) message(ctx context.Context, row notification.MessageRow) (m Message, ok bool) {
	id := func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	}
	m = Message{
		ID: row.ID, Type: gadb.EnumOutgoingMessagesType(row.MessageType),
		VerifyID: id(row.UserVerificationCodeID), ServiceID: id(row.ServiceID), ScheduleID: id(row.ScheduleID),
		CreatedAt: row.CreatedAt, StatusAlertIDs: row.StatusAlertIDs,
	}
	if row.AlertID != nil {
		m.AlertID = int(*row.AlertID)
	}
	if row.AlertLogID != nil {
		m.AlertLogID = int(*row.AlertLogID)
	}
	if row.SentAt != nil {
		m.SentAt = *row.SentAt
	}
	var err error
	if row.ContactMethodID != nil {
		m.DestID.CMID.UUID, err = uuid.Parse(*row.ContactMethodID)
		m.DestID.CMID.Valid = true
	} else if row.ChannelID != nil {
		m.DestID.NCID.UUID, err = uuid.Parse(*row.ChannelID)
		m.DestID.NCID.Valid = true
	}
	if err != nil {
		log.Log(ctx, errors.Wrapf(err, "message %s", row.ID))
		return m, false
	}
	d, known := u.dests[m.DestID]
	if !known {
		d, err = u.dest(row)
		if err != nil {
			log.Log(ctx, errors.Wrapf(err, "message %s: destination", row.ID))
		}
		u.dests[m.DestID] = d
	}
	m.Dest, m.UserID = d.DestV1, d.userID
	if row.SentAt == nil && d.disabled && row.MessageType != string(gadb.EnumOutgoingMessagesTypeVerificationMessage) {
		return m, false
	}
	return m, m.Dest.Type != ""
}

func (u *update) dest(row notification.MessageRow) (d dest, err error) {
	if row.ContactMethodID != nil {
		cm, ok := u.t.ContactMethod.Methods.Get(u.tx, *row.ContactMethodID)
		if !ok {
			return d, nil
		}
		d.userID, d.disabled = cm.UserID, cm.Disabled
		d.DestV1, err = cm.DestV1()
		return d, err
	}
	if row.ChannelID == nil {
		return d, nil
	}
	if _, ok := u.t.NotificationChannel.Channels.Get(u.tx, *row.ChannelID); !ok {
		return d, nil
	}
	d.DestV1, err = u.t.NotificationChannel.Dest(u.tx, *row.ChannelID)
	return d, err
}

// claim claims what is to be sent now.
func (u *update) claim(ctx context.Context) {
	q := newQueue(u.queued(ctx), u.now)
	types := q.Types()
	slices.Sort(types)
	var todo []todo
	for _, typ := range types {
		for m := q.NextByType(typ); m != nil; m = q.NextByType(typ) {
			todo = append(todo, u.do(m.ID, func() error {
				row, ok := u.t.Notification.Messages.Get(u.tx, m.ID)
				if !ok || Status(row.LastStatus) != StatusPending {
					return fmt.Errorf("it is not pending: %q", row.LastStatus)
				}
				deadline := u.now.Add(claimTime)
				row.LastStatus, row.LastStatusAt, row.StatusDetails = string(StatusSending), &u.now, ""
				row.SendingDeadline, row.FiredAt, row.NextRetryAt = &deadline, &u.now, nil
				row.ProviderSeq, row.ProviderMsgID = 0, nil
				if err := u.t.Notification.SetMessageStatus(u.tx, row); err != nil {
					return err
				}
				u.b.claims = append(u.b.claims, claim{*m, u.now})
				return nil
			}))
		}
	}
	u.run(ctx, "claim messages", todo)
}

// queued is what waits to be sent, without duplicates and bundled, and what was
// sent lately, which counts for the limits.
func (u *update) queued(ctx context.Context) []Message {
	var msgs []Message
	add := func(row notification.MessageRow) bool {
		if m, ok := u.message(ctx, row); ok {
			msgs = append(msgs, m)
		}
		return true
	}
	for _, row := range u.unfinished(StatusPending) {
		add(row)
	}
	if len(msgs) == 0 {
		return nil // it does not matter what was sent
	}
	cutoff := u.now.Add(-maxThrottleDuration(PerCMThrottle, GlobalCMThrottle))
	u.t.Notification.MessagesSent(u.tx, cutoff, u.now.Add(time.Microsecond), add)

	result := msgs
	err := u.tx.Try(func() (err error) {
		result, err = u.dedup(ctx, msgs)
		return err
	})
	if err != nil {
		// Too many messages are better than none.
		log.Log(ctx, errors.Wrap(err, "remove duplicate messages"))
		return msgs
	}
	return result
}

// dedup removes the messages that others make needless, and bundles those about
// alerts of the same service.
func (u *update) dedup(ctx context.Context, msgs []Message) ([]Message, error) {
	messages := u.t.Notification.Messages
	remove := func(ids []string) error {
		for _, id := range ids {
			if _, err := messages.Delete(u.tx, id); err != nil {
				return err
			}
		}
		return nil
	}
	bundle := func(parentID string, ids []string) error {
		for _, id := range ids {
			row, _ := messages.Get(u.tx, id)
			row.LastStatus, row.LastStatusAt, row.StatusDetails = string(gadb.EnumOutgoingMessagesStatusBundled), &u.now, parentID
			row.CycleID = nil
			if err := u.t.Notification.SetMessageStatus(u.tx, row); err != nil {
				return fmt.Errorf("bundle '%v' by pointing to '%s': %w", ids, parentID, err)
			}
		}
		return nil
	}

	msgs, ids := dedupOnCallNotifications(msgs)
	if err := remove(ids); err != nil {
		return nil, fmt.Errorf("delete duplicate on-call notifications: %w", err)
	}
	msgs, ids = dedupStatusMessages(msgs)
	if err := remove(ids); err != nil {
		return nil, fmt.Errorf("delete duplicate status updates: %w", err)
	}
	msgs, err := dedupAlerts(msgs, bundle)
	if err != nil {
		return nil, fmt.Errorf("dedup alerts: %w", err)
	}
	if config.FromContext(ctx).General.DisableMessageBundles {
		return msgs, nil
	}
	msgs, err = bundleAlertMessages(msgs, func(m Message) (string, error) {
		row := notification.MessageRow{
			MessageType: string(gadb.EnumOutgoingMessagesTypeAlertNotificationBundle),
			CreatedAt:   m.CreatedAt, ServiceID: &m.ServiceID,
		}
		if m.DestID.CMID.Valid {
			id := m.DestID.CMID.UUID.String()
			row.ContactMethodID, row.UserID = &id, &m.UserID
		} else {
			id := m.DestID.NCID.UUID.String()
			row.ChannelID = &id
		}
		row, err := u.t.Notification.InsertMessage(u.tx, row)
		return row.ID, err
	}, bundle)
	if err != nil {
		return nil, fmt.Errorf("bundle alerts: %w", err)
	}
	return msgs, nil
}

// checkRemote finds the messages to ask their providers about, and gives up on
// those that had long enough.
func (u *update) checkRemote(ctx context.Context) {
	var check []notification.MessageRow
	var late, todo []todo
	for _, m := range u.unfinished(StatusQueuedRemotely) {
		switch {
		case m.SentAt == nil:
		case u.now.Sub(*m.SentAt) >= remoteTimeout:
			late = append(late, u.do(m.ID, func() error {
				return u.fail(m, "timed out waiting for final status from provider (last known: "+m.StatusDetails+")")
			}))
		case m.ProviderMsgID != nil && m.LastStatusAt != nil && u.now.Sub(*m.LastStatusAt) > time.Minute:
			check = append(check, m)
		}
	}
	u.run(ctx, "time out messages", late)

	slices.SortFunc(check, func(a, b notification.MessageRow) int {
		return cmp.Or(a.LastStatusAt.Compare(*b.LastStatusAt), cmp.Compare(a.ID, b.ID))
	})
	if len(check) > maxRemote {
		check = check[:maxRemote]
	}
	for _, m := range check {
		todo = append(todo, u.do(m.ID, func() error {
			m.LastStatusAt = &u.now // or it would always be among the first
			if err := u.t.Notification.UpdateMessage(u.tx, m); err != nil {
				return err
			}
			id, err := gadb.ParseProviderMessageID(*m.ProviderMsgID)
			if err != nil {
				log.Log(ctx, errors.Wrapf(err, "message %s", m.ID))
				return nil
			}
			u.b.remotes = append(u.b.remotes, remote{m.ID, id})
			return nil
		}))
	}
	u.run(ctx, "check messages", todo)
}

// tempFail marks a message as failed, to be sent again if it has retries left.
func tempFail(tx *filestore.Tx, t *notification.DeltaTables, m notification.MessageRow, providerID *string, details string) error {
	now := tx.Now()
	m.LastStatus, m.LastStatusAt, m.StatusDetails = string(StatusFailed), &now, details
	if providerID != nil {
		m.ProviderMsgID = providerID
	}
	m.NextRetryAt = nil
	if m.RetryCount < maxRetries {
		next := now.Add(retryDelay)
		m.NextRetryAt = &next
	}
	return t.SetMessageStatus(tx, m)
}

// apply records a status of a message, as a provider or a send reports it.
func apply(tx *filestore.Tx, t *notification.DeltaTables, m notification.MessageRow, res *notification.SendResult) error {
	now := tx.Now()
	providerID := notification.ProviderMsgIDColumn(res.ProviderMessageID)
	var status Status
	switch res.State {
	case notification.StateFailedTemp:
		return tempFail(tx, t, m, providerID, res.Details)
	case notification.StateFailedPerm:
		m.LastStatus, m.LastStatusAt, m.StatusDetails = string(StatusFailed), &now, res.Details
		m.CycleID, m.NextRetryAt = nil, nil
		if providerID != nil {
			m.ProviderMsgID = providerID
		}
		return t.SetMessageStatus(tx, m)
	case notification.StateSending:
		status = StatusQueuedRemotely
	case notification.StateSent:
		status = StatusSent
	case notification.StateDelivered:
		status = StatusDelivered
	case notification.StateRead:
		status = StatusRead
	default:
		return fmt.Errorf("unknown state of a message: %d", res.State)
	}
	if s := Status(m.LastStatus); s == StatusFailed || s == StatusPending {
		return nil
	}
	if res.Sequence != -1 && m.ProviderSeq > int32(res.Sequence) {
		return nil // something newer is known
	}
	m.LastStatus, m.LastStatusAt, m.StatusDetails = string(status), &now, res.Details
	m.CycleID, m.SendingDeadline, m.NextRetryAt = nil, nil, nil
	switch {
	case m.SentAt != nil:
	case m.FiredAt != nil:
		m.SentAt = m.FiredAt
	default:
		m.SentAt = &now
	}
	m.FiredAt = nil
	if providerID != nil {
		m.ProviderMsgID = providerID
	}
	if res.Sequence != -1 {
		m.ProviderSeq = int32(res.Sequence)
	}
	if m.SrcValue == nil && res.SrcValue != "" {
		m.SrcValue = &res.SrcValue
	}
	return t.SetMessageStatus(tx, m)
}

// record records what came of a claim, unless the message has been claimed
// again since (F54), or a provider has reported something newer. What follows
// from sending it is done whatever the message's status is.
func (db *DeltaDB) record(ctx context.Context, tx *filestore.Tx, o outcome) error {
	m, ok := db.t.Notification.Messages.Get(tx, o.ID)
	if !ok {
		return nil
	}
	mine := Status(m.LastStatus) == StatusSending && m.FiredAt != nil && m.FiredAt.Equal(o.at)
	if mine {
		if err := apply(tx, db.t.Notification, m, o.res); err != nil {
			return err
		}
	}
	if o.follow == nil || o.unclear && !mine {
		return nil // it may have been done
	}
	if err := tx.Try(func() error { return o.follow(ctx, tx) }); err != nil {
		log.Log(ctx, errors.Wrapf(err, "message %s was sent, but", o.ID))
	}
	return nil
}

// Done is called once the transaction of UpdateAll has committed.
func (db *DeltaDB) Done(b *Batch) {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, o := range b.recorded {
		if db.unrecorded[o.ID].at.Equal(o.at) {
			delete(db.unrecorded, o.ID)
		}
	}
}

// Unclear is called if the transaction of UpdateAll may have committed.
func (db *DeltaDB) Unclear(b *Batch) {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, o := range b.recorded {
		if u, ok := db.unrecorded[o.ID]; ok && u.at.Equal(o.at) {
			u.unclear = true
			db.unrecorded[o.ID] = u
		}
	}
}

// Send sends the messages that a committed transaction claimed, and asks
// providers what became of earlier ones. It returns once all of that is done.
func (db *DeltaDB) Send(ctx context.Context, b *Batch, send DeltaSendFunc, status StatusFunc) {
	var wg sync.WaitGroup
	for _, c := range b.claims {
		wg.Go(func() { db.send(ctx, c, send) })
	}
	for _, r := range b.remotes {
		wg.Go(func() { db.refresh(ctx, r, status) })
	}
	wg.Wait()
}

func (db *DeltaDB) send(ctx context.Context, c claim, send DeltaSendFunc) {
	ctx = log.WithFields(ctx, log.Fields{"DestID": c.DestID, "DestType": c.Dest.Type, "CallbackID": c.ID})
	if c.AlertID != 0 {
		ctx = log.WithField(ctx, "AlertID", c.AlertID)
	}
	if left := c.at.Add(claimTime).Sub(db.db.Now()); left < sendTimeout {
		// Committing the claim took so long that another pod could send it at the
		// same time. Nothing was sent, so it is safe to leave it to expire (F3).
		log.Log(ctx, fmt.Errorf("a message was claimed too late to be sent now: %s left", left))
		return
	}

	o := outcome{claim: c}
	defer func() {
		if v := recover(); v != nil {
			log.Log(ctx, fmt.Errorf("PANIC while sending a message: %v", v))
			o.res = &notification.SendResult{ID: c.ID, Status: notification.Status{State: notification.StateFailedTemp, Details: "internal error"}}
		}
		db.finish(ctx, o)
	}()
	sCtx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()
	err := retry.DoTemporaryError(func(int) (err error) {
		o.res, o.follow, err = send(sCtx, &c.Message)
		return err
	},
		retry.Log(ctx),
		retry.Limit(10),
		retry.FibBackoff(65*time.Millisecond),
	)
	if err == nil && o.res == nil {
		err = errors.New("no result")
	}
	if err != nil {
		log.Log(ctx, errors.Wrap(err, "send message"))
		var id notification.ProviderMessageID
		if o.res != nil {
			id = o.res.ProviderMessageID
		}
		o.res = &notification.SendResult{
			ID: c.ID, ProviderMessageID: id,
			Status: notification.Status{State: notification.StateFailedTemp, Details: err.Error()},
		}
		o.follow = nil
	}
}

// finish records what came of a claim. It needs no lease: whoever has it, the
// message was sent, or was not. What can't be recorded now is in the next cycle,
// as long as this pod runs.
func (db *DeltaDB) finish(ctx context.Context, o outcome) {
	db.mu.Lock()
	db.unrecorded[o.ID] = o
	db.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), claimTime)
	defer cancel()
	delay := finishDelay
	for attempt := range 4 {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			delay *= 2
		}
		_, err := db.db.Update(ctx, func(tx *filestore.Tx) error { return db.record(ctx, tx, o) })
		if err == nil {
			db.Done(&Batch{recorded: []outcome{o}})
			return
		}
		log.Log(ctx, errors.Wrap(err, "record what came of a message"))
		if errors.Is(err, objstore.ErrUnknown) {
			o.unclear = true
			db.Unclear(&Batch{recorded: []outcome{o}})
		}
	}
}

// refresh asks a message's provider what became of it.
func (db *DeltaDB) refresh(ctx context.Context, r remote, status StatusFunc) {
	if status == nil {
		return
	}
	res := func() *notification.SendResult {
		ctx, cancel := context.WithTimeout(ctx, sendTimeout)
		defer cancel()
		st, err := status(ctx, r.provider)
		res := &notification.SendResult{ID: r.id, ProviderMessageID: r.provider}
		switch {
		case errors.Is(err, notification.ErrStatusUnsupported):
			return nil // not available
		case errors.Is(err, notification.ErrStatusNotFound):
			// provider no longer has the message, so no further updates will ever arrive
			log.Log(ctx, err)
			res.State, res.Details = notification.StateFailedPerm, "message no longer exists at provider; final status unknown"
		case err != nil:
			log.Log(ctx, err)
			return nil
		default:
			res.Status = *st
			if res.State == notification.StateFailedTemp {
				res.State = notification.StateFailedPerm
			}
		}
		res.Sequence = -1
		return res
	}()
	if err := db.UpdateMessageStatus(ctx, res); err != nil {
		log.Log(ctx, errors.Wrap(err, "update stale message status"))
	}
}

// UpdateMessageStatus updates the state of a message. Any pod does, since a
// provider can call any.
func (db *DeltaDB) UpdateMessageStatus(ctx context.Context, res *notification.SendResult) error {
	if res == nil {
		return nil // nothing to do
	}
	if err := permission.LimitCheckAny(ctx, permission.System); err != nil {
		return err
	}
	_, err := db.db.Update(ctx, func(tx *filestore.Tx) error {
		t := db.t.Notification
		var rows []notification.MessageRow
		if m, ok := t.Messages.Get(tx, res.ID); ok && res.ID != "" {
			rows = append(rows, m)
		}
		if id := notification.ProviderMsgIDColumn(res.ProviderMessageID); id != nil {
			if m, ok := t.MessagesByProviderMsgID.Get(tx, *id); ok && (len(rows) == 0 || rows[0].ID != m.ID) {
				rows = append(rows, m)
			}
		}
		for _, m := range rows {
			if err := apply(tx, t, m, res); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

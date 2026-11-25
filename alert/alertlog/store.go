package alertlog

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/sqlc-dev/pqtype"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/integrationkey"
	"github.com/target/goalert/notification/nfydest"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/sqltxn"
	"github.com/target/goalert/util"
	"github.com/target/goalert/util/log"
	"github.com/target/goalert/validation/validate"

	"github.com/pkg/errors"
)

type Store struct {
	db    *sql.DB
	delta *deltaLogs // the Delta backend, if set (delta.go)

	findAll       *sql.Stmt
	findAllByType *sql.Stmt
	findOne       *sql.Stmt

	lookupIKeyType *sql.Stmt

	reg *nfydest.Registry
}

func NewStore(ctx context.Context, db *sql.DB, reg *nfydest.Registry) (*Store, error) {
	p := &util.Prepare{DB: db, Ctx: ctx}

	return &Store{
		db:  db,
		reg: reg,

		lookupIKeyType: p.P(`select "type" from integration_keys where id = $1`),
		findOne: p.P(`
			select
				log.id,
				log.alert_id,
				log.timestamp,
				log.event,
				log.message,
				log.sub_type,
				log.sub_user_id,
				usr.name,
				log.sub_integration_key_id,
				ikey.name,
				log.sub_hb_monitor_id,
				hb.name,
				log.sub_channel_id,
				nc.name,
				log.sub_classifier,
				log.meta
			from alert_logs log
			left join users usr on usr.id = log.sub_user_id
			left join integration_keys ikey on ikey.id = log.sub_integration_key_id
			left join heartbeat_monitors hb on hb.id = log.sub_hb_monitor_id
			left join notification_channels nc on nc.id = log.sub_channel_id
			where log.id = $1
		`),
		findAll: p.P(`
			select
				log.id,
				log.alert_id,
				log.timestamp,
				log.event,
				log.message,
				log.sub_type,
				log.sub_user_id,
				usr.name,
				log.sub_integration_key_id,
				ikey.name,
				log.sub_hb_monitor_id,
				hb.name,
				log.sub_channel_id,
				nc.name,
				log.sub_classifier,
				log.meta
			from alert_logs log
			left join users usr on usr.id = log.sub_user_id
			left join integration_keys ikey on ikey.id = log.sub_integration_key_id
			left join heartbeat_monitors hb on hb.id = log.sub_hb_monitor_id
			left join notification_channels nc on nc.id = log.sub_channel_id
			where log.alert_id = $1
			order by id
		`),
		findAllByType: p.P(`
			select
				log.id,
				log.alert_id,
				log.timestamp,
				log.event,
				log.message,
				log.sub_type,
				log.sub_user_id,
				usr.name,
				log.sub_integration_key_id,
				ikey.name,
				log.sub_hb_monitor_id,
				hb.name,
				log.sub_channel_id,
				nc.name,
				log.sub_classifier,
				log.meta
			from alert_logs log
			left join users usr on usr.id = log.sub_user_id
			left join integration_keys ikey on ikey.id = log.sub_integration_key_id
			left join heartbeat_monitors hb on hb.id = log.sub_hb_monitor_id
			left join notification_channels nc on nc.id = log.sub_channel_id
			where log.alert_id = $1 and log.event = $2
			order by id DESC
			limit 1
		`),
	}, p.Err
}

func (s *Store) MustLog(ctx context.Context, alertID int, _type Type, meta interface{}) {
	s.MustLogTx(ctx, nil, alertID, _type, meta)
}

func (s *Store) MustLogTx(ctx context.Context, tx txn.Tx, alertID int, _type Type, meta interface{}) {
	// suppress duplicate duplicate-suppression logs since they aren't actionable and just add noise
	if s.delta != nil {
		var err error
		if _type == TypeDuplicateSupressed {
			err = s.logDeltaDuplicate(ctx, tx, alertID, meta)
		} else {
			err = s.LogTx(ctx, tx, alertID, _type, meta)
		}
		if err != nil {
			log.Log(ctx, errors.Wrap(err, "append alert log"))
		}
		return
	}

	stx := sqltxn.Unwrap(tx)
	if _type == TypeDuplicateSupressed {
		hasRecent, err := gadb.New(stx).AlertLog_HasRecentDuplicate(ctx, int64(alertID))
		if err != nil {
			log.Log(ctx, errors.Wrap(err, "check for recent duplicate alert log"))
			return
		}

		if hasRecent {
			// skip it if there's been one in the last 5 seconds
			return
		}
	}

	err := s.LogTx(ctx, stx, alertID, _type, meta)
	if err != nil {
		log.Log(ctx, errors.Wrap(err, "append alert log"))
	}
}

func (s *Store) logDeltaDuplicate(ctx context.Context, tx txn.Tx, alertID int, meta interface{}) error {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return err
	}
	return s.delta.logDuplicate(ctx, tx, alertID, meta)
}

// Prepare gets ready to add log entries on behalf of ctx's source. Call it
// before a transaction that does, unless it is the log's own: naming the
// source's destination type can call its provider, and a transaction on the
// Delta backend must do no I/O (F3). On Postgres it does nothing.
func (s *Store) Prepare(ctx context.Context) {
	if s.delta != nil {
		s.delta.prepare(ctx)
	}
}

func (s *Store) LogEPTx(ctx context.Context, tx txn.Tx, epID string, _type Type, meta *EscalationMetaData) error {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return err
	}

	err = validate.UUID("EscalationPolicyID", epID)
	if err != nil {
		return err
	}

	if s.delta != nil {
		return s.delta.logEP(ctx, tx, uuid.MustParse(epID), _type, meta)
	}
	stx := sqltxn.Unwrap(tx)
	e, err := logEntry(ctx, sqlSubjects{s, stx}, _type, meta)
	if err != nil {
		return err
	}
	params := gadb.AlertLog_InsertEPParams{
		EscalationPolicyID:  uuid.MustParse(epID),
		Event:               gadb.EnumAlertLogEvent(e._type),
		SubType:             gadb.NullEnumAlertLogSubjectType{Valid: e.subject._type != SubjectTypeNone, EnumAlertLogSubjectType: gadb.EnumAlertLogSubjectType(e.subject._type)},
		SubUserID:           e.subject.userID,
		SubIntegrationKeyID: e.subject.integrationKeyID,
		SubHbMonitorID:      e.subject.heartbeatMonitorID,
		SubChannelID:        e.subject.channelID,
		SubClassifier:       e.subject.classifier,
		Meta:                pqtype.NullRawMessage{Valid: e.meta != nil, RawMessage: json.RawMessage(e.meta)},
		Message:             e.message,
	}

	return s.queries(stx).AlertLog_InsertEP(ctx, params)
}

func (s *Store) LogServiceTx(ctx context.Context, tx txn.Tx, serviceID string, _type Type, meta interface{}) error {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return err
	}

	err = validate.UUID("ServiceID", serviceID)
	if err != nil {
		return err
	}
	t := _type
	switch _type {
	case TypeAcknowledged:
		t = _TypeAcknowledgeAll
	case TypeClosed:
		t = _TypeCloseAll
	}
	if s.delta != nil {
		return s.delta.logService(ctx, tx, uuid.MustParse(serviceID), t, meta)
	}
	stx := sqltxn.Unwrap(tx)
	e, err := logEntry(ctx, sqlSubjects{s, stx}, t, meta)
	if err != nil {
		return err
	}

	params := gadb.AlertLog_InsertSvcParams{
		ServiceID:           uuid.NullUUID{Valid: true, UUID: uuid.MustParse(serviceID)},
		Event:               gadb.EnumAlertLogEvent(e._type),
		SubType:             gadb.NullEnumAlertLogSubjectType{Valid: e.subject._type != SubjectTypeNone, EnumAlertLogSubjectType: gadb.EnumAlertLogSubjectType(e.subject._type)},
		SubUserID:           e.subject.userID,
		SubIntegrationKeyID: e.subject.integrationKeyID,
		SubHbMonitorID:      e.subject.heartbeatMonitorID,
		SubChannelID:        e.subject.channelID,
		SubClassifier:       e.subject.classifier,
		Meta:                pqtype.NullRawMessage{Valid: e.meta != nil, RawMessage: json.RawMessage(e.meta)},
		Message:             e.message,
	}

	return s.queries(stx).AlertLog_InsertSvc(ctx, params)
}

func (s *Store) LogManyTx(ctx context.Context, tx txn.Tx, alertIDs []int, _type Type, meta interface{}) error {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return err
	}

	if s.delta != nil {
		return s.delta.logMany(ctx, tx, alertIDs, _type, meta)
	}
	stx := sqltxn.Unwrap(tx)
	e, err := logEntry(ctx, sqlSubjects{s, stx}, _type, meta)
	if err != nil {
		return err
	}

	var ids []int64
	for _, id := range alertIDs {
		ids = append(ids, int64(id))
	}

	params := gadb.AlertLog_InsertManyParams{
		Column1:             ids,
		Event:               gadb.EnumAlertLogEvent(e._type),
		SubType:             gadb.NullEnumAlertLogSubjectType{Valid: e.subject._type != SubjectTypeNone, EnumAlertLogSubjectType: gadb.EnumAlertLogSubjectType(e.subject._type)},
		SubUserID:           e.subject.userID,
		SubIntegrationKeyID: e.subject.integrationKeyID,
		SubHbMonitorID:      e.subject.heartbeatMonitorID,
		SubChannelID:        e.subject.channelID,
		SubClassifier:       e.subject.classifier,
		Meta:                pqtype.NullRawMessage{Valid: e.meta != nil, RawMessage: json.RawMessage(e.meta)},
		Message:             e.message,
	}

	return s.queries(stx).AlertLog_InsertMany(ctx, params)
}

func (s *Store) queries(tx txn.Tx) *gadb.Queries {
	stx := sqltxn.Unwrap(tx)
	if stx != nil {
		return gadb.New(stx)
	}
	return gadb.New(s.db)
}

func (s *Store) LogTx(ctx context.Context, tx txn.Tx, alertID int, _type Type, meta interface{}) error {
	return s.LogManyTx(ctx, tx, []int{alertID}, _type, meta)
}

func txWrap(ctx context.Context, tx txn.Tx, stmt *sql.Stmt) *sql.Stmt {
	stx := sqltxn.Unwrap(tx)
	if stx == nil {
		return stmt
	}
	return stx.StmtContext(ctx, stmt)
}

// subjects looks up what describes the subject of a new log entry.
type subjects interface {
	channelDest(ctx context.Context, id uuid.UUID) (gadb.NullDestV1, error)
	contactMethodDest(ctx context.Context, id uuid.UUID) (gadb.NullDestV1, error)
	messageDest(ctx context.Context, id uuid.UUID) (gadb.NullDestV1, error)
	heartbeatMinutes(ctx context.Context, id uuid.UUID) (int32, error)
	integrationKeyType(ctx context.Context, id string) (integrationkey.Type, error)

	// typeName returns the name of a destination type.
	typeName(ctx context.Context, destType string) (string, error)
}

// sqlSubjects looks up subjects in Postgres, in tx if it isn't nil.
type sqlSubjects struct {
	s  *Store
	tx *sql.Tx
}

func (q sqlSubjects) channelDest(ctx context.Context, id uuid.UUID) (gadb.NullDestV1, error) {
	return q.s.queries(q.tx).AlertLog_LookupNCDest(ctx, id)
}

func (q sqlSubjects) contactMethodDest(ctx context.Context, id uuid.UUID) (gadb.NullDestV1, error) {
	return q.s.queries(q.tx).AlertLog_LookupCMDest(ctx, id)
}

func (q sqlSubjects) messageDest(ctx context.Context, id uuid.UUID) (gadb.NullDestV1, error) {
	return q.s.queries(q.tx).AlertLog_LookupCallbackDest(ctx, id)
}

func (q sqlSubjects) heartbeatMinutes(ctx context.Context, id uuid.UUID) (int32, error) {
	return q.s.queries(q.tx).AlertLog_HBIntervalMinutes(ctx, id)
}

func (q sqlSubjects) integrationKeyType(ctx context.Context, id string) (typ integrationkey.Type, err error) {
	err = txWrap(ctx, q.tx, q.s.lookupIKeyType).QueryRowContext(ctx, id).Scan(&typ)
	return typ, err
}

func (q sqlSubjects) typeName(ctx context.Context, destType string) (string, error) {
	info, err := q.s.reg.TypeInfo(ctx, destType)
	if err != nil {
		return "", err
	}
	return info.Name, nil
}

func logEntry(ctx context.Context, look subjects, _type Type, meta interface{}) (*Entry, error) {
	var classExtras []string
	switch _type {
	case _TypeAcknowledgeAll:
		classExtras = append(classExtras, "Ack-All")
		_type = TypeAcknowledged
	case _TypeCloseAll:
		classExtras = append(classExtras, "Close-All")
		_type = TypeClosed
	}

	var r Entry
	r._type = _type
	var err error
	if meta != nil {
		r.meta, err = json.Marshal(meta)
		if err != nil {
			return nil, err
		}
	}

	src := permission.Source(ctx)
	if src != nil {
		switch src.Type {
		case permission.SourceTypeNotificationChannel:
			r.subject._type = SubjectTypeChannel
			id, err := uuid.Parse(src.ID)
			if err != nil {
				return nil, errors.Wrap(err, "parse channel ID")
			}
			dest, err := look.channelDest(ctx, id)
			if err != nil {
				return nil, errors.Wrap(err, "lookup notification channel destination")
			}
			r.subject.classifier, err = look.typeName(ctx, dest.DestV1.Type)
			if err != nil {
				return nil, errors.Wrap(err, "lookup notification channel destination type")
			}

			r.subject.channelID.UUID = id
			r.subject.channelID.Valid = true
		case permission.SourceTypeAuthProvider:
			r.subject.classifier = "Web"
			r.subject._type = SubjectTypeUser
			r.subject.userID = permission.UserNullUUID(ctx)
		case permission.SourceTypeContactMethod:
			r.subject._type = SubjectTypeUser
			r.subject.userID = permission.UserNullUUID(ctx)
			if _type == TypeNoNotificationSent {
				// no CMID for no notification sent
				r.subject.classifier = "no immediate rule"
				break
			}
			dest, err := look.contactMethodDest(ctx, uuid.MustParse(src.ID))
			if err != nil {
				return nil, errors.Wrap(err, "lookup contact method type for callback ID")
			}
			r.subject.classifier, err = look.typeName(ctx, dest.DestV1.Type)
			if err != nil {
				return nil, errors.Wrap(err, "lookup contact method type")
			}

		case permission.SourceTypeNotificationCallback:
			r.subject._type = SubjectTypeUser
			r.subject.userID = permission.UserNullUUID(ctx)

			id, err := uuid.Parse(src.ID)
			if err != nil {
				return nil, errors.Wrap(err, "parse channel ID")
			}
			dest, err := look.messageDest(ctx, id)
			if err != nil {
				return nil, errors.Wrap(err, "lookup notification callback type")
			}

			r.subject.classifier, err = look.typeName(ctx, dest.DestV1.Type)
			if err != nil {
				return nil, errors.Wrap(err, "lookup notification channel destination type")
			}
		case permission.SourceTypeHeartbeat:
			r.subject._type = SubjectTypeHeartbeatMonitor
			minutes, err := look.heartbeatMinutes(ctx, uuid.MustParse(src.ID))
			if err != nil {
				return nil, errors.Wrap(err, "lookup heartbeat monitor interval by ID")
			}
			if r.Type() == TypeCreated {
				s := "s"
				if minutes == 1 {
					s = ""
				}
				r.subject.classifier = fmt.Sprintf("expired after %d minute"+s, minutes)
			} else if r.Type() == TypeClosed {
				r.subject.classifier = "healthy"
			}

			r.subject.heartbeatMonitorID.Valid = true
			r.subject.heartbeatMonitorID.UUID = uuid.MustParse(src.ID)
		case permission.SourceTypeIntegrationKey:
			r.subject._type = SubjectTypeIntegrationKey
			ikeyType, err := look.integrationKeyType(ctx, src.ID)
			if err != nil {
				return nil, errors.Wrap(err, "lookup integration key type by ID")
			}
			switch ikeyType {
			case integrationkey.TypeGeneric:
				r.subject.classifier = "Generic API"
			case integrationkey.TypeGrafana:
				r.subject.classifier = "Grafana"
			case integrationkey.TypeSite24x7:
				r.subject.classifier = "Site24x7"
			case integrationkey.TypeEmail:
				r.subject.classifier = "Email"
			}
			r.subject.integrationKeyID.Valid = true
			r.subject.integrationKeyID.UUID = uuid.MustParse(src.ID)
		}
	}

	if r.subject.classifier != "" {
		classExtras = append([]string{r.subject.classifier}, classExtras...)
	}
	r.subject.classifier = strings.Join(classExtras, ", ")

	return &r, nil
}

func (s *Store) FindOne(ctx context.Context, logID int) (*Entry, error) {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}

	if s.delta != nil {
		return s.delta.findOne(ctx, logID)
	}
	var e Entry
	row := s.findOne.QueryRowContext(ctx, logID)
	err = e.scanWith(row.Scan)
	if err != nil {
		return nil, err
	}

	return &e, nil
}

// FindLatestByType returns the latest Log Entry given alertID and status type
func (s *Store) FindLatestByType(ctx context.Context, alertID int, status Type) (*Entry, error) {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}
	if s.delta != nil {
		return s.delta.findLatestByType(ctx, alertID, status)
	}
	var e Entry
	row := s.findAllByType.QueryRowContext(ctx, alertID, status)
	err = e.scanWith(row.Scan)
	if err != nil {
		return nil, err
	}
	return &e, nil
}

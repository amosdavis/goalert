package alertlog

import (
	"context"
	"encoding/json"

	"strings"

	"github.com/google/uuid"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/integrationkey"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/util/log"
	"github.com/target/goalert/validation/validate"

	"github.com/pkg/errors"
)

// Store provides alert log operations on the Delta backend.
type Store struct {
	delta *deltaLogs
}

func (s *Store) MustLog(ctx context.Context, alertID int, _type Type, meta interface{}) {
	s.MustLogTx(ctx, nil, alertID, _type, meta)
}

func (s *Store) MustLogTx(ctx context.Context, tx txn.Tx, alertID int, _type Type, meta interface{}) {
	// suppress duplicate duplicate-suppression logs since they aren't actionable and just add noise
	var err error
	if _type == TypeDuplicateSupressed {
		err = s.logDeltaDuplicate(ctx, tx, alertID, meta)
	} else {
		err = s.LogTx(ctx, tx, alertID, _type, meta)
	}
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
// Delta backend must do no I/O (F3).
func (s *Store) Prepare(ctx context.Context) {
	s.delta.prepare(ctx)
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

	return s.delta.logEP(ctx, tx, uuid.MustParse(epID), _type, meta)
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
	return s.delta.logService(ctx, tx, uuid.MustParse(serviceID), t, meta)
}

func (s *Store) LogManyTx(ctx context.Context, tx txn.Tx, alertIDs []int, _type Type, meta interface{}) error {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return err
	}

	return s.delta.logMany(ctx, tx, alertIDs, _type, meta)
}

func (s *Store) LogTx(ctx context.Context, tx txn.Tx, alertID int, _type Type, meta interface{}) error {
	return s.LogManyTx(ctx, tx, []int{alertID}, _type, meta)
}

// subjects looks up what describes the subject of a new log entry.
type subjects interface {
	channelDest(ctx context.Context, id uuid.UUID) (gadb.NullDestV1, error)
	contactMethodDest(ctx context.Context, id uuid.UUID) (gadb.NullDestV1, error)
	messageDest(ctx context.Context, id uuid.UUID) (gadb.NullDestV1, error)

	integrationKeyType(ctx context.Context, id string) (integrationkey.Type, error)

	// typeName returns the name of a destination type.
	typeName(ctx context.Context, destType string) (string, error)
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

		case permission.SourceTypeIntegrationKey:
			r.subject._type = SubjectTypeIntegrationKey
			ikeyType, err := look.integrationKeyType(ctx, src.ID)
			if err != nil {
				return nil, errors.Wrap(err, "lookup integration key type by ID")
			}
			if ikeyType == integrationkey.TypeGeneric {
				r.subject.classifier = "Generic API"
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

	return s.delta.findOne(ctx, logID)
}

// FindLatestByType returns the latest Log Entry given alertID and status type
func (s *Store) FindLatestByType(ctx context.Context, alertID int, status Type) (*Entry, error) {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}
	return s.delta.findLatestByType(ctx, alertID, status)
}

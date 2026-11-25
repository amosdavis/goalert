package integrationkey

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/sqltxn"
	"github.com/target/goalert/util/log"
	"github.com/target/goalert/validation"
	"github.com/target/goalert/validation/validate"
)

const (
	MaxRules   = 100
	MaxActions = 10
	MaxParams  = 10
)

func destHash(dest gadb.DestV1) (hash [32]byte) {
	data, err := json.Marshal(dest)
	if err != nil {
		panic(err)
	}
	h := sha256.New()
	_, _ = h.Write(data)
	copy(hash[:], h.Sum(nil))
	return hash
}

func (s *Store) validateActions(ctx context.Context, fname string, actions []gadb.UIKActionV1) error {
	err := validate.Len(fname, actions, 0, MaxActions)
	if err != nil {
		return err
	}

	uniqDest := make(map[[32]byte]struct{}, len(actions))
	for i, a := range actions {
		err := s.reg.ValidateAction(ctx, a)
		if err != nil {
			return err
		}
		hash := destHash(a.Dest)
		if _, ok := uniqDest[hash]; ok {
			name, err := s.reg.LookupTypeName(ctx, a.Dest.Type)
			if err != nil {
				// Handle error by logging it and using the type ID as the name.
				// This is unlikely since the destination type should have been validated earlier.
				log.Log(ctx, err)
				name = a.Dest.Type
			}
			return validation.NewFieldErrorf(fmt.Sprintf("%s[%d]", fname, i), "duplicate destination '%s' not allowed", name)
		}
		uniqDest[hash] = struct{}{}
	}

	return nil
}

func (s *Store) ValidateUIKConfigV1(ctx context.Context, cfg gadb.UIKConfigV1) error {
	err := validate.Many(
		validate.Len("Rules", cfg.Rules, 0, MaxRules),
		s.validateActions(ctx, "DefaultActions", cfg.DefaultActions),
	)
	if err != nil {
		return err
	}

	for i, r := range cfg.Rules {
		field := fmt.Sprintf("Rules[%d]", i)
		err := validate.Many(
			validate.Name(field+".Name", r.Name),
			validate.Text(field+".Description", r.Description, 0, 255), // these are arbitrary and will likely change as the feature is developed
			validate.Text(field+".ConditionExpr", r.ConditionExpr, 1, 1024),
			s.validateActions(ctx, field+".Actions", r.Actions),
		)
		if err != nil {
			return err
		}
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}

	if len(data) > 64*1024 {
		return validation.NewFieldError("Config", "must be less than 64KiB in total")
	}

	return nil
}

func (s *Store) Config(ctx context.Context, tx txn.Tx, keyID uuid.UUID) (*gadb.UIKConfigV1, error) {
	err := permission.LimitCheckAny(ctx, permission.User, permission.Service)
	if err != nil {
		return nil, err
	}

	var cfg gadb.UIKConfig
	if s.delta != nil {
		cfg, err = s.delta.config(ctx, tx, keyID)
	} else {
		cfg, err = gadb.New(sqltxn.DBTX(s.db, tx)).IntKeyGetConfig(ctx, keyID)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return &gadb.UIKConfigV1{}, nil
	}
	if err != nil {
		return nil, err
	}

	if cfg.Version != 1 {
		return nil, fmt.Errorf("unsupported config version: %d", cfg.Version)
	}

	return &cfg.V1, nil
}

func (s *Store) SetConfig(ctx context.Context, tx txn.Tx, keyID uuid.UUID, cfg *gadb.UIKConfigV1) error {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return err
	}

	if cfg != nil {
		err := s.ValidateUIKConfigV1(ctx, *cfg)
		if err != nil {
			return err
		}
	}

	keyType, err := s.keyType(ctx, tx, keyID)
	if err != nil {
		return err
	}
	if keyType != TypeUniversal {
		return validation.NewGenericError("config only supported for universal keys")
	}

	if cfg == nil {
		if s.delta != nil {
			return s.delta.deleteConfig(ctx, tx, keyID)
		}
		return gadb.New(sqltxn.DBTX(s.db, tx)).IntKeyDeleteConfig(ctx, keyID)
	}

	// ensure all rule IDs are set, and all actions have a channel
	for i := range cfg.Rules {
		if cfg.Rules[i].ID == uuid.Nil {
			cfg.Rules[i].ID = uuid.New()
		}
		err := s.setActionChannels(ctx, tx, cfg.Rules[i].Actions)
		if err != nil {
			return err
		}
	}
	err = s.setActionChannels(ctx, tx, cfg.DefaultActions)
	if err != nil {
		return err
	}

	if s.delta != nil {
		return s.delta.setConfig(ctx, tx, keyID, cfg)
	}
	err = gadb.New(sqltxn.DBTX(s.db, tx)).IntKeySetConfig(ctx, gadb.IntKeySetConfigParams{
		ID:     keyID,
		Config: gadb.UIKConfig{Version: 1, V1: *cfg},
	})
	if err != nil {
		return err
	}

	return nil
}

// keyType returns a key's type, or sql.ErrNoRows if it doesn't exist.
func (s *Store) keyType(ctx context.Context, tx txn.Tx, keyID uuid.UUID) (Type, error) {
	if s.delta != nil {
		return s.delta.keyType(ctx, tx, keyID)
	}
	t, err := gadb.New(sqltxn.DBTX(s.db, tx)).IntKeyGetType(ctx, keyID)
	return Type(t), err
}

func (s *Store) setActionChannels(ctx context.Context, tx txn.Tx, actions []gadb.UIKActionV1) error {
	for i, act := range actions {
		ok, err := s.reg.IsDynamicAction(ctx, act.Dest.Type)
		if err != nil {
			return err
		}

		if !ok {
			return validation.NewFieldError(fmt.Sprintf("Actions[%d]", i), "invalid destination type")
		}

		actions[i].ChannelID, err = s.ncStore.MapDestToID(ctx, tx, act.Dest)
		if err != nil {
			return err
		}
	}

	return nil
}

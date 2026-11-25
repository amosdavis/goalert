package keyring

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util/log"
)

// keyringRow is a keyring row on the Delta backend.
type keyringRow struct {
	ID               string     `parquet:"id"`
	NextKey          []byte     `parquet:"next_key"`
	NextRotation     *time.Time `parquet:"next_rotation,optional,timestamp(microsecond:utc)"`
	RotationCount    int64      `parquet:"rotation_count"`
	SigningKey       []byte     `parquet:"signing_key"`
	VerificationKeys []byte     `parquet:"verification_keys"`
}

// DeltaTables are the keyring tables on the Delta backend.
type DeltaTables struct {
	keyrings *filestore.Table[keyringRow]
}

// DefineDeltaTables adds the keyring tables to the Delta backend's schema:
// keyring, keyed by id (keyring_pkey).
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	return &DeltaTables{
		keyrings: filestore.NewTable(s, "keyring", func(r keyringRow) string { return r.ID }),
	}
}

// NewDeltaDB returns a keyring on the Delta backend. Every keyring shares
// the table, one row per keyring name.
func NewDeltaDB(ctx context.Context, logger *log.Logger, db txn.DB, tables *DeltaTables, cfg *Config) (*DB, error) {
	d, err := newDB(logger, cfg)
	if err != nil {
		return nil, err
	}
	d.txdb, d.delta = db, tables
	return d.start(ctx)
}

// ReEncrypt encrypts the secrets of every keyring with the first of keys. The
// others can decrypt them too.
func (t *DeltaTables) ReEncrypt(tx *filestore.Tx, keys Keys) error {
	var rows []keyringRow
	t.keyrings.Ascend(tx, func(r keyringRow) bool { rows = append(rows, r); return true })
	for _, r := range rows {
		var err error
		if r.SigningKey, err = keys.reEncrypt(r.SigningKey); err != nil {
			return fmt.Errorf("signing key for '%s': %w", r.ID, err)
		}
		if r.NextKey, err = keys.reEncrypt(r.NextKey); err != nil {
			return fmt.Errorf("next key for '%s': %w", r.ID, err)
		}
		if err := t.keyrings.Put(tx, r); err != nil {
			return fmt.Errorf("update keyring secrets for '%s': %w", r.ID, err)
		}
	}
	return nil
}

func (k Keys) reEncrypt(data []byte) ([]byte, error) {
	dec, label, err := k.Decrypt(data)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}
	enc, err := k.Encrypt(label, dec)
	if err != nil {
		return nil, fmt.Errorf("encrypt: %w", err)
	}
	return enc, nil
}

// nextRotation is when a keyring set up or rotated at now rotates next.
func (db *DB) nextRotation(now time.Time) *time.Time {
	if db.cfg.RotationDays <= 0 {
		return nil
	}
	// An explicit wait rather than a date, so that pods agree on handoffs
	// whatever their time zones.
	t := now.Add(24 * time.Hour * time.Duration(db.cfg.RotationDays))
	return &t
}

// deltaRefreshAndRotate is refreshAndRotateKeys on the Delta backend. If
// two pods create the keyring at once, the one whose commit loses runs
// again and uses the other's keys, as the Postgres store's "on conflict do
// nothing" does. If both find a rotation due, the second run finds it done
// and doesn't rotate again (F4).
func (db *DB) deltaRefreshAndRotate(ctx context.Context, forceRotation bool) error {
	var (
		verificationKeys map[byte]ecdsa.PublicKey
		signKeyData      []byte
		count            int
	)
	err := db.txdb.Run(ctx, nil, func(tx txn.Tx) error {
		ftx, r := deltatxn.Unwrap(tx)
		now := ftx.Now()

		row, ok := db.delta.keyrings.Get(r, db.cfg.Name)
		if !ok {
			signKey, signData, err := db.newKey()
			if err != nil {
				return err
			}
			nextKey, nextData, err := db.newKey()
			if err != nil {
				return err
			}
			v := map[byte]ecdsa.PublicKey{0: signKey.PublicKey, 1: nextKey.PublicKey}
			vData, err := marshalVerificationKeys(v)
			if err != nil {
				return err
			}
			verificationKeys, signKeyData, count = v, signData, 0
			return db.delta.keyrings.Put(ftx, keyringRow{
				ID: db.cfg.Name, VerificationKeys: vData, SigningKey: signData, NextKey: nextData,
				NextRotation: db.nextRotation(now),
			})
		}

		v, err := parseVerificationKeys(row.VerificationKeys)
		if err != nil {
			return errors.Wrap(err, "unmarshal verification keys")
		}
		verificationKeys, signKeyData, count = v, row.SigningKey, int(row.RotationCount)
		if !forceRotation && (row.NextRotation == nil || now.Before(*row.NextRotation)) {
			return nil
		}

		// Rotate: the next key signs from now on, and a new one is next.
		nextKey, nextData, err := db.newKey()
		if err != nil {
			return err
		}
		count++
		verificationKeys = db.rotateVerificationKeys(v, count, nextKey.PublicKey)
		vData, err := marshalVerificationKeys(verificationKeys)
		if err != nil {
			return err
		}
		signKeyData = row.NextKey
		row.VerificationKeys, row.SigningKey, row.NextKey = vData, signKeyData, nextData
		row.NextRotation, row.RotationCount = db.nextRotation(now), int64(count)
		return db.delta.keyrings.Put(ftx, row)
	})
	if err != nil {
		return err
	}

	signKey, err := db.loadKey(signKeyData)
	if err != nil {
		// Without the signing key, the verification keys still work.
		log.Log(ctx, errors.Wrap(err, "load signing key"))
	}

	db.mx.Lock()
	defer db.mx.Unlock()
	db.verificationKeys = verificationKeys
	db.signingKey = signKey
	db.rotationCount = count
	return nil
}

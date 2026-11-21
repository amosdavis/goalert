package schedule

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/sqltxn"
	"github.com/target/goalert/util/jsonutil"
	"github.com/target/goalert/util/sqlutil"
)

func (store *Store) scheduleData(ctx context.Context, tx txn.Tx, scheduleID uuid.UUID) (*Data, error) {
	stx := sqltxn.Unwrap(tx)
	db := gadb.New(store.db)
	if stx != nil {
		db = db.WithTx(stx)
	}
	rawData, err := db.SchedFindData(ctx, scheduleID)
	if err == sql.ErrNoRows {
		err = nil
	}
	if err != nil {
		return nil, err
	}

	var data Data
	if len(rawData) > 0 {
		err = json.Unmarshal(rawData, &data)
		if err != nil {
			return nil, err
		}
	}

	return &data, nil
}

func (store *Store) updateScheduleData(ctx context.Context, tx txn.Tx, scheduleID uuid.UUID, apply func(data *Data) error) error {
	stx := sqltxn.Unwrap(tx)
	var err error
	externalTx := stx != nil
	if !externalTx {
		stx, err = store.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer sqlutil.Rollback(ctx, "schedule: update data", stx)
	}

	db := gadb.New(store.db).WithTx(stx)

	var rawData json.RawMessage
	// Select for update, if it does not exist try inserting, if that fails due to a race, re-try select for update
	rawData, err = db.SchedFindDataForUpdate(ctx, scheduleID)
	if err == sql.ErrNoRows {
		err = db.SchedInsertData(ctx, scheduleID)
		if isDataPkeyConflict(err) {
			// insert happened after orig. select for update and our subsequent insert, re-try select for update
			rawData, err = db.SchedFindDataForUpdate(ctx, scheduleID)
		}
	}
	if err != nil {
		return err
	}

	var data Data
	if len(rawData) > 0 {
		err = json.Unmarshal(rawData, &data)
		if err != nil {
			return err
		}
	}

	err = apply(&data)
	if err != nil {
		return err
	}

	// preserve unknown fields
	rawData, err = jsonutil.Apply(rawData, data)
	if err != nil {
		return err
	}

	err = db.SchedUpdateData(ctx, gadb.SchedUpdateDataParams{
		ScheduleID: scheduleID,
		Data:       rawData,
	})
	if err != nil {
		return err
	}

	if !externalTx {
		return stx.Commit()
	}

	return nil
}

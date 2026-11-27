// Package fixture runs the SQL of test fixtures on a Delta table. It knows the
// statements that the smoke tests use, and nothing else:
//
//	INSERT INTO table (columns) VALUES (expressions), ...
//	UPDATE table SET column = expression, ... [WHERE column = expression AND ...]
//	DELETE FROM table [WHERE column = expression AND ...]
//	SET TIMEZONE = 'zone'
//
// Columns that are left out get the defaults that they had in Postgres. Rows are
// written as GoAlert writes them, with all that was a trigger or constraint in
// Postgres, so a fixture can't make data that GoAlert could not.
package fixture

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/alert"
	"github.com/target/goalert/deltastores"
	"github.com/target/goalert/escalation"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/notification"
	"github.com/target/goalert/notificationchannel"
	"github.com/target/goalert/oncall"
	"github.com/target/goalert/schedule/rotation"
	"github.com/target/goalert/user"
	"github.com/target/goalert/user/contactmethod"
)

// DB is the table that fixtures are run on.
type DB struct {
	Delta  *filestore.DeltaDB
	Stores *deltastores.Stores
}

// Exec runs SQL statements in one transaction.
func (db *DB) Exec(ctx context.Context, sql string) error {
	stmts, err := parse(sql)
	if err != nil {
		return fmt.Errorf("fixture: %w", err)
	}
	_, err = db.Delta.Update(ctx, func(tx *filestore.Tx) error {
		x := &execution{tx: tx, schema: db.Delta.Schema(), s: db.Stores, zone: time.UTC}
		for _, st := range stmts {
			if err := st.run(x); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("fixture: %w", err)
	}
	return nil
}

type execution struct {
	tx     *filestore.Tx
	schema *filestore.Schema
	s      *deltastores.Stores
	zone   *time.Location // of the session
}

func (st setZoneStmt) run(x *execution) (err error) {
	x.zone, err = time.LoadLocation(st.zone)
	return err
}

func (x *execution) table(name string) (filestore.Rows, error) {
	rows, ok := x.schema.Rows(name)
	if !ok {
		return rows, fmt.Errorf("there is no table %s", name)
	}
	return rows, nil
}

// field is the field of a row (addressable) for a column.
func field(row reflect.Value, col string) (reflect.Value, error) {
	for i := range row.NumField() {
		name, _, _ := strings.Cut(row.Type().Field(i).Tag.Get("parquet"), ",")
		if name == col {
			return row.Field(i), nil
		}
	}
	return reflect.Value{}, fmt.Errorf("there is no column %s", col)
}

func (x *execution) set(row reflect.Value, col string, e expr) error {
	f, err := field(row, col)
	if err != nil {
		return err
	}
	v, err := e.eval(x)
	if err == nil {
		err = x.assign(f, v)
	}
	if err != nil {
		return fmt.Errorf("%s: %w", col, err)
	}
	return nil
}

func (st insertStmt) run(x *execution) error {
	rows, err := x.table(st.table)
	if err != nil {
		return err
	}
	for _, vals := range st.rows {
		row := reflect.New(rows.Type()).Elem()
		err := x.setDefaults(st.table, row, st.cols)
		for i, col := range st.cols {
			if droppedColumns[st.table][col] {
				continue
			}
			if err == nil {
				err = x.set(row, col, vals[i])
			}
		}
		if err == nil {
			err = x.insert(st.table, rows, row.Addr().Interface())
		}
		if err != nil {
			return fmt.Errorf("insert into %s: %w", st.table, err)
		}
	}
	return nil
}

type defaultValue func(x *execution) (any, error)

func val(v any) defaultValue { return func(*execution) (any, error) { return v, nil } }

func nowValue(x *execution) (any, error) { return x.tx.Now(), nil }

func newID(*execution) (any, error) { return literal(uuid.NewString()), nil }

func seq(name string) defaultValue {
	return func(x *execution) (any, error) {
		id, err := x.tx.NextID(name)
		return id, err
	}
}

// defaults are the columns' defaults in Postgres that are not what a Go field
// starts with, and that no function that inserts the row sets.
var defaults = map[string]map[string]defaultValue{
	"alert_logs":                 {"id": seq("alert_logs_id_seq"), "timestamp": nowValue},
	"auth_basic_users":           {"id": seq("auth_basic_users_id_seq")},
	"ep_step_on_call_users":      {"id": seq("ep_step_on_call_users_id_seq"), "start_time": nowValue},
	"escalation_policies":        {"id": newID},
	"escalation_policy_steps":    {"delay": val(int64(1))},
	"integration_keys":           {"id": newID},
	"labels":                     {"id": seq("labels_id_seq")},
	"notification_policy_cycles": {"id": newID, "started_at": nowValue},
	"rotation_participants":      {"id": newID},
	"rotations":                  {"id": newID, "shift_length": val(int64(1)), "start_time": nowValue},
	"schedule_data":              {"id": seq("schedule_data_id_seq")},
	"schedule_on_call_users":     {"id": seq("schedule_on_call_users_id_seq"), "start_time": nowValue},
	"schedules":                  {"id": newID},
	"services":                   {"id": newID},
	"user_contact_methods":       {"pending": val(true)},
	"user_favorites":             {"id": seq("user_favorites_id_seq")},
	"user_notification_rules":    {"id": newID, "created_at": nowValue},
	"user_verification_codes":    {"id": newID},
	"users":                      {"id": newID, "role": val(literal("unknown"))},
	"notification_channels":      {"id": newID},
	"schedule_rules": {
		"id": newID, "created_at": nowValue, "start_time": val(clockTime(0)), "end_time": val(clockTime(23*time.Hour + 59*time.Minute + 59*time.Second)),
		"sunday": val(true), "monday": val(true), "tuesday": val(true), "wednesday": val(true),
		"thursday": val(true), "friday": val(true), "saturday": val(true),
	},

	// These have functions that set their defaults.
	"alerts": {}, "escalation_policy_actions": {}, "escalation_policy_state": {}, "outgoing_messages": {},
}

// droppedColumns are columns the Delta schema no longer needs. A fixture may
// still name them, as the old SQL did, without changing the new behavior.
var droppedColumns = map[string]map[string]bool{
	"notification_policy_cycles": {"checked": true, "repeat_count": true},
}

func (x *execution) setDefaults(table string, row reflect.Value, given []string) error {
	cols, ok := defaults[table]
	if !ok {
		return errors.New("fixtures can't insert into this table yet")
	}
	for col, def := range cols {
		if containsString(given, col) {
			continue
		}
		v, err := def(x)
		if err != nil {
			return err
		}
		if err := x.set(row, col, constant{v}); err != nil {
			return err
		}
	}
	return nil
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// insert adds a row, with everything that the triggers on an insert did.
func (x *execution) insert(table string, rows filestore.Rows, row any) error {
	t := x.s.Tables
	switch r := row.(type) {
	case *alert.DeltaAlert:
		_, err := x.s.Alert.DeltaInsert(x.tx, *r)
		return err
	case *contactmethod.Row:
		return t.ContactMethod.Insert(x.tx, *r, -1)
	case *escalation.StepRow:
		_, err := t.Escalation.InsertStep(x.tx, *r)
		return err
	case *escalation.ActionRow:
		return t.Escalation.InsertAction(x.tx, *r)
	case *escalation.StateRow:
		return t.Escalation.InsertState(x.tx, *r)
	case *notification.MessageRow:
		_, err := t.Notification.InsertMessage(x.tx, *r)
		return err
	case *notificationchannel.Row:
		return t.NotificationChannel.Insert(x.tx, *r)
	case *oncall.ScheduleOnCallRow:
		if err := rows.Insert(x.tx, r, table+"_pkey"); err != nil {
			return err
		}
		return t.OnCall.PutScheduleOnCall(x.tx, *r) // its constraint
	case *rotation.ParticipantRow:
		return x.s.Rotation.DeltaInsertParticipant(x.tx, *r)
	}
	if table == "auth_basic_users" {
		if err := rows.Insert(x.tx, row, table+"_pkey"); err != nil {
			return err
		}
		// trg_insert_basic_user also links the username to the user.
		v := reflect.ValueOf(row).Elem()
		userID, _ := field(v, "user_id")
		username, _ := field(v, "username")
		return x.s.Tables.User.SetAuthSubject(x.tx, user.AuthSubject{
			ProviderID: "basic", SubjectID: username.String(), UserID: userID.String(),
		})
	}
	if table == "schedule_rules" {
		if err := normalizeClocks(reflect.ValueOf(row).Elem(), "start_time", "end_time"); err != nil {
			return err
		}
	}
	return rows.Insert(x.tx, row, table+"_pkey")
}

// normalizeClocks makes times of day what a column of type time would have.
func normalizeClocks(row reflect.Value, cols ...string) error {
	for _, col := range cols {
		f, err := field(row, col)
		if err != nil {
			return err
		}
		c, err := parseClock(f.String())
		if err != nil {
			return err
		}
		f.SetString(c.String())
	}
	return nil
}

// matching returns the rows that have the values.
func (x *execution) matching(table string, where []columnValue) (rows filestore.Rows, found []any, err error) {
	rows, err = x.table(table)
	if err != nil {
		return rows, nil, err
	}
	want := reflect.New(rows.Type()).Elem()
	for _, w := range where {
		if err := x.set(want, w.col, w.val); err != nil {
			return rows, nil, err
		}
	}
	rows.Ascend(x.tx, func(row any) bool {
		v := reflect.ValueOf(row).Elem()
		for _, w := range where {
			have, _ := field(v, w.col)
			wanted, _ := field(want, w.col)
			// A null is not equal to anything, not even a null.
			if wanted.Kind() == reflect.Pointer && wanted.IsNil() || !reflect.DeepEqual(have.Interface(), wanted.Interface()) {
				return true
			}
		}
		found = append(found, row)
		return true
	})
	return rows, found, nil
}

func (st updateStmt) run(x *execution) error {
	if err := x.update(st); err != nil {
		return fmt.Errorf("update %s: %w", st.table, err)
	}
	return nil
}

func (x *execution) update(st updateStmt) error {
	rows, found, err := x.matching(st.table, st.where)
	if err != nil {
		return err
	}
	for _, old := range found {
		row := reflect.New(rows.Type())
		row.Elem().Set(reflect.ValueOf(old).Elem())
		for _, s := range st.set {
			if err := x.set(row.Elem(), s.col, s.val); err != nil {
				return err
			}
		}
		oldKey, _ := rows.Key(old)
		if key, err := rows.Key(row.Interface()); err != nil || key != oldKey {
			return errors.New("fixtures can't change a primary key")
		}
		switch r := row.Interface().(type) {
		case *escalation.StateRow:
			_, err = x.s.Tables.Escalation.UpdateState(x.tx, r.AlertID, func(st *escalation.StateRow) { *st = *r })
		case *notification.MessageRow:
			err = x.s.Tables.Notification.SetMessageStatus(x.tx, *r)
		case *rotation.StateRow:
			// trg_set_rot_state_pos_on_active_change
			if p, ok := x.s.Tables.Rotation.Participants.Get(x.tx, r.ParticipantID); ok {
				r.Position = p.Position
			}
			err = rows.Put(x.tx, r)
		default:
			if !plainUpdates[st.table] {
				return errors.New("fixtures can't update this table yet")
			}
			err = rows.Put(x.tx, r)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// plainUpdates are the tables with no triggers on updates of their rows.
var plainUpdates = map[string]bool{"users": true, "config_limits": true}

func (st deleteStmt) run(x *execution) error {
	if !plainDeletes[st.table] {
		return fmt.Errorf("delete from %s: fixtures can't delete from this table yet", st.table)
	}
	rows, found, err := x.matching(st.table, st.where)
	for _, row := range found {
		if err == nil {
			key, _ := rows.Key(row)
			_, err = rows.Delete(x.tx, key)
		}
	}
	if err != nil {
		return fmt.Errorf("delete from %s: %w", st.table, err)
	}
	return nil
}

// plainDeletes are the tables with no triggers on deletes of their rows.
var plainDeletes = map[string]bool{"alert_logs": true}

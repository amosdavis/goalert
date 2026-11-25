package label

import (
	"context"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/target/goalert/assignment"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
)

// labelRow is a labels row on the Delta backend; the store sets exactly one
// target.
type labelRow struct {
	ID            int64   `parquet:"id"`
	Key           string  `parquet:"key"`
	Value         string  `parquet:"value"`
	TgtServiceID  *string `parquet:"tgt_service_id,optional"`
	TgtScheduleID *string `parquet:"tgt_schedule_id,optional"`
	TgtRotationID *string `parquet:"tgt_rotation_id,optional"`
	TgtEPID       *string `parquet:"tgt_ep_id,optional"`
}

func newLabelRow(id int64, l *Label) labelRow {
	return labelRow{
		ID: id, Key: l.Key, Value: l.Value,
		TgtServiceID:  optID(l.tgtSvc()),
		TgtScheduleID: optID(l.tgtSched()),
		TgtRotationID: optID(l.tgtRot()),
		TgtEPID:       optID(l.tgtEP()),
	}
}

func optID(id uuid.NullUUID) *string {
	if !id.Valid {
		return nil
	}
	s := id.UUID.String()
	return &s
}

// targetKeys are the row's keys in the index of labels by target.
func (r labelRow) targetKeys() []string {
	var keys []string
	add := func(t assignment.TargetType, id *string) {
		if id != nil {
			keys = append(keys, t.String()+"/"+*id)
		}
	}
	add(assignment.TargetTypeService, r.TgtServiceID)
	add(assignment.TargetTypeSchedule, r.TgtScheduleID)
	add(assignment.TargetTypeRotation, r.TgtRotationID)
	add(assignment.TargetTypeEscalationPolicy, r.TgtEPID)
	return keys
}

// resourceKey is the row's key in idx_labels_resource_id: a target has one
// label with a key. On Postgres the index doesn't ensure that, because nulls
// are distinct there, and every row has three targets that are null.
func (r labelRow) resourceKey() (string, bool) {
	id := func(p *string) string { s, _ := filestore.Ref(p); return s }
	return strings.Join([]string{id(r.TgtServiceID), id(r.TgtScheduleID), id(r.TgtRotationID), id(r.TgtEPID), r.Key}, "/"), true
}

// DeltaTables are the label store's tables on the Delta backend.
type DeltaTables struct {
	labels   *filestore.Table[labelRow]
	byTarget *filestore.Index[labelRow]
	byKey    *filestore.Index[labelRow]
}

// DefineDeltaTables adds the label store's tables to the Delta backend's
// schema: labels, keyed by id (labels_pkey).
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	t := &DeltaTables{labels: filestore.NewTable(s, "labels", func(r labelRow) string { return filestore.IntKey(r.ID) })}
	t.byTarget = t.labels.Index("labels_tgt", labelRow.targetKeys)
	t.byKey = t.labels.Index("labels_key", func(r labelRow) []string {
		if r.Key == "" {
			return nil
		}
		return []string{r.Key}
	})
	t.labels.Unique("idx_labels_resource_id", labelRow.resourceKey)
	return t
}

// DeclareDeltaForeignKeys declares this package's foreign keys to other
// packages' tables.
func DeclareDeltaForeignKeys[Svc, Sched, Rot, EP any](t *DeltaTables, services *filestore.Table[Svc], schedules *filestore.Table[Sched], rotations *filestore.Table[Rot], policies *filestore.Table[EP]) {
	filestore.References(t.labels, "labels_tgt_service_id_fkey", services,
		func(r labelRow) (string, bool) { return filestore.Ref(r.TgtServiceID) }, filestore.Cascade, nil)
	filestore.References(t.labels, "labels_tgt_schedule_id_fkey", schedules,
		func(r labelRow) (string, bool) { return filestore.Ref(r.TgtScheduleID) }, filestore.Cascade, nil)
	filestore.References(t.labels, "labels_tgt_rotation_id_fkey", rotations,
		func(r labelRow) (string, bool) { return filestore.Ref(r.TgtRotationID) }, filestore.Cascade, nil)
	filestore.References(t.labels, "labels_tgt_ep_id_fkey", policies,
		func(r labelRow) (string, bool) { return filestore.Ref(r.TgtEPID) }, filestore.Cascade, nil)
}

// ServicesWithLabel returns, in r, the IDs of the services that have a label
// with the key, and with the value unless it is empty. The service store's
// search uses it.
func (t *DeltaTables) ServicesWithLabel(r filestore.Reader, key, value string) []string {
	var ids []string
	seen := make(map[string]bool)
	t.byKey.Ascend(r, key, func(row labelRow) bool {
		if row.TgtServiceID != nil && (value == "" || row.Value == value) && !seen[*row.TgtServiceID] {
			seen[*row.TgtServiceID] = true
			ids = append(ids, *row.TgtServiceID)
		}
		return true
	})
	return ids
}

// NewDeltaStore returns a label store on the Delta backend.
func NewDeltaStore(db txn.DB, tables *DeltaTables) *Store {
	return &Store{delta: &deltaLabels{db: db, DeltaTables: tables}}
}

type deltaLabels struct {
	db txn.DB
	*DeltaTables
}

// set adds the label, or changes the value of the one with its key. (On
// Postgres it always adds one: see resourceKey.)
func (d *deltaLabels) set(ctx context.Context, tx txn.Tx, l *Label) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		for _, row := range d.ofTarget(s.Reader, l) {
			if row.Key != l.Key {
				continue
			}
			if row.Value == l.Value {
				return nil
			}
			row.Value = l.Value
			return d.labels.Put(s.Tx, row)
		}
		id, err := s.Tx.NextID("labels_id_seq")
		if err != nil {
			return err
		}
		return d.labels.Insert(s.Tx, newLabelRow(id, l), "labels_pkey")
	})
}

// deleteKey removes the target's labels with l's key.
func (d *deltaLabels) deleteKey(ctx context.Context, tx txn.Tx, l *Label) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		for _, row := range d.ofTarget(s.Reader, l) {
			if row.Key != l.Key {
				continue
			}
			if _, err := d.labels.Delete(s.Tx, filestore.IntKey(row.ID)); err != nil {
				return err
			}
		}
		return nil
	})
}

// ofTarget returns the labels of l's target, oldest first.
func (d *deltaLabels) ofTarget(r filestore.Reader, l *Label) []labelRow {
	var rows []labelRow
	for _, k := range newLabelRow(0, l).targetKeys() {
		d.byTarget.Ascend(r, k, func(row labelRow) bool {
			rows = append(rows, row)
			return true
		})
	}
	return rows
}

func (d *deltaLabels) findAllByTarget(ctx context.Context, tx txn.Tx, t assignment.Target) ([]Label, error) {
	var labels []Label
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		labels = []Label{} // not nil, as the Postgres store returns
		for _, row := range d.ofTarget(s.Reader, &Label{Target: t}) {
			labels = append(labels, Label{Target: t, Key: row.Key, Value: row.Value})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return labels, nil
}

// uniqueKeys returns every label key once, in order. Like the Postgres query,
// it reads all labels.
func (d *deltaLabels) uniqueKeys(ctx context.Context, tx txn.Tx) ([]string, error) {
	var keys []string
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		keys = nil
		seen := make(map[string]bool)
		d.labels.Ascend(s.Reader, func(row labelRow) bool {
			if !seen[row.Key] {
				seen[row.Key] = true
				keys = append(keys, row.Key)
			}
			return true
		})
		slices.Sort(keys)
		return nil
	})
	return keys, err
}

func (d *deltaLabels) searchKeys(ctx context.Context, opts KeySearchOptions) ([]string, error) {
	keys, err := d.uniqueKeys(ctx, nil)
	if err != nil {
		return nil, err
	}
	return opts.filter(keys), nil
}

// searchValues returns the matching values of one key.
func (d *deltaLabels) searchValues(ctx context.Context, opts ValueSearchOptions) ([]string, error) {
	var values []string
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		values = nil
		seen := make(map[string]bool)
		d.byKey.Ascend(s.Reader, opts.Key, func(row labelRow) bool {
			if !seen[row.Value] {
				seen[row.Value] = true
				values = append(values, row.Value)
			}
			return true
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return opts.filter(values), nil
}

// filter returns the first of the distinct names that the options match, in
// their order, as the search templates do.
func (opts KeySearchOptions) filter(names []string) []string {
	search, after := strings.ToLower(opts.Search), strings.ToLower(opts.After)
	var result []string
	for _, name := range names {
		lower := strings.ToLower(name)
		match := !slices.Contains(opts.Omit, name) && strings.Contains(lower, search)
		if opts.After != "" {
			// AND binds tighter than OR, as in the templates: a name that
			// differs from After only in case ignores the other options.
			match = match && lower > after || lower == after && name > opts.After
		}
		if match {
			result = append(result, name)
		}
	}
	slices.SortFunc(result, func(a, b string) int {
		if c := strings.Compare(strings.ToLower(a), strings.ToLower(b)); c != 0 {
			return c
		}
		return strings.Compare(a, b)
	})
	if len(result) > opts.Limit {
		result = result[:opts.Limit]
	}
	return result
}

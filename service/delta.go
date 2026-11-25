package service

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
)

// serviceRow is a services row on the Delta backend.
type serviceRow struct {
	ID                   string     `parquet:"id"`
	Name                 string     `parquet:"name"`
	Description          string     `parquet:"description"`
	EscalationPolicyID   string     `parquet:"escalation_policy_id"`
	MaintenanceExpiresAt *time.Time `parquet:"maintenance_expires_at,optional,timestamp(microsecond:utc)"`
}

// newServiceRow returns the row of a valid service. IDs are stored in the form
// Postgres gives UUIDs.
func newServiceRow(s *Service) serviceRow {
	r := serviceRow{
		ID:                 uuid.MustParse(s.ID).String(),
		Name:               s.Name,
		Description:        s.Description,
		EscalationPolicyID: uuid.MustParse(s.EscalationPolicyID).String(),
	}
	if !s.MaintenanceExpiresAt.IsZero() {
		r.MaintenanceExpiresAt = &s.MaintenanceExpiresAt
	}
	return r
}

func (r serviceRow) service() Service {
	s := Service{
		ID:                 r.ID,
		Name:               r.Name,
		Description:        r.Description,
		EscalationPolicyID: r.EscalationPolicyID,
	}
	if r.MaintenanceExpiresAt != nil {
		s.MaintenanceExpiresAt = *r.MaintenanceExpiresAt
	}
	return s
}

// DeltaTables are the service store's tables on the Delta backend. Other
// packages declare their foreign keys to Services, and find the services that
// use an escalation policy with ByEP.
type DeltaTables struct {
	Services *filestore.Table[serviceRow]
	ByEP     *filestore.Index[serviceRow]
}

// DefineDeltaTables adds the service store's tables to the Delta backend's
// schema: services, keyed by id (services_pkey).
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	t := &DeltaTables{Services: filestore.NewTable(s, "services", func(r serviceRow) string { return r.ID })}
	t.ByEP = t.Services.Index("services_escalation_policy_id", func(r serviceRow) []string { return []string{r.EscalationPolicyID} })
	// The exact name first: Postgres checks its older index first.
	t.Services.Unique("services_name_key", func(r serviceRow) (string, bool) { return r.Name, true })
	t.Services.Unique("services_name", func(r serviceRow) (string, bool) { return strings.ToLower(r.Name), true })
	t.Services.Unique("svc_ep_uniq", func(r serviceRow) (string, bool) { return r.ID + "/" + r.EscalationPolicyID, true })
	return t
}

// DeclareDeltaForeignKeys declares this package's foreign keys to other
// packages' tables.
func DeclareDeltaForeignKeys[EP any](t *DeltaTables, policies *filestore.Table[EP]) {
	filestore.References(t.Services, "services_escalation_policy_id_fkey", policies,
		func(r serviceRow) (string, bool) { return r.EscalationPolicyID, true }, filestore.Restrict, nil)
}

// DeltaLookups are the reads of other packages' tables, and the trigger that
// changes one, that the store needs. They run in the store's transaction, so
// they must do no I/O.
type DeltaLookups struct {
	// EscalationPolicyName is the join with escalation_policies. Without it,
	// the methods that return policy names return deltatxn.ErrNotPorted.
	EscalationPolicyName func(r filestore.Reader, id string) (name string, ok bool)

	// FavoriteServices returns the IDs of the services that userID has as
	// favorites (user_favorites). Without it, no service is a favorite.
	FavoriteServices func(r filestore.Reader, userID string) []string

	// IntegrationKeyService returns the service of an integration key
	// (integration_keys), and ServicesWithLabel the IDs of the services that
	// have a label (labels) with the key and value; an empty value means any.
	// A search that needs one that is absent returns deltatxn.ErrNotPorted.
	IntegrationKeyService func(r filestore.Reader, keyID string) (serviceID string, ok bool)
	ServicesWithLabel     func(r filestore.Reader, key, value string) []string

	// ClearEscalationPolicyState is trg_10_clear_ep_state_on_svc_ep_change: it
	// resets the service's rows in escalation_policy_state to the start of
	// its new policy. Without it, changing a service's policy returns
	// deltatxn.ErrNotPorted.
	ClearEscalationPolicyState func(tx *filestore.Tx, serviceID, newPolicyID string) error
}

// NewDeltaStore returns a service store on the Delta backend.
func NewDeltaStore(db txn.DB, tables *DeltaTables, lookups DeltaLookups) *Store {
	if lookups.FavoriteServices == nil {
		lookups.FavoriteServices = func(filestore.Reader, string) []string { return nil }
	}
	return &Store{delta: &deltaServices{db: db, DeltaTables: tables, DeltaLookups: lookups}}
}

type deltaServices struct {
	db txn.DB
	*DeltaTables
	DeltaLookups
}

// favorites returns the user's favorite services; no user has none.
func (d *deltaServices) favorites(r filestore.Reader, userID uuid.NullUUID) map[string]bool {
	if !userID.Valid {
		return nil
	}
	ids := d.FavoriteServices(r, userID.UUID.String())
	favs := make(map[string]bool, len(ids))
	for _, id := range ids {
		favs[id] = true
	}
	return favs
}

func (d *deltaServices) insert(ctx context.Context, tx txn.Tx, svc *Service) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		return d.Services.Insert(s.Tx, newServiceRow(svc), "services_pkey")
	})
}

func (d *deltaServices) update(ctx context.Context, tx txn.Tx, svc *Service) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row := newServiceRow(svc) // an UPDATE of every column but the id
		old, ok := d.Services.Get(s.Reader, row.ID)
		if !ok {
			return nil // an UPDATE of no rows
		}
		newPolicy := old.EscalationPolicyID != row.EscalationPolicyID
		if newPolicy && d.ClearEscalationPolicyState == nil {
			return deltatxn.ErrNotPorted
		}
		if err := d.Services.Put(s.Tx, row); err != nil {
			return err
		}
		if newPolicy {
			return d.ClearEscalationPolicyState(s.Tx, row.ID, row.EscalationPolicyID)
		}
		return nil
	})
}

func (d *deltaServices) delete(ctx context.Context, tx txn.Tx, ids []uuid.UUID) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		for _, id := range ids {
			if _, err := d.Services.Delete(s.Tx, id.String()); err != nil {
				return err
			}
		}
		return nil
	})
}

// findOneForUpdate returns sql.ErrNoRows for a missing service, and neither
// the policy's name nor the favorite flag, like the Postgres query. Unlike it,
// it returns when maintenance ends: callers change the service they get and
// pass it to UpdateTx, and would otherwise end maintenance with any change.
//
// In a transaction, no other change can come between this and the
// transaction's own, as with FOR UPDATE (F4).
func (d *deltaServices) findOneForUpdate(ctx context.Context, tx txn.Tx, id uuid.UUID) (*Service, error) {
	var svc Service
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.Services.Get(s.Reader, id.String())
		if !ok {
			return sql.ErrNoRows
		}
		svc = row.service()
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &svc, nil
}

// find returns those of the services that exist, each once, with the names of
// their policies and whether userID has them as favorites.
func (d *deltaServices) find(ctx context.Context, userID uuid.NullUUID, ids []uuid.UUID) ([]Service, error) {
	if d.EscalationPolicyName == nil {
		return nil, deltatxn.ErrNotPorted
	}
	var services []Service
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		services = nil // as scanAllFrom returns without rows
		favs := d.favorites(s.Reader, userID)
		seen := make(map[uuid.UUID]bool, len(ids))
		for _, id := range ids {
			if seen[id] {
				continue // like `s.id = any($1)`
			}
			seen[id] = true
			row, ok := d.Services.Get(s.Reader, id.String())
			if !ok {
				continue
			}
			svc := row.service()
			if svc.epName, ok = d.EscalationPolicyName(s.Reader, row.EscalationPolicyID); !ok {
				continue // like JOIN
			}
			svc.isUserFavorite = favs[row.ID]
			services = append(services, svc)
		}
		return nil
	})
	return services, err
}

// findOne returns sql.ErrNoRows for a missing service.
func (d *deltaServices) findOne(ctx context.Context, userID uuid.NullUUID, id uuid.UUID) (*Service, error) {
	services, err := d.find(ctx, userID, []uuid.UUID{id})
	if err != nil {
		return nil, err
	}
	if len(services) == 0 {
		return nil, sql.ErrNoRows
	}
	return &services[0], nil
}

func (d *deltaServices) findAllByEP(ctx context.Context, epID string) ([]Service, error) {
	if d.EscalationPolicyName == nil {
		return nil, deltatxn.ErrNotPorted
	}
	id, err := uuid.Parse(epID)
	if err != nil {
		return nil, err // the Postgres query fails too
	}
	var services []Service
	err = deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		services = nil // as scanAllFrom returns without rows
		epName, ok := d.EscalationPolicyName(s.Reader, id.String())
		if !ok {
			return nil // like JOIN
		}
		d.ByEP.Ascend(s.Reader, id.String(), func(row serviceRow) bool {
			svc := row.service()
			svc.epName = epName
			services = append(services, svc)
			return true
		})
		return nil
	})
	return services, err
}

// parseIDs parses validated UUIDs.
func parseIDs(ids []string) []uuid.UUID {
	parsed := make([]uuid.UUID, len(ids))
	for i, id := range ids {
		parsed[i] = uuid.MustParse(id)
	}
	return parsed
}

// parseUserID parses a validated user ID; an empty one is no user.
func parseUserID(id string) uuid.NullUUID {
	if id == "" {
		return uuid.NullUUID{}
	}
	return uuid.NullUUID{UUID: uuid.MustParse(id), Valid: true}
}

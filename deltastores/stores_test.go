package deltastores

import (
	"context"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/config"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/keyring"
	"github.com/target/goalert/notification/nfydest"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/user"
	"github.com/target/goalert/util/log"
)

type inventorySection struct {
	name string
	fks  []filestore.ForeignKey
}

var foreignKeyLine = regexp.MustCompile(`^- ([a-zA-Z0-9_]+): .* REFERENCES ([a-zA-Z0-9_]+)\(`)

// portedInventory reads the inventory's own declaration of which tables have
// moved. Keeping this check beside NewSchema makes a missed package obvious.
func portedInventory(t *testing.T) []inventorySection {
	t.Helper()
	data, err := os.ReadFile("../docs/filestore-inventory.md")
	require.NoError(t, err)
	var result []inventorySection
	for _, section := range strings.Split(string(data), "\n### ") {
		if !strings.Contains(section, "**Go:** ported") && !strings.Contains(section, "**Go:** defined") {
			continue
		}
		name, _, _ := strings.Cut(section, "\n")
		sec := inventorySection{name: name}
		for _, line := range strings.Split(section, "\n") {
			m := foreignKeyLine.FindStringSubmatch(line)
			if m == nil {
				continue
			}
			fk := filestore.ForeignKey{Name: m[1], Child: sec.name, Parent: m[2]}
			switch {
			case strings.Contains(line, "ON DELETE CASCADE"):
				fk.OnDelete = filestore.Cascade
			case strings.Contains(line, "ON DELETE SET NULL"):
				fk.OnDelete = filestore.SetNull
			}
			sec.fks = append(sec.fks, fk)
		}
		result = append(result, sec)
	}
	require.NotEmpty(t, result)
	return result
}

func TestNewSchemaMatchesInventory(t *testing.T) {
	schema, _ := NewSchema()
	tables := make(map[string]bool)
	for _, name := range schema.TableNames() {
		tables[name] = true
	}
	foreignKeys := make(map[string]filestore.ForeignKey)
	for _, fk := range schema.ForeignKeys() {
		foreignKeys[fk.Name] = fk
	}
	for _, sec := range portedInventory(t) {
		t.Run(sec.name, func(t *testing.T) {
			assert.True(t, tables[sec.name], "table is not defined")
			for _, want := range sec.fks {
				if want.Name == "svc_ep_fkey" {
					// A composite key, checked by escalation.InsertState/UpdateState
					// rather than filestore.References.
					continue
				}
				if want.Name == "rotation_participants_user_id_fkey" {
					// Deliberately Restrict, as the inventory notes: the user
					// store removes participants first to keep rotations valid.
					want.OnDelete = filestore.Restrict
				}
				got, ok := foreignKeys[want.Name]
				if assert.True(t, ok, "foreign key %s is not declared", want.Name) {
					assert.Equal(t, want, got)
				}
			}
		})
	}
}

func TestNewStores(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	objects := objstore.NewMem()
	opts := filestore.DeltaOptions{PollInterval: -1, Logf: func(string, ...any) {}}
	schema, _ := NewSchema()
	require.NoError(t, filestore.CreateDelta(ctx, objects, schema, opts))
	schema, tables := NewSchema()
	delta, err := filestore.OpenDelta(ctx, objects, schema, opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = delta.Close() })

	stores, err := New(ctx, delta, tables, Options{
		Logger:   log.NewLogger(),
		Registry: nfydest.NewRegistry(),
		Config:   config.StoreConfig{Keys: keyring.Keys{[]byte("test key")}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = stores.Close(context.Background()) })

	u, err := stores.User.Insert(ctx, &user.User{Name: "Test User", Email: "test@example.com", Role: permission.RoleUser})
	require.NoError(t, err)
	got, err := stores.User.FindOne(ctx, u.ID)
	require.NoError(t, err)
	assert.Equal(t, u.Name, got.Name)
	assert.Equal(t, u.Email, got.Email)
}

package deltastores_test

import (
	"os"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/deltastores"
	"github.com/target/goalert/deltastores/deltastorestest"
	"github.com/target/goalert/filestore"
)

// inventoryTable is a Postgres table as docs/filestore-inventory.md has it.
type inventoryTable struct {
	name, status string
	foreign      []filestore.ForeignKey
	uniques      map[string]string // the columns of each
}

func readInventory(t *testing.T) []inventoryTable {
	t.Helper()
	data, err := os.ReadFile("../docs/filestore-inventory.md")
	require.NoError(t, err)
	foreignRx := regexp.MustCompile(`(?m)^- (\w+): .* REFERENCES (\w+)\(.*$`)
	uniqueRx := regexp.MustCompile("(?m)^- (\\w+)(?: \\(index\\))?: `(\\([^`]*)`$")
	statusRx := regexp.MustCompile(`(?m)^\*\*Go:\*\* (.*)$`)
	var out []inventoryTable
	for _, section := range strings.Split(string(data), "\n### ")[1:] {
		name, rest, _ := strings.Cut(section, "\n")
		table := inventoryTable{name: name, uniques: map[string]string{}}
		m := statusRx.FindStringSubmatch(rest)
		require.NotNil(t, m, name)
		table.status = m[1]
		for _, m := range foreignRx.FindAllStringSubmatch(rest, -1) {
			fk := filestore.ForeignKey{Name: m[1], Child: name, Parent: m[2], OnDelete: filestore.Restrict}
			switch {
			case strings.Contains(m[0], "ON DELETE CASCADE"):
				fk.OnDelete = filestore.Cascade
			case strings.Contains(m[0], "ON DELETE SET NULL"):
				fk.OnDelete = filestore.SetNull
			}
			table.foreign = append(table.foreign, fk)
		}
		if _, list, ok := strings.Cut(rest, "Unique:\n\n"); ok {
			list, _, _ = strings.Cut(list, "\n\n")
			for _, m := range uniqueRx.FindAllStringSubmatch(list, -1) {
				if !strings.HasSuffix(m[1], "_pkey") { // the table's key
					table.uniques[m[1]] = m[2]
				}
			}
		}
		out = append(out, table)
	}
	require.Greater(t, len(out), 60)
	return out
}

// TestSchema checks that the schema has every table, foreign key and unique
// constraint that Postgres's had, or that the inventory says why not (F7, F8,
// F51).
func TestSchema(t *testing.T) {
	schema, tables := deltastores.NewSchema()
	for i, v := 0, reflect.ValueOf(tables).Elem(); i < v.NumField(); i++ {
		assert.False(t, v.Field(i).IsNil(), v.Type().Field(i).Name)
	}
	names := schema.TableNames()
	foreign := map[string]filestore.ForeignKey{}
	for _, fk := range schema.ForeignKeys() {
		foreign[fk.Name] = fk
	}

	// One index keeps what were five: a favorite has one target.
	otherwise := map[string]string{}
	for _, target := range []string{"escalation_policy", "rotation", "schedule", "service", "user"} {
		otherwise["user_favorites_user_id_tgt_"+target+"_id_key"] = "user_favorites_user_id_tgt_key"
	}
	known := []string{"engine_lease", "engine_state"} // new
	for _, table := range readInventory(t) {
		if strings.HasPrefix(table.status, "gone") {
			assert.NotContains(t, names, table.name)
			continue
		}
		known = append(known, table.name)
		if !assert.Contains(t, names, table.name) {
			continue
		}
		for _, want := range table.foreign {
			switch want.Name {
			case "svc_ep_fkey":
				// Checked, and cascaded, by escalation.DeltaTables.
				assert.NotContains(t, foreign, want.Name)
				continue
			case "rotation_participants_user_id_fkey":
				// A cascade would skip what else has to happen to the rotation.
				want.OnDelete = filestore.Restrict
			}
			assert.Equal(t, want, foreign[want.Name], want.Name)
			delete(foreign, want.Name)
		}
		have := schema.UniqueNames(table.name)
		for name, columns := range table.uniques {
			switch {
			case columns == "(id)":
				// From a sequence, or a uuid that is the table's key.
			case otherwise[name] != "":
				assert.Contains(t, have, otherwise[name], name)
			default:
				assert.Contains(t, have, name, "%s %s", table.name, columns)
			}
		}
	}
	assert.ElementsMatch(t, known, names)
	assert.Empty(t, foreign, "foreign keys that Postgres did not have")
}

// TestNothingMissing checks that no store lacks a lookup of another's table
// (F51).
func TestNothingMissing(t *testing.T) {
	p := deltastorestest.New(t).Pod(t)
	// What these were for, the engine finds in the data (F52).
	absent := []string{"alert.DeltaLookups.StatusChanged", "integrationkey.DeltaLookups.NewSignal"}
	seen := map[uintptr]bool{}
	checked := 0
	var walk func(v reflect.Value, path string)
	walk = func(v reflect.Value, path string) {
		switch v.Kind() {
		case reflect.Pointer:
			if v.IsNil() || seen[v.Pointer()] || !strings.HasPrefix(v.Type().Elem().PkgPath(), "github.com/target/goalert/") {
				return
			}
			seen[v.Pointer()] = true
			walk(v.Elem(), path)
		case reflect.Struct:
			if !strings.HasPrefix(v.Type().PkgPath(), "github.com/target/goalert/") {
				return
			}
			for i := range v.NumField() {
				f, field := v.Field(i), v.Type().Field(i)
				if f.Kind() != reflect.Func {
					walk(f, path+"."+field.Name)
					continue
				}
				checked++
				if slices.Contains(absent, v.Type().String()+"."+field.Name) {
					assert.True(t, f.IsNil(), "%s.%s is there after all", path, field.Name)
				} else {
					assert.False(t, f.IsNil(), "%s.%s", path, field.Name)
				}
			}
		}
	}
	v := reflect.ValueOf(p.Stores).Elem()
	for i := range v.NumField() {
		f, name := v.Field(i), v.Type().Field(i).Name
		if f.Kind() != reflect.Pointer || f.Type().Elem().Name() != "Store" {
			continue
		}
		require.False(t, f.IsNil(), name)
		if delta := f.Elem().FieldByName("delta"); delta.IsValid() {
			require.False(t, delta.IsNil(), name)
			walk(delta, name)
		} else {
			require.Equal(t, "TimeZone", name, "it has no Delta implementation")
		}
	}
	assert.Greater(t, checked, 60)
}

package filestore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSchemaIntrospection(t *testing.T) {
	type parent struct{ ID string }
	type child struct {
		ID, Parent string
	}
	s := NewSchema()
	parents := NewTable(s, "parents", func(p parent) string { return p.ID })
	children := NewTable(s, "children", func(c child) string { return c.ID })
	children.Unique("children_parent", func(c child) (string, bool) { return c.Parent, true })
	References(children, "children_parent_fkey", parents,
		func(c child) (string, bool) { return c.Parent, true }, Cascade, nil)

	assert.Equal(t, []string{"children", "parents"}, s.TableNames())
	assert.Equal(t, []string{"children_parent"}, s.UniqueNames("children"))
	assert.Nil(t, s.UniqueNames("missing"))
	assert.Equal(t, []ForeignKey{{Name: "children_parent_fkey", Child: "children", Parent: "parents", OnDelete: Cascade}}, s.ForeignKeys())
}

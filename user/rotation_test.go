package user

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestActiveIndexWithout checks that deleting a user doesn't change who is on
// call, unless it was them (F5).
func TestActiveIndexWithout(t *testing.T) {
	check := func(participants string, active, want int) {
		t.Helper()
		var ids []string
		for _, p := range participants {
			ids = append(ids, string(p))
		}
		assert.Equal(t, want, activeIndexWithout(ids, active, "U"), "%s with %d active", participants, active)
	}

	check("AUB", 0, 0) // A
	check("AUB", 1, 1) // B, after U
	check("AUB", 2, 1) // B
	check("UA", 0, 0)
	check("UA", 1, 0)
	check("AU", 1, 0) // the first, after the last
	check("ABU", 2, 0)
	check("AUU", 2, 0)
	check("AUU", 1, 0)
	check("AB", 1, 1) // not in it

	// More than once, before the active one: A was skipped, and C paged.
	check("UBUAC", 3, 1)
	check("UUA", 2, 0)
	check("UUUAB", 3, 0)

	check("U", 0, 0) // nobody is left
	check("UU", 1, 0)
}

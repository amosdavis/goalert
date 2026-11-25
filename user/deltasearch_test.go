package user

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
)

func TestDeltaSearchPages(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaTestEnv(t, nil)
	s, db := e.pod(t, "a")
	require.NoError(t, db.Run(ctx, nil, func(tx txn.Tx) error {
		for i := range 40 {
			if _, err := s.InsertTx(ctx, tx, &User{Name: fmt.Sprintf("User %02d", i), Role: permission.RoleUser}); err != nil {
				return err
			}
		}
		return nil
	}))

	var got []string
	var opts SearchOptions
	for {
		page, err := s.Search(ctx, &opts)
		require.NoError(t, err)
		if len(page) == 0 {
			break
		}
		assert.LessOrEqual(t, len(page), 15, "15 by default")
		for _, u := range page {
			got = append(got, u.Name)
		}
		opts.After.Name = got[len(got)-1]
	}
	assert.Len(t, got, 40, "every user, once")
	assert.True(t, slices.IsSorted(got))

	// The cursor holds only a name, so users with the same one as the last of
	// a page are skipped, as on Postgres.
	same := []string{addDeltaUser(t, s, "Zed", ""), addDeltaUser(t, s, "zed", ""), addDeltaUser(t, s, "ZED", "")}
	slices.Sort(same)
	users, err := s.Search(ctx, &SearchOptions{Search: "zed"})
	require.NoError(t, err)
	assert.Equal(t, same, userIDs(users), "one name whatever its case, so ordered by id")
	users, err = s.Search(ctx, &SearchOptions{Search: "zed", After: SearchCursor{Name: "zeD"}})
	require.NoError(t, err)
	assert.Nil(t, users)
}

func TestNameMatcher(t *testing.T) {
	for _, c := range []struct {
		search, name string
		want         bool
	}{
		{"", "Alice", true},
		{"ali", "Alice", true},
		{"LIC", "alice", true}, // contains, in any case
		{"al sm", "Alice Smith", true},
		{"sm al", "Alice Smith", false},   // not in that order
		{"al mith", "Alice Smith", false}, // not the start of a word
		{"a s", "Alice B. Smith", true},   // words may be between
		{"al al", "Alice", false},         // one word is not two
		{"al al", "Alice Alvarez", true},
		{"alsm", "Alice Smith", false},
		{"al-sm", "Alice Smith", true}, // terms are letters and digits
		{"al_sm", "Alice Smith", true},
		{"al smi", "alice_smith", true}, // "_" and "-" separate words
		{"al smi", "alice-smith", true},
		{"br", "Pat O'Brien", true},
		{"100%", "100 percent", true}, // the term 100
		{"0%", "100% sure", true},     // "%" stands for itself
		{"0%", "100 sure", false},
		{"a_c", "abc", false}, // and so does "_"
		{"a_c", "xa_cx", true},
		{`a\c`, `xa\cx`, true},
		{"--", "Alice", true}, // no terms: any word will do
		{"--", "!!", false},
		{"ÉMI", "Émilie Zola", true},
		{"mi zo", "Émilie Zola", false}, // É is a letter, so mi starts no word
		{"2 b", "User2 A B", false},     // a digit doesn't start a word after a letter either
		{"2 b", "User2 B", true},        // contains
		{"user2 b", "User2 A B", true},
	} {
		assert.Equal(t, c.want, nameMatcher(c.search)(c.name), "%q in %q", c.search, c.name)
	}
}

func TestDeltaSearchEmail(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaTestEnv(t, nil)
	s, db := e.pod(t, "a")
	alice := addDeltaUser(t, s, "Alice", "same@example.com")
	bob := addDeltaUser(t, s, "Bob", "same@example.com")
	addDeltaUser(t, s, "Carol", "")
	odd := addDeltaUser(t, s, "email= is in my name", "odd@example.com")

	// Too long to look up by, but not to store.
	long := strings.Repeat("x", 5000) + "@example.com"
	require.NoError(t, inTx(db, func(tx *filestore.Tx) error { return e.tables.UpdateProfile(tx, odd, "", long) }))
	u, err := s.FindOne(ctx, odd)
	require.NoError(t, err)
	assert.Equal(t, long, u.Email)

	for _, c := range []struct {
		opts SearchOptions
		want []string
	}{
		{SearchOptions{Search: "email=SAME@example.com"}, []string{alice, bob}},
		{SearchOptions{Search: "email=same@example.com", Omit: []string{alice}}, []string{bob}},
		{SearchOptions{Search: "email=same@example.com", Only: []string{alice}}, []string{alice}},
		{SearchOptions{Search: "email=same"}, []string{}}, // only a whole address
		{SearchOptions{Search: "email=odd@example.com"}, []string{}},
		{SearchOptions{Search: "email="}, []string{odd}}, // no address, so a name
	} {
		users, err := s.Search(ctx, &c.opts)
		require.NoError(t, err)
		assert.Equal(t, c.want, userIDs(users), "%+v", c.opts)
	}
}

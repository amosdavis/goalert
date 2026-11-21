package objstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
)

// SelfTestPrefix holds the objects SelfTest writes. Delta readers never see
// them, since no commit refers to them.
const SelfTestPrefix = "_goalert_selftest/"

// SelfTest checks, with the credentials the store has, that it does what
// GoAlert relies on (docs/filestore-failure-modes.md, F33): it creates an
// object if absent, sees a second create of it refused, reads and lists it,
// replaces another object, and deletes both. Run it before trusting a store,
// such as at startup. An error matching ErrDenied means the credentials lack
// a permission.
func SelfTest(ctx context.Context, s Store) error {
	id := make([]byte, 8)
	if _, err := rand.Read(id); err != nil {
		return err
	}
	name := SelfTestPrefix + hex.EncodeToString(id)
	other := name + "-replace"
	fail := func(step string, err error) error {
		// Best effort: the objects are tiny and ignored if left behind.
		_ = s.Delete(ctx, name)
		_ = s.Delete(ctx, other)
		return fmt.Errorf("objstore: self-test: %s: %w", step, err)
	}

	data := []byte("goalert self-test " + name)
	if err := s.PutIfAbsent(ctx, name, data); err != nil {
		return fail("create", err)
	}
	switch err := s.PutIfAbsent(ctx, name, []byte("second")); {
	case err == nil:
		return fail("create if absent", errors.New("a second create of the same object succeeded, so this store does not enforce create-if-absent and two pods could commit the same version"))
	case !errors.Is(err, ErrExists):
		return fail("create if absent", err)
	}
	got, err := s.Get(ctx, name)
	if err != nil {
		return fail("read", err)
	}
	if !bytes.Equal(got, data) {
		return fail("read", errors.New("the object changed after a refused create"))
	}
	names, err := s.List(ctx, SelfTestPrefix, "")
	if err != nil {
		return fail("list", err)
	}
	if !slices.Contains(names, name) {
		return fail("list", errors.New("a new object is missing from the listing"))
	}
	for _, v := range []string{"1", "2"} {
		if err := s.Put(ctx, other, []byte(v)); err != nil {
			return fail("replace", err)
		}
	}
	if got, err := s.Get(ctx, other); err != nil || string(got) != "2" {
		return fail("replace", fmt.Errorf("read back %q, %v", got, err))
	}
	for _, n := range []string{name, other} {
		if err := s.Delete(ctx, n); err != nil {
			return fail("delete", err)
		}
		if _, err := s.Get(ctx, n); !errors.Is(err, ErrNotFound) {
			return fail("delete", fmt.Errorf("a deleted object can still be read: %v", err))
		}
	}
	return nil
}

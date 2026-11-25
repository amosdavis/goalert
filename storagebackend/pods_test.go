package storagebackend_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/storagebackend"
	"github.com/target/goalert/storagebackend/backendtest"
)

// TestDeltaBackends runs the whole-backend suite on Delta in memory and in a
// directory.
func TestDeltaBackends(t *testing.T) {
	for name, newStore := range map[string]func(t *testing.T) objstore.Store{
		"Mem": func(t *testing.T) objstore.Store { return objstore.NewMem() },
		"Dir": func(t *testing.T) objstore.Store {
			s, err := objstore.NewDir(t.TempDir())
			require.NoError(t, err)
			return s
		},
	} {
		t.Run(name, func(t *testing.T) {
			backendtest.Run(t, func(t *testing.T) func(string) *storagebackend.Backend {
				store := newStore(t)
				return func(writer string) *storagebackend.Backend {
					return &storagebackend.Backend{Store: store, Log: deltalog.New(store, writer)}
				}
			})
		})
	}
}

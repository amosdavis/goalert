package chunkstore_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/objstore/chunkstore"
	"github.com/target/goalert/objstore/chunkstore/chunkstoretest"
	"github.com/target/goalert/objstore/objstoretest"
	"github.com/target/goalert/storagebackend"
	"github.com/target/goalert/storagebackend/backendtest"
	"github.com/target/goalert/tablelog"
	"github.com/target/goalert/tablelog/tablelogtest"
)

// TestConformance runs every suite an adapter must pass on the in-memory
// records, so that a failure of an adapter is the adapter's.
func TestConformance(t *testing.T) {
	newRecords := func(t *testing.T) chunkstore.RecordStore { return chunkstore.NewMemRecords() }
	t.Run("Records", func(t *testing.T) { chunkstoretest.Run(t, newRecords) })

	for name, opts := range map[string]chunkstore.Options{
		"Default":     {},
		"TinyChunks":  {ChunkSize: 1 << 10, InlineSize: -1},
		"InlineLarge": {InlineSize: chunkstore.MaxRecord},
	} {
		newStore := func(t *testing.T) objstore.Store {
			s, err := chunkstore.New(newRecords(t), opts)
			require.NoError(t, err)
			return s
		}
		t.Run(name, func(t *testing.T) {
			t.Run("Objects", func(t *testing.T) { objstoretest.Run(t, newStore) })
			t.Run("Log", func(t *testing.T) {
				tablelogtest.Run(t, newStore, func(t *testing.T, store objstore.Store, writerID string) tablelog.Log {
					return deltalog.New(store, writerID)
				})
			})
			t.Run("Backend", func(t *testing.T) {
				backendtest.Run(t, func(t *testing.T) func(string) *storagebackend.Backend {
					store := newStore(t)
					return func(writer string) *storagebackend.Backend {
						return &storagebackend.Backend{Store: store, Log: deltalog.New(store, writer)}
					}
				})
			})
		})
	}
}

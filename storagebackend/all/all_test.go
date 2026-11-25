//go:build !goalert_no_gcs && !goalert_no_azure && !goalert_no_cosmosdb && !goalert_no_firestore && !goalert_no_iceberg

package all

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/target/goalert/storagebackend"
)

// TestDrivers checks what a full build has.
func TestDrivers(t *testing.T) {
	objects, logs := storagebackend.Drivers()
	assert.Equal(t, []string{"az", "cosmos", "file", "firestore", "gs"}, objects)
	assert.Equal(t, []string{"iceberg+http", "iceberg+https"}, logs)
}

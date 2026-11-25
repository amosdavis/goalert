package deltalog_test

import (
	"testing"

	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/tablelog"
	"github.com/target/goalert/tablelog/tablelogtest"
)

func TestTableLogConformance(t *testing.T) {
	tablelogtest.Run(t,
		func(t *testing.T) objstore.Store { return objstore.NewMem() },
		func(t *testing.T, store objstore.Store, writerID string) tablelog.Log {
			return deltalog.New(store, writerID)
		},
	)
}

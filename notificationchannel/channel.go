package notificationchannel

import (
	"github.com/google/uuid"
	"github.com/target/goalert/gadb"
)

type Channel struct {
	ID   uuid.UUID
	Name string
	Dest gadb.DestV1
}

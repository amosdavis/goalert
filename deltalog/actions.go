// Package deltalog reads and writes a Delta Lake transaction log
// (https://github.com/delta-io/delta/blob/master/PROTOCOL.md) on an
// objstore.Store.
//
// Only the features of reader version 1 and writer version 2 are used, so
// any Delta reader can open GoAlert's table (docs/filestore-failure-modes.md,
// F19, F35). A commit is the create-if-absent write of the next
// _delta_log/<version>.json, which only one writer can win (F4).
package deltalog

// Action is one line of a commit file. Exactly one field is set.
type Action struct {
	Protocol   *Protocol   `json:"protocol,omitempty"`
	MetaData   *MetaData   `json:"metaData,omitempty"`
	Add        *Add        `json:"add,omitempty"`
	Remove     *Remove     `json:"remove,omitempty"`
	Txn        *Txn        `json:"txn,omitempty"`
	CommitInfo *CommitInfo `json:"commitInfo,omitempty"`
}

// Protocol is the protocol versions a table needs.
type Protocol struct {
	MinReaderVersion int `json:"minReaderVersion"`
	MinWriterVersion int `json:"minWriterVersion"`
}

// The protocol versions GoAlert writes and reads.
const (
	ReaderVersion = 1
	WriterVersion = 2
)

// Format names the data file format.
type Format struct {
	Provider string            `json:"provider"`
	Options  map[string]string `json:"options"`
}

// MetaData describes the table.
type MetaData struct {
	ID               string            `json:"id"`
	Name             string            `json:"name,omitempty"`
	Description      string            `json:"description,omitempty"`
	Format           Format            `json:"format"`
	SchemaString     string            `json:"schemaString"`
	PartitionColumns []string          `json:"partitionColumns"`
	Configuration    map[string]string `json:"configuration"`
	CreatedTime      int64             `json:"createdTime,omitempty"`
}

// Add adds a data file to the table.
type Add struct {
	Path             string            `json:"path"`
	PartitionValues  map[string]string `json:"partitionValues"`
	Size             int64             `json:"size"`
	ModificationTime int64             `json:"modificationTime"`
	DataChange       bool              `json:"dataChange"`
	Stats            string            `json:"stats,omitempty"`
}

// Remove removes a data file from the table. The file stays in storage until
// it is vacuumed after the retention window (F6).
type Remove struct {
	Path                 string            `json:"path"`
	DeletionTimestamp    int64             `json:"deletionTimestamp,omitempty"`
	DataChange           bool              `json:"dataChange"`
	ExtendedFileMetadata bool              `json:"extendedFileMetadata,omitempty"`
	PartitionValues      map[string]string `json:"partitionValues,omitempty"`
	Size                 int64             `json:"size,omitempty"`
}

// Txn records an application's transaction version, for idempotent writes.
type Txn struct {
	AppID       string `json:"appId"`
	Version     int64  `json:"version"`
	LastUpdated int64  `json:"lastUpdated,omitempty"`
}

// CommitInfo is free-form in the protocol. GoAlert's fields identify every
// commit's writer, store and format, so a pod can refuse commits it did not
// expect (F11, F30) and recognize its own commit after an unclear result
// (F32). Other readers ignore them.
type CommitInfo struct {
	Timestamp  int64  `json:"timestamp"`
	Operation  string `json:"operation"`
	EngineInfo string `json:"engineInfo,omitempty"`

	GoAlertStoreID  string `json:"goalertStoreId,omitempty"`
	GoAlertWriterID string `json:"goalertWriterId,omitempty"`
	GoAlertCommitID string `json:"goalertCommitId,omitempty"`
	GoAlertFormat   int    `json:"goalertFormat,omitempty"`
	// GoAlertTime is the commit's time in microseconds since the epoch. The
	// next commit's time is never earlier, whatever the writer's clock says
	// (F13).
	GoAlertTime int64 `json:"goalertTime,omitempty"`
}

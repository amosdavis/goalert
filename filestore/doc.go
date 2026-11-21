// Package filestore is GoAlert's file-backed storage engine. All data is held
// in memory, and every change is appended to a log on disk and fsynced before
// it is acknowledged.
//
// The design follows docs/filestore-failure-modes.md:
//
//   - One writer. Update runs transactions one at a time, so a change is never
//     lost to a concurrent read-modify-write (F4). Readers get an immutable
//     snapshot and never see a half-applied change (F10).
//   - A transaction's changes become a single log record, which is fsynced
//     before Update returns (F1). On recovery, only a record that is
//     incomplete and not followed by any valid record (an unacknowledged
//     write cut short by a crash) is dropped. Any other damage stops startup
//     (F6).
//   - Log records hold the resulting rows, not the operations that produced
//     them, so replay never re-runs business logic and always rebuilds the
//     same state (F25). Each row is stored exactly as it decodes from its
//     encoded form, so the live state and a replayed state are identical.
//   - The data directory is locked so that only one process can open it, and
//     network filesystems are refused (F11).
//   - Every file starts with a format version and the store's ID. Unknown
//     versions, foreign files and unknown tables stop startup instead of
//     being guessed at (F19, F30).
//   - After any write or fsync error, the store refuses further writes until
//     it is reopened, and Health reports the error (F21).
//   - Files are created readable only by the owner (F20).
//
// Snapshots bound recovery time and log size (F16). A snapshot is written
// from an immutable view, so writers keep going while it runs. The newest two
// snapshots are kept, along with the log needed to replay from the older one.
//
// OpenDelta keeps the same tables in a Delta table on object storage instead,
// shared by several pods (delta.go). Each pod still holds the whole state in
// memory. A transaction becomes one Delta commit, made on top of the latest
// version, so a pod that loses a race to another catches up and runs the
// transaction again (F4). The local log is superseded by it.
package filestore

package chunkstore

// NewMemRecords returns an in-memory RecordStore for the external tests.
func NewMemRecords() RecordStore { return newMemRecords() }

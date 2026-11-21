//go:build !linux && !windows && !darwin && !freebsd

package filestore

// checkLocalFS cannot detect network filesystems on this platform. Keeping
// the data directory on a local disk is the operator's responsibility here
// (F11).
func checkLocalFS(string) error { return nil }

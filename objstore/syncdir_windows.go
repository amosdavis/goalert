//go:build windows

package objstore

// syncDir is a no-op on Windows: a directory cannot be fsynced there, and
// NTFS journals directory changes itself. Each file is still flushed before
// it is linked or renamed into place.
func syncDir(string) error { return nil }

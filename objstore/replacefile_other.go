//go:build !windows

package objstore

import "os"

// replaceFile atomically renames a file over another, whoever has it open.
func replaceFile(from, to string) error { return os.Rename(from, to) }

//go:build !windows

package objstore

import "os"

func readWholeFile(name string) ([]byte, error) { return os.ReadFile(name) }

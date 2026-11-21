//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly || windows)

package filestore

import (
	"errors"
	"io"
)

var errUnsupported = errors.New("filestore: unsupported platform")

func syncDir(string) error               { return errUnsupported }
func lockFile(string) (io.Closer, error) { return nil, errUnsupported }
func restrictDir(string) error           { return errUnsupported }

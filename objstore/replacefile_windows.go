//go:build windows

package objstore

import (
	"errors"
	"os"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// replaceFile atomically renames a file over another. os.Rename can't replace
// a file a reader has open, so this asks for POSIX semantics: the name moves
// to the new file at once, and readers of the old one finish reading it. They
// must share delete access, as readWholeFile does.
//
// A program that doesn't, such as a virus scanner, blocks the rename while it
// has the file open, so that is retried for a while.
func replaceFile(from, to string) error {
	deadline := time.Now().Add(2 * time.Second)
	for delay := time.Millisecond; ; delay = min(2*delay, 50*time.Millisecond) {
		err := posixRename(from, to)
		if errors.Is(err, windows.ERROR_INVALID_PARAMETER) || errors.Is(err, windows.ERROR_NOT_SUPPORTED) {
			// The filesystem has no POSIX semantics (only NTFS does).
			err = os.Rename(from, to)
		}
		busy := errors.Is(err, windows.ERROR_ACCESS_DENIED) || errors.Is(err, windows.ERROR_SHARING_VIOLATION)
		if !busy || time.Now().After(deadline) {
			return err
		}
		time.Sleep(delay)
	}
}

// fileRenameInfoEx is FILE_RENAME_INFO with the flags the Ex class takes.
type fileRenameInfoEx struct {
	flags          uint32
	rootDirectory  windows.Handle
	fileNameLength uint32
	fileName       [1]uint16
}

func posixRename(from, to string) error {
	path, err := windows.UTF16PtrFromString(from)
	if err != nil {
		return err
	}
	name, err := windows.UTF16FromString(to)
	if err != nil {
		return err
	}
	h, err := windows.CreateFile(path, windows.DELETE|windows.SYNCHRONIZE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return &os.LinkError{Op: "rename", Old: from, New: to, Err: err}
	}
	defer func() { _ = windows.CloseHandle(h) }()

	// The name follows the fixed fields, so the buffer is their size plus the
	// name's, whose terminating NUL is not counted.
	size := int(unsafe.Offsetof(fileRenameInfoEx{}.fileName)) + 2*len(name)
	buf := make([]uint64, (max(size, int(unsafe.Sizeof(fileRenameInfoEx{})))+7)/8)
	info := (*fileRenameInfoEx)(unsafe.Pointer(&buf[0]))
	info.flags = windows.FILE_RENAME_REPLACE_IF_EXISTS | windows.FILE_RENAME_POSIX_SEMANTICS
	info.fileNameLength = uint32(2 * (len(name) - 1))
	copy(unsafe.Slice(&info.fileName[0], len(name)), name)
	err = windows.SetFileInformationByHandle(h, windows.FileRenameInfoEx, (*byte)(unsafe.Pointer(info)), uint32(8*len(buf)))
	if err != nil {
		return &os.LinkError{Op: "rename", Old: from, New: to, Err: err}
	}
	return nil
}

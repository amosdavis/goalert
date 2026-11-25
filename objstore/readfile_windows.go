//go:build windows

package objstore

import (
	"io"
	"os"

	"golang.org/x/sys/windows"
)

// readWholeFile opens name with delete sharing as well as read and write
// sharing. Without FILE_SHARE_DELETE, another process cannot atomically rename
// a replacement over the file while a reader still has it open.
func readWholeFile(name string) ([]byte, error) {
	path, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return nil, err
	}
	h, err := windows.CreateFile(path, windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(h), name)
	defer func() { _ = f.Close() }()
	return io.ReadAll(f)
}

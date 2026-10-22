//go:build windows

package lode

import (
	"os"

	"golang.org/x/sys/windows"
)

// openRead opens a stored object for reading with delete sharing. os.Open
// on Windows does not grant FILE_SHARE_DELETE, so an open reader would make
// a concurrent CompareAndSwap fail to replace the pointer, and retention
// fail to delete an object, with a sharing violation.
func openRead(name string) (*os.File, error) {
	path, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	const share = windows.FILE_SHARE_READ | windows.FILE_SHARE_WRITE | windows.FILE_SHARE_DELETE
	h, err := windows.CreateFile(path, windows.GENERIC_READ, share, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		// The Errno keeps os.IsNotExist and errors.Is(err, fs.ErrNotExist)
		// working, as they do for os.Open.
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	return os.NewFile(uintptr(h), name), nil
}

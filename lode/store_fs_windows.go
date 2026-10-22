//go:build windows

package lode

import (
	"os"
	"path/filepath"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

// On Windows an open file blocks its replacement and deletion unless every
// handle to it grants delete sharing, and os.Open does not grant it. The
// filesystem store therefore opens objects for reading, and replaces the
// latest pointer, through the Win32 API directly.

const shareAll = windows.FILE_SHARE_READ | windows.FILE_SHARE_WRITE | windows.FILE_SHARE_DELETE

// openRead opens a stored object for reading with delete sharing, so that a
// reader never prevents a concurrent CompareAndSwap from replacing the
// pointer or retention from deleting the object.
func openRead(name string) (*os.File, error) {
	h, err := createFile(name, windows.GENERIC_READ)
	if err != nil {
		// The Errno keeps os.IsNotExist and errors.Is(err, fs.ErrNotExist)
		// working, as they do for os.Open.
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	return os.NewFile(uintptr(h), name), nil
}

// replaceFile atomically renames from to to, replacing an existing to even
// while readers hold it open. os.Rename cannot: MoveFileEx fails on a target
// that is open. The POSIX-semantics rename used here requires Windows 10
// version 1607 or later on NTFS; elsewhere it returns an error.
func replaceFile(from, to string) error {
	fail := func(err error) error { return &os.LinkError{Op: "rename", Old: from, New: to, Err: err} }
	h, err := createFile(from, windows.DELETE|windows.SYNCHRONIZE)
	if err != nil {
		return fail(err)
	}
	defer func() { _ = windows.CloseHandle(h) }()

	target, err := extendedLength(to)
	if err != nil {
		return fail(err)
	}
	name, err := windows.UTF16FromString(target) // NUL-terminated
	if err != nil {
		return fail(err)
	}
	info, size := newRenameInfo(name)
	if err := windows.SetFileInformationByHandle(h, windows.FileRenameInfoEx, info, size); err != nil {
		return fail(err)
	}
	return nil
}

// fileRenameInfo is FILE_RENAME_INFO with its variable-length FileName. Go
// lays the fixed fields out as the C struct does on 32- and 64-bit Windows.
type fileRenameInfo struct {
	Flags          uint32
	RootDirectory  windows.Handle
	FileNameLength uint32
	FileName       [1]uint16
}

// newRenameInfo returns a FILE_RENAME_INFO buffer that replaces the target
// with POSIX semantics, and its size. name is the NUL-terminated target.
func newRenameInfo(name []uint16) (*byte, uint32) {
	size := unsafe.Offsetof(fileRenameInfo{}.FileName) + uintptr(len(name))*unsafe.Sizeof(name[0])
	buf := make([]uint64, (size+7)/8) // 8-byte aligned, as the struct requires
	info := (*fileRenameInfo)(unsafe.Pointer(&buf[0]))
	info.Flags = windows.FILE_RENAME_REPLACE_IF_EXISTS | windows.FILE_RENAME_POSIX_SEMANTICS
	info.FileNameLength = uint32((len(name) - 1) * 2) // bytes, excluding the NUL
	copy(unsafe.Slice(&info.FileName[0], len(name)), name)
	return (*byte)(unsafe.Pointer(&buf[0])), uint32(size)
}

// createFile opens an existing file with full sharing.
func createFile(name string, access uint32) (windows.Handle, error) {
	path, err := extendedLength(name)
	if err != nil {
		return windows.InvalidHandle, err
	}
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return windows.InvalidHandle, err
	}
	return windows.CreateFile(p, access, shareAll, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
}

// extendedLength returns the absolute \\?\ form of p, which lifts the
// MAX_PATH limit. os.Open applies this conversion itself; Win32 calls made
// directly must apply it.
func extendedLength(p string) (string, error) {
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	switch {
	case strings.HasPrefix(abs, `\\?\`):
		return abs, nil
	case strings.HasPrefix(abs, `\\`):
		return `\\?\UNC\` + abs[2:], nil
	default:
		return `\\?\` + abs, nil
	}
}

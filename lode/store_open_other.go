//go:build !windows

package lode

import "os"

// openRead opens a stored object for reading. Outside Windows an open file
// never prevents another process from replacing or deleting the object.
func openRead(name string) (*os.File, error) {
	return os.Open(name)
}

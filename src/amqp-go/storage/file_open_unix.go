//go:build !windows

package storage

import "os"

func openStorageFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, perm)
}

func platformLiteralName(name string) bool { return true }
func platformSegmentName(name string) bool { return true }

func replaceStorageFile(source, target string) error { return os.Rename(source, target) }

//go:build !windows

package server

import "golang.org/x/sys/unix"

func readDiskFreeBytes(path string) (uint64, bool) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, false
	}
	return stat.Bavail * uint64(stat.Bsize), true
}

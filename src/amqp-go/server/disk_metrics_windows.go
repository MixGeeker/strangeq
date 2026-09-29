package server

import "golang.org/x/sys/windows"

func readDiskFreeBytes(path string) (uint64, bool) {
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, false
	}
	var available uint64
	if err := windows.GetDiskFreeSpaceEx(name, &available, nil, nil); err != nil {
		return 0, false
	}
	return available, true
}

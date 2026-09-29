package server

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	kernelMemory       = windows.NewLazySystemDLL("kernel32.dll")
	processMemoryInfo  = kernelMemory.NewProc("K32GetProcessMemoryInfo")
	globalMemoryStatus = kernelMemory.NewProc("GlobalMemoryStatusEx")
)

// 字段布局对应 Windows PROCESS_MEMORY_COUNTERS，SIZE_T 随目标架构变化。
type processMemoryCounters struct {
	Size, PageFaultCount                               uint32
	PeakWorkingSetSize, WorkingSetSize                 uintptr
	QuotaPeakPagedPoolUsage, QuotaPagedPoolUsage       uintptr
	QuotaPeakNonPagedPoolUsage, QuotaNonPagedPoolUsage uintptr
	PagefileUsage, PeakPagefileUsage                   uintptr
}

func readProcessRSSBytes() (uint64, bool) {
	var counters processMemoryCounters
	counters.Size = uint32(unsafe.Sizeof(counters))
	result, _, _ := processMemoryInfo.Call(uintptr(windows.CurrentProcess()), uintptr(unsafe.Pointer(&counters)), uintptr(counters.Size))
	return uint64(counters.WorkingSetSize), result != 0
}

func readTotalMemoryBytes() (uint64, bool) {
	// MEMORYSTATUSEX 中容量字段全部为 DWORDLONG。
	status := struct {
		Length, MemoryLoad                                 uint32
		TotalPhys, AvailPhys, TotalPageFile, AvailPageFile uint64
		TotalVirtual, AvailVirtual, AvailExtendedVirtual   uint64
	}{}
	status.Length = uint32(unsafe.Sizeof(status))
	result, _, _ := globalMemoryStatus.Call(uintptr(unsafe.Pointer(&status)))
	return status.TotalPhys, result != 0
}

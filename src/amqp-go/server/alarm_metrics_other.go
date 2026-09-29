//go:build !linux && !windows

package server

// On non-Linux platforms (notably the darwin dev/test host) there is no clean,
// dependency-free way to read process RSS or a memory ceiling, so the SQ-12
// memory arm degrades gracefully: buildAlarmThresholds sees no detectable total
// memory, leaves the memory arm disarmed, and the monitor never trips a memory
// alarm. Disk capacity uses the platform-specific readDiskFreeBytes function.
// Linux and Windows provide their own process and machine memory samplers.
// Tests inject deterministic samplers directly, so they are platform-independent.
func readProcessRSSBytes() (uint64, bool) { return 0, false }

func readTotalMemoryBytes() (uint64, bool) { return 0, false }

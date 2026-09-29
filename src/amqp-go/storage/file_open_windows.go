package storage

import (
	"os"
	"path/filepath"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

// 长期持有的 WAL/segment 句柄允许替换或删除路径，读取仍指向原文件。
// 这里只接受存储层实际使用的只读和追加模式，避免模拟完整 os.OpenFile 合同。
func openStorageFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	fail := func(err error) (*os.File, error) {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	access := uint32(windows.GENERIC_READ)
	disposition := uint32(windows.OPEN_EXISTING)
	switch flag {
	case os.O_RDONLY:
	case os.O_CREATE | os.O_WRONLY | os.O_APPEND, os.O_CREATE | os.O_RDWR | os.O_APPEND:
		access = windows.FILE_APPEND_DATA | windows.FILE_WRITE_ATTRIBUTES | windows.FILE_WRITE_EA | windows.STANDARD_RIGHTS_WRITE | windows.SYNCHRONIZE
		if flag&os.O_RDWR != 0 {
			access |= windows.GENERIC_READ
		}
		disposition = windows.OPEN_ALWAYS
	default:
		return fail(os.ErrInvalid)
	}
	path, err := storageWindowsPath(name)
	if err != nil {
		return fail(err)
	}
	attrs := uint32(windows.FILE_ATTRIBUTE_NORMAL)
	if perm&0222 == 0 && flag&os.O_CREATE != 0 {
		attrs = windows.FILE_ATTRIBUTE_READONLY
	}
	handle, err := windows.CreateFile(path, access,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, disposition, attrs, 0)
	if err != nil {
		return fail(err)
	}
	return os.NewFile(uintptr(handle), name), nil
}

func storageWindowsPath(name string) (*uint16, error) {
	absolute, err := filepath.Abs(name)
	if err != nil {
		return nil, err
	}
	// 显式扩展路径，支持 Unicode 数据目录与超过 MAX_PATH 的完整路径。
	if !strings.HasPrefix(absolute, `\\?\`) {
		if strings.HasPrefix(absolute, `\\`) {
			absolute = `\\?\UNC\` + strings.TrimPrefix(absolute, `\\`)
		} else {
			absolute = `\\?\` + absolute
		}
	}
	return windows.UTF16PtrFromString(absolute)
}

// 压缩只在同一目录内替换文件。POSIX 语义保留旧读取句柄，新的打开操作
// 得到压缩后的文件；不支持该语义时返回错误并保留旧文件与索引。
func replaceStorageFile(source, target string) error {
	fail := func(err error) error {
		return &os.LinkError{Op: "rename", Old: source, New: target, Err: err}
	}
	if filepath.Dir(source) != filepath.Dir(target) {
		return fail(os.ErrInvalid)
	}
	path, err := storageWindowsPath(source)
	if err != nil {
		return fail(err)
	}
	handle, err := windows.CreateFile(path, windows.DELETE|windows.SYNCHRONIZE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return fail(err)
	}
	defer windows.CloseHandle(handle)
	targetPath, err := storageWindowsPath(target)
	if err != nil {
		return fail(err)
	}
	name, err := windows.UTF16FromString(windows.UTF16PtrToString(targetPath))
	if err != nil {
		return fail(err)
	}
	type renameInfo struct {
		Flags  uint32
		Root   windows.Handle
		Length uint32
		Name   [1]uint16
	}
	var layout renameInfo
	nameBytes := (len(name) - 1) * 2
	buffer := make([]byte, int(unsafe.Offsetof(layout.Name))+nameBytes)
	info := (*renameInfo)(unsafe.Pointer(&buffer[0]))
	info.Flags = windows.FILE_RENAME_REPLACE_IF_EXISTS | windows.FILE_RENAME_POSIX_SEMANTICS
	info.Length = uint32(nameBytes)
	copy(unsafe.Slice(&info.Name[0], len(name)-1), name[:len(name)-1])
	if err := windows.SetFileInformationByHandle(handle, windows.FileRenameInfoEx, &buffer[0], uint32(len(buffer))); err != nil {
		return fail(err)
	}
	return nil
}

// 小写 ASCII 的安全名称可直接保存；其余名称使用既有的字节编码槽，
// 从而让 Windows 的大小写折叠、设备名与尾随点/空格保持互不混淆。
func platformLiteralName(name string) bool {
	for _, ch := range name {
		if ch >= 127 || ch >= 'A' && ch <= 'Z' {
			return false
		}
	}
	return platformSegmentName(name)
}

// Segment 的标记和实际目录枚举已经区分大小写，保留合法历史目录。
func platformSegmentName(name string) bool {
	if strings.HasSuffix(name, ".") || strings.HasSuffix(name, " ") {
		return false
	}
	for _, ch := range name {
		if ch < 32 || strings.ContainsRune(`<>:"/\|?*`, ch) {
			return false
		}
	}
	stem, _, _ := strings.Cut(strings.ToLower(name), ".")
	stem = strings.TrimRight(stem, " ")
	switch stem {
	case "con", "prn", "aux", "nul", "conin$", "conout$",
		"com¹", "com²", "com³", "lpt¹", "lpt²", "lpt³":
		return false
	}
	if len(stem) == 4 && (strings.HasPrefix(stem, "com") || strings.HasPrefix(stem, "lpt")) && stem[3] >= '1' && stem[3] <= '9' {
		return false
	}
	return true
}

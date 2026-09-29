# Windows 原生部署

StrangeQ 在 Windows 10 1809 / Windows Server 2019 或更高版本的 x64 系统上以独立 `amqp-server.exe` 运行，客户端使用 AMQP 0-9-1。发行构建固定使用 `ci/contract.json` 指定的 Go 版本；数据保存在本机 NTFS 卷，不需要 Erlang 或 Docker。

使用 Windows 发行 ZIP 时，解压后在程序目录执行 `./amqp-server.exe --generate-config config.local.yaml`，编辑配置后执行 `./amqp-server.exe --config config.local.yaml`。从源码构建使用以下命令：

```powershell
Set-Location src/amqp-go
go build -o ../../bin/amqp-server.exe ./cmd/amqp-server
../../bin/amqp-server.exe --generate-config config.local.yaml
../../bin/amqp-server.exe --config config.local.yaml
```

配置中明确指定监听地址、独立数据目录和认证文件。仅供本机业务使用时监听 `127.0.0.1:5672`；`storage.fsync` 保持 `true`。账号、密码哈希和 vhost 权限按 [授权合同](AUTHORIZATION.md) 配置；跨机器传输按 [TLS 合同](TLS.md) 配置。业务数据、账号文件和日志放在程序升级目录之外。

Windows 使用前台进程生命周期：`server.daemonize` 为 `false`，由服务包装器或进程管理器管理常驻运行。Unix 双重派生守护模式在 Windows 上返回明确错误。停止会关闭监听、等待连接处理结束并关闭自有存储文件和日志，全部清理完成后主进程才返回，便于原目录重启或升级。交互运行时 Ctrl+C 触发停止；强制终止后的恢复遵循持久消息合同，尚未确认的发布允许重发，消费者需要幂等。

服务管理器可使用 `--shutdown-on-stdin-eof` 并持有子进程专用 stdin 管道；关闭管道触发同样的有序停止，父进程异常退出也会触发。此选项与 daemonize 互斥，不启用时 stdin 不控制服务。管理器应等待进程退出，再报告服务已停止。

安装器用 `--hash-password-stdin` 生成 bcrypt 哈希：stdin 提供 1 至 72 字节的原始密码，stdout 只返回哈希及换行，错误不回显输入。此命令不启动 broker，也不从命令行接收密码。账号文件和服务配置仍须限制为部署身份可访问。

磁盘容量通过 Windows API 采集；内存告警使用当前进程工作集与系统物理内存。WAL 使用文件同步，Windows 目录同步沿用独立平台实现。进程崩溃恢复测试验证已确认消息恢复，实际断电、存储设备缓存与备份恢复属于部署验收。

持久化名称保留 AMQP 的大小写与字节身份。Windows 文件系统中的保留名称、路径分隔符、特殊字符和可能发生大小写折叠的名称使用独立编码槽保存，原始名称从元数据或目录标记恢复。WAL 与 segment 句柄允许删除共享，使日志回收和压缩替换能与既有读取共存；写入仍经过文件同步，再切换索引。数据目录由单个 broker 进程独占。

压缩替换使用 [Windows 文件重命名的 POSIX 语义](https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntifs/ns-ntifs-_file_rename_information)，保留旧读取句柄。部署验收在实际数据卷运行存储回归；不支持该操作的卷会明确报告压缩失败并保留原数据。跨操作系统迁移通过消息导出与重新声明拓扑完成。

Windows 构建和协议测试由构建流水线执行，发行流水线提供 Windows x64 ZIP。兼容性验收覆盖确认发布、无路由返回、拒绝与重投、死信、消费者流控以及持久消息重启恢复。

被动声明只检查既有 exchange/queue，缺失返回 channel 级 404；主动声明的属性与 Profile 死信参数冲突返回 406。

`basic.cancel` 停止后续投递，已发送到客户端的未确认消息仍属于原 channel，可继续 ACK、NACK 或 Reject。尚在内部缓冲的消息回到 ready 队列；关闭 channel 或连接时再重投剩余未确认消息。这使客户端能先取消消费，再排空正在处理的业务操作。

## 历史本地验证记录

2026-09-29，本机 Windows x64 / NTFS，Go 1.26.1：

- auth、config、interfaces、protocol、cmd/amqp-server 测试通过。
- `go test ./server ./storage ./broker -short` 对应各包回归通过；Windows 专用名称、长路径和压缩替换测试通过。依赖 POSIX 权限位与 RLIMIT_FSIZE 的故障注入由 Unix 测试覆盖。
- 原生进程强制结束并从同一数据目录重启后，已确认持久消息与 binding 恢复；认证、TLS 和持久恢复定向测试通过。
- `go vet ./server ./storage ./broker ./cmd/amqp-server` 通过；Linux amd64、macOS amd64 交叉构建通过。
- Echoo 的 AMQP Profile 19 项测试及支付模块 24 项测试在原生 broker 上通过。

以上是该次本地验证范围。当前流水线工具链由 `ci/contract.json` 固定，Windows/Linux 安装、升级、回退和同平台迁移的结果见 [Actions](https://github.com/MixGeeker/strangeq/actions)；发版门禁与未覆盖范围见 [发行合同](RELEASE.md)。

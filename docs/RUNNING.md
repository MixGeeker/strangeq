# StrangeQ 原生程序

解包后运行 `./amqp-server --version` 核对版本。使用 `./amqp-server --generate-config config.local.yaml` 生成完整配置，再执行 `./amqp-server --config config.local.yaml`。

显式设置监听地址、独立持久数据目录、认证文件及 vhost 权限。仅供本机业务使用时监听 loopback。持久化默认开启 fsync；认证与 TLS 按同包 AUTHORIZATION.md 和 TLS.md 配置。样例用于说明配置字段，不直接充当生产环境配置。

程序与数据分目录部署，数据目录由单个 broker 实例使用。服务管理器保持前台运行并等待正常停止；支持 `--shutdown-on-stdin-eof` 以专用 stdin 管道通知停止。`--hash-password-stdin` 从 stdin 接收密码并返回 bcrypt 哈希，不在命令行传递密码。

升级前停止服务，保存完整数据和配置冷备份，按已经验证的准确版本路径更换程序。跨版本协议兼容不代表数据格式兼容。失败恢复使用对应旧程序和完整冷备份，保留失败现场。发布确认后的持久消息恢复与业务幂等共同承担消息可靠性；消费者应允许重投。

包内 build-metadata.json 记录版本、源码提交、工具链、依赖和逐文件摘要。归档另提供 SHA-256，GitHub Release 提供来源证明及对应测试结果。

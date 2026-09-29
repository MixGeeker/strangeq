# StrangeQ GitHub Actions

工具链、六个平台和准确升级基线统一由 `ci/contract.json` 管理。发行与兼容声明以 [发行合同](../../docs/RELEASE.md) 为准。

| 工作流 | 入口 | 结果 |
| --- | --- | --- |
| `build.yml` | PR、main/develop/codex 分支、手动运行、发行复用 | 格式、vet、依赖校验、发行工具回归、Linux race、Windows 原生、协议测试、六平台归档、安装/升级/回退/迁移验收；统一 `CI gate` |
| `release.yml` | 推送 `v*` tag | 复用完整 CI，核验报告对应最终归档，生成来源证明，创建不可覆盖的 Release 草稿 |
| `publish.yml` | 手动输入已有草稿 tag | 验证来源、准确 tag、原构建成功、完整资产摘要后发布原草稿 |
| `codeql.yml` | main 的推送/PR、每周 | 静态分析，具体权限及计划见工作流 |
| `../dependabot.yml` | 每周 | Go module 与 GitHub Actions 依赖更新建议 |

## 验证层次

`package` 在 Linux 固定工具链构建六种归档。`upgrade` 在 Windows Server 2022 与 Ubuntu 24.04 下载本次归档，校验后运行候选程序；旧程序从固定历史提交重建。测试空目录安装、旧消息恢复、强杀与重启、不支持的 WAL 格式拒绝启动、失败冷备回退和目录迁移。

`upgrade` 输出只含合成测试数据的冷快照。独立 `migration` job 在另一台同平台虚拟机下载快照和同一归档，校验完整文件集合，再恢复旧消息、写入新消息并重启。来源和目的 job、平台、运行编号及程序摘要必须相符。快照保存 3 天，验证报告和 Go JSON 事件保存 14 天。Release 只携带报告与摘要，不携带快照。

升级与迁移是必跑套件；缺少输入或跳过都使门禁失败。基础测试的已有跳过逐项写入摘要，不能当作对应能力已验证。macOS、Linux arm64/386 目前只有构建与归档校验证据。整机重启、掉电和服务注册不在托管 runner 场景中。

## 本地命令

在仓库根目录，使用合同指定的 Go 和 Python 3.10+：

```text
python -m unittest discover -s scripts -p test_*.py -v
python scripts/ci.py check
python scripts/ci.py unit
python scripts/ci.py race
python scripts/ci.py conformance
go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.7
python scripts/release.py --help
```

Windows 的 race 需要另配 C 编译器；流水线在 Linux 执行 race。构建命令默认拒绝有已跟踪修改的源码；本地临时使用 `--allow-dirty` 的归档明确标记 dirty，发行门禁拒绝该归档。

Windows 原生套件按 Go package 顺序运行，各测试内部的并发场景保持原样，避免几个独立存储压力套件同时争用宿主资源而干扰多队列速率断言。吞吐阈值和消息完整性断言保持原值。

`scripts/ci.py upgrade` 要求 `STRANGEQ_BASELINE_BINARY`、`STRANGEQ_CANDIDATE_BINARY`、`STRANGEQ_UPGRADE_REPORT`；设置 `STRANGEQ_MIGRATION_EXPORT` 可导出测试快照。`scripts/ci.py migration` 要求 `STRANGEQ_MIGRATION_SOURCE`、`STRANGEQ_CANDIDATE_BINARY`、`STRANGEQ_MIGRATION_REPORT`。本地同机导入报告的 `separateMachineTested` 为 false，不能代替 CI 换机证据。

## 仓库配置与故障处理

管理员可将 `CI gate` 设为合并必需检查，为 tag 配置保护，并在 `release` Environment 配置审核人和允许发布的分支。Environment 名称本身不表示已启用人工审核。手动工作流需要先进入仓库默认分支才会显示标准入口。

tag 工作流只创建草稿。发布时输入草稿 tag；脚本先验证清单来源，再验证原构建、资产字节和 tag 没有改变，最后将草稿公开。预发布版本不会设为 latest。

失败时查看 Actions 对应 job 的完整 JSON 事件与摘要。安装或迁移失败时额外保存 broker 日志。修复代码后使用新提交重新跑 CI；已创建的同名 Release 不会被覆盖。如果上传中断留下不完整草稿，维护者须先核查并处理该未发布草稿，再重跑对应 tag 工作流；已公开版本使用新版本修复，不复用或移动旧 tag。

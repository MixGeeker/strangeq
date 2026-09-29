package upgrade

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

type process struct {
	cmd     *exec.Cmd
	input   io.WriteCloser
	done    chan error
	log     *os.File
	stopped bool
}

func (p *process) stop(kill bool) error {
	if p.stopped {
		return nil
	}
	p.stopped = true
	defer p.input.Close()
	if kill {
		_ = p.cmd.Process.Kill()
	} else {
		_ = p.input.Close()
	}
	select {
	case err := <-p.done:
		_ = p.log.Close()
		if kill {
			return nil
		}
		return err
	case <-time.After(30 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
		_ = p.log.Close()
		return fmt.Errorf("broker did not stop within deadline")
	}
}

func launch(t *testing.T, executable, cfg, url, logPath string) (*process, *amqp.Connection) {
	t.Helper()
	command := exec.Command(executable, "--config", cfg, "--shutdown-on-stdin-eof")
	for _, item := range os.Environ() {
		if !strings.HasPrefix(strings.ToUpper(item), "AMQP_") {
			command.Env = append(command.Env, item)
		}
	}
	input, err := command.StdinPipe()
	require.NoError(t, err)
	log, err := os.Create(logPath)
	require.NoError(t, err)
	command.Stdout, command.Stderr = log, log
	require.NoError(t, command.Start())
	p := &process{cmd: command, input: input, done: make(chan error, 1), log: log}
	go func() { p.done <- command.Wait() }()
	t.Cleanup(func() { _ = p.stop(true) })
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
		conn, err := amqp.DialConfig(url, amqp.Config{Dial: amqp.DefaultDial(time.Second)})
		if err == nil {
			t.Cleanup(func() { _ = conn.Close() })
			return p, conn
		}
		select {
		case err := <-p.done:
			p.stopped = true
			_ = log.Close()
			t.Fatalf("broker exited before readiness: %v; log: %s", err, logPath)
		default:
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("broker readiness timed out; log: %s", logPath)
	return nil, nil
}

func hashFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func copyTree(t *testing.T, source, target string) map[string]string {
	t.Helper()
	_, err := os.Stat(target)
	require.True(t, os.IsNotExist(err), "copy target must not exist")
	hashes := map[string]string{}
	require.NoError(t, filepath.WalkDir(source, func(path string, entry fs.DirEntry, failure error) error {
		if failure != nil {
			return failure
		}
		relative, err := filepath.Rel(source, path)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return fmt.Errorf("snapshot path outside source")
		}
		destination := filepath.Join(target, relative)
		if entry.IsDir() {
			return os.MkdirAll(destination, 0700)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("unsupported snapshot entry: %s", relative)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := os.WriteFile(destination, data, 0600); err != nil {
			return err
		}
		hashes[filepath.ToSlash(relative)] = hashFile(t, path)
		require.Equal(t, hashes[filepath.ToSlash(relative)], hashFile(t, destination))
		return nil
	}))
	return hashes
}

func treeHashes(t *testing.T, root string) map[string]string {
	t.Helper()
	hashes, err := snapshotHashes(root)
	require.NoError(t, err)
	return hashes
}

func snapshotHashes(root string) (map[string]string, error) {
	hashes := map[string]string{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("unexpected data entry")
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(data)
		hashes[filepath.ToSlash(relative)] = hex.EncodeToString(digest[:])
		return nil
	})
	return hashes, err
}

func verifySnapshot(root string, expected map[string]string) error {
	actual, err := snapshotHashes(root)
	if err != nil {
		return err
	}
	if len(expected) == 0 || !reflect.DeepEqual(actual, expected) {
		return fmt.Errorf("snapshot file set or checksum mismatch")
	}
	return nil
}

func saveReport(t *testing.T, variable, root string, report map[string]any) {
	t.Helper()
	t.Cleanup(func() {
		path := os.Getenv(variable)
		if path == "" {
			return
		}
		if t.Failed() {
			report["status"] = "failed"
		}
		data, err := json.MarshalIndent(report, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
		require.NoError(t, os.WriteFile(path, append(data, '\n'), 0600))
		if t.Failed() {
			// 只归档 broker 日志，不复制账号文件、密码或配置。
			logs, err := filepath.Glob(filepath.Join(root, "*.log"))
			require.NoError(t, err)
			for _, log := range logs {
				data, err := os.ReadFile(log)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(path), filepath.Base(log)), data, 0600))
			}
		}
	})
}

func rejectUnsupportedStorage(t *testing.T, executable, configPath, store string) {
	t.Helper()
	var wal string
	require.NoError(t, filepath.WalkDir(store, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".wal") && wal == "" {
			wal = path
		}
		return nil
	}))
	require.NotEmpty(t, wal, "fixture must contain persisted WAL")
	file, err := os.OpenFile(wal, os.O_RDWR, 0)
	require.NoError(t, err)
	header := make([]byte, 6)
	_, err = file.ReadAt(header, 0)
	require.NoError(t, err)
	require.Equal(t, "SQWAL", string(header[:5]))
	_, err = file.WriteAt([]byte{255}, 5)
	require.NoError(t, err)
	require.NoError(t, file.Sync())
	require.NoError(t, file.Close())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "--config", configPath)
	for _, item := range os.Environ() {
		if !strings.HasPrefix(strings.ToUpper(item), "AMQP_") {
			command.Env = append(command.Env, item)
		}
	}
	output, err := command.CombinedOutput()
	require.Error(t, err, "unsupported storage must refuse startup")
	require.NoError(t, ctx.Err(), "refusal must terminate without being killed by the test")
	require.Contains(t, strings.ToLower(string(output)), "unsupported wal", "refusal must identify storage incompatibility")
}

func publish(t *testing.T, conn *amqp.Connection, queue, prefix string, count int) {
	t.Helper()
	channel, err := conn.Channel()
	require.NoError(t, err)
	defer channel.Close()
	require.NoError(t, channel.ExchangeDeclare("upgrade.exchange", "direct", true, false, false, false, nil))
	_, err = channel.QueueDeclare(queue, true, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, channel.QueueBind(queue, queue, "upgrade.exchange", false, nil))
	require.NoError(t, channel.Confirm(false))
	confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	returns := channel.NotifyReturn(make(chan amqp.Return, 1))
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("%s-%04d", prefix, i)
		require.NoError(t, channel.PublishWithContext(context.Background(), "upgrade.exchange", queue, true, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent, MessageId: id, CorrelationId: "upgrade-fixture", ContentType: "application/octet-stream",
			Headers: amqp.Table{"fixture": "upgrade-v1"}, Body: bytes.Repeat([]byte(id), 128),
		}))
		select {
		case result := <-confirms:
			require.True(t, result.Ack, "publish rejected: %s", id)
		case <-time.After(10 * time.Second):
			t.Fatal("publisher confirmation timed out")
		}
		select {
		case <-returns:
			t.Fatal("fixture message was unroutable")
		default:
		}
	}
}

func verifyMessages(t *testing.T, conn *amqp.Connection, queue, prefix string, count int) {
	t.Helper()
	channel, err := conn.Channel()
	require.NoError(t, err)
	defer channel.Close()
	require.NoError(t, channel.ExchangeDeclarePassive("upgrade.exchange", "direct", true, false, false, false, nil))
	_, err = channel.QueueDeclarePassive(queue, true, false, false, false, nil)
	require.NoError(t, err)
	seen := map[string]bool{}
	var last uint64
	for i := 0; i < count; i++ {
		message, ok, err := channel.Get(queue, false)
		require.NoError(t, err)
		require.True(t, ok, "persistent fixture missing")
		require.False(t, seen[message.MessageId], "duplicate fixture delivery")
		seen[message.MessageId] = true
		require.Equal(t, bytes.Repeat([]byte(message.MessageId), 128), message.Body)
		require.Equal(t, "upgrade-fixture", message.CorrelationId)
		require.Equal(t, "application/octet-stream", message.ContentType)
		require.Equal(t, "upgrade-v1", message.Headers["fixture"])
		require.Equal(t, uint8(amqp.Persistent), message.DeliveryMode)
		last = message.DeliveryTag
	}
	for i := 0; i < count; i++ {
		require.True(t, seen[fmt.Sprintf("%s-%04d", prefix, i)], "fixture identity mismatch")
	}
	_, ok, err := channel.Get(queue, false)
	require.NoError(t, err)
	require.False(t, ok, "unexpected extra message")
	// 检查后保留原消息；本测试证明恢复，不声明 ACK 崩溃后绝不重投。
	require.NoError(t, channel.Nack(last, true, true))
	_, err = channel.QueueInspect(queue)
	require.NoError(t, err)
}

func TestUpgradeAndColdRestore(t *testing.T) {
	baseline, candidate := os.Getenv("STRANGEQ_BASELINE_BINARY"), os.Getenv("STRANGEQ_CANDIDATE_BINARY")
	if baseline == "" || candidate == "" {
		if os.Getenv("STRANGEQ_REQUIRE_UPGRADE") == "1" {
			t.Fatal("upgrade binaries are required")
		}
		t.Skip("升级验证通过 scripts/ci.py upgrade 提供准确新旧归档程序")
	}
	root := t.TempDir()
	instance := filepath.Join(root, "实例")
	require.NoError(t, os.Mkdir(instance, 0700))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	secret := make([]byte, 24)
	_, err = rand.Read(secret)
	require.NoError(t, err)
	password := hex.EncodeToString(secret)
	hasher := exec.Command(baseline, "--hash-password-stdin")
	hasher.Stdin = strings.NewReader(password)
	hash, err := hasher.Output()
	require.NoError(t, err)
	auth, err := json.Marshal(map[string]any{"users": []any{map[string]any{
		"username": "upgrade", "password_hash": strings.TrimSpace(string(hash)), "loopback_only": true,
		"vhost_permissions": []any{map[string]any{"vhost": "/", "permission": map[string]string{"configure": ".*", "write": ".*", "read": ".*"}}},
	}}})
	require.NoError(t, err)
	authPath := filepath.Join(instance, "auth.json")
	require.NoError(t, os.WriteFile(authPath, auth, 0600))
	cfg := config.DefaultConfig()
	cfg.Network.Address, cfg.Network.Port = fmt.Sprintf("127.0.0.1:%d", port), port
	cfg.Storage.Path = filepath.Join(instance, "store")
	cfg.Security.AuthenticationEnabled, cfg.Security.AuthorizationEnabled = true, true
	cfg.Security.AuthenticationFilePath = authPath
	cfg.Server.Daemonize, cfg.Server.LogLevel = false, "warn"
	cfg.Engine.RingBufferSize, cfg.Engine.SpillThresholdPercent = 1024, 50
	cfg.Engine.SegmentCheckpointIntervalMS = 100
	configPath := filepath.Join(instance, "config.yaml")
	require.NoError(t, cfg.Save(configPath))
	url := fmt.Sprintf("amqp://upgrade:%s@127.0.0.1:%d/", password, port)
	report := map[string]any{"schemaVersion": 1, "platform": runtime.GOOS + "-" + runtime.GOARCH,
		"baselineSha256": hashFile(t, baseline), "candidateSha256": hashFile(t, candidate),
		"coverage": "native-process-upgrade-crash-and-cold-restore", "machineRebootTested": false,
		"powerLossTested": false, "status": "failed"}
	saveReport(t, "STRANGEQ_UPGRADE_REPORT", root, report)
	// 首次安装消费归档内程序，生成配置并在空目录启动，不借用旧数据。
	fresh := filepath.Join(root, "fresh-install")
	require.NoError(t, os.Mkdir(fresh, 0700))
	freshConfig := filepath.Join(fresh, "config.yaml")
	generated := exec.Command(candidate, "--generate-config", freshConfig)
	_, err = generated.CombinedOutput()
	require.NoError(t, err)
	freshCfg := config.DefaultConfig()
	require.NoError(t, freshCfg.Load(freshConfig))
	require.True(t, freshCfg.FsyncEnabled())
	freshCfg.Network = cfg.Network
	freshCfg.Security = cfg.Security
	freshCfg.Storage.Path = filepath.Join(fresh, "store")
	freshCfg.Security.AuthenticationFilePath = filepath.Join(fresh, "auth.json")
	require.NoError(t, os.WriteFile(freshCfg.Security.AuthenticationFilePath, auth, 0600))
	require.NoError(t, freshCfg.Save(freshConfig))
	installed, freshConn := launch(t, candidate, freshConfig, url, filepath.Join(root, "fresh.log"))
	publish(t, freshConn, "upgrade.fresh", "fresh", 3)
	verifyMessages(t, freshConn, "upgrade.fresh", "fresh", 3)
	require.NoError(t, freshConn.Close())
	require.NoError(t, installed.stop(false))
	report["freshInstallation"] = true
	old, conn := launch(t, baseline, configPath, url, filepath.Join(root, "baseline.log"))
	publish(t, conn, "upgrade.original", "old", 640)
	require.NoError(t, conn.Close())
	require.NoError(t, old.stop(false))
	snapshot := filepath.Join(root, "snapshot")
	snapshotHashes := copyTree(t, instance, snapshot)
	report["snapshotFiles"] = len(snapshotHashes)
	newBroker, conn := launch(t, candidate, configPath, url, filepath.Join(root, "candidate.log"))
	verifyMessages(t, conn, "upgrade.original", "old", 640)
	publish(t, conn, "upgrade.crash", "new", 16)
	require.NoError(t, newBroker.stop(true))
	_ = conn.Close()
	restarted, conn := launch(t, candidate, configPath, url, filepath.Join(root, "restart.log"))
	verifyMessages(t, conn, "upgrade.original", "old", 640)
	verifyMessages(t, conn, "upgrade.crash", "new", 16)
	require.NoError(t, conn.Close())
	require.NoError(t, restarted.stop(false))
	// 模拟升级后的数据格式不受支持；随后从原快照恢复，而非启动旧程序碰撞新数据。
	rejectUnsupportedStorage(t, candidate, configPath, cfg.Storage.Path)
	require.Equal(t, snapshotHashes, treeHashes(t, snapshot), "failed upgrade must preserve the recovery snapshot")
	report["unsupportedStorageRefused"] = true
	require.NoError(t, os.Rename(instance, filepath.Join(root, "failed-candidate")))
	require.Equal(t, snapshotHashes, copyTree(t, snapshot, instance))
	restored, conn := launch(t, baseline, configPath, url, filepath.Join(root, "restored.log"))
	verifyMessages(t, conn, "upgrade.original", "old", 640)
	channel, err := conn.Channel()
	require.NoError(t, err)
	_, err = channel.QueueDeclarePassive("upgrade.crash", true, false, false, false, nil)
	require.Error(t, err, "candidate-only queue must not leak into restored snapshot")
	require.NoError(t, conn.Close())
	require.NoError(t, restored.stop(false))
	report["failedUpgradeColdRestore"] = true
	// 同平台换机模型：程序、数据和配置均迁入另一空目录，源目录保持逐字不变。
	sourceHashes := treeHashes(t, instance)
	destination := filepath.Join(root, "another-machine", "数据")
	require.NoError(t, os.Mkdir(filepath.Dir(destination), 0700))
	copyTree(t, instance, destination)
	program := filepath.Join(filepath.Dir(destination), filepath.Base(candidate))
	programBytes, err := os.ReadFile(candidate)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(program, programBytes, 0700))
	cfg.Storage.Path = filepath.Join(destination, "store")
	cfg.Security.AuthenticationFilePath = filepath.Join(destination, "auth.json")
	destinationConfig := filepath.Join(destination, "config.yaml")
	require.NoError(t, cfg.Save(destinationConfig))
	migrated, conn := launch(t, program, destinationConfig, url, filepath.Join(root, "migrated.log"))
	verifyMessages(t, conn, "upgrade.original", "old", 640)
	require.NoError(t, conn.Close())
	require.NoError(t, migrated.stop(false))
	require.Equal(t, sourceHashes, treeHashes(t, instance), "migration must not modify its source")
	report["directoryMigration"] = true
	report["samePlatformRelocation"] = true
	report["separateMachineTested"] = false
	report["status"] = "passed"
	report["verifiedOriginalMessages"] = 640
	report["verifiedNewMessagesAfterCrash"] = 16
	if destination := os.Getenv("STRANGEQ_MIGRATION_EXPORT"); destination != "" {
		require.NoError(t, os.Mkdir(destination, 0700), "migration export must be a new directory")
		require.NoError(t, verifySnapshot(snapshot, snapshotHashes))
		copyTree(t, snapshot, filepath.Join(destination, "instance"))
		fixture := migrationFixture{SchemaVersion: 1, TestOnly: true, Platform: runtime.GOOS + "-" + runtime.GOARCH,
			RunID: os.Getenv("GITHUB_RUN_ID"), RunAttempt: os.Getenv("GITHUB_RUN_ATTEMPT"), Job: os.Getenv("GITHUB_JOB"),
			BaselineSHA: hashFile(t, baseline), CandidateSHA: hashFile(t, candidate), Password: password, Files: snapshotHashes}
		data, err := json.MarshalIndent(fixture, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(destination, "fixture.json"), append(data, '\n'), 0600))
	}
}

type migrationFixture struct {
	SchemaVersion int               `json:"schemaVersion"`
	TestOnly      bool              `json:"testOnly"`
	Platform      string            `json:"platform"`
	RunID         string            `json:"sourceRunId"`
	RunAttempt    string            `json:"sourceRunAttempt"`
	Job           string            `json:"sourceJob"`
	BaselineSHA   string            `json:"baselineSha256"`
	CandidateSHA  string            `json:"candidateSha256"`
	Password      string            `json:"testPassword"`
	Files         map[string]string `json:"files"`
}

func TestImportMigrationFixture(t *testing.T) {
	source, candidate := os.Getenv("STRANGEQ_MIGRATION_SOURCE"), os.Getenv("STRANGEQ_CANDIDATE_BINARY")
	if source == "" || candidate == "" {
		if os.Getenv("STRANGEQ_REQUIRE_MIGRATION") == "1" {
			t.Fatal("migration fixture and candidate are required")
		}
		t.Skip("换机恢复通过 scripts/ci.py migration 提供另一 job 的测试快照与准确归档程序")
	}
	root := t.TempDir()
	platform := runtime.GOOS + "-" + runtime.GOARCH
	report := map[string]any{"schemaVersion": 1, "platform": platform, "status": "failed",
		"candidateSha256": hashFile(t, candidate), "machineRebootTested": false, "powerLossTested": false}
	saveReport(t, "STRANGEQ_MIGRATION_REPORT", root, report)
	data, err := os.ReadFile(filepath.Join(source, "fixture.json"))
	require.NoError(t, err)
	var fixture migrationFixture
	require.NoError(t, json.Unmarshal(data, &fixture))
	require.Equal(t, 1, fixture.SchemaVersion)
	require.True(t, fixture.TestOnly)
	require.Equal(t, platform, fixture.Platform)
	require.Equal(t, hashFile(t, candidate), fixture.CandidateSHA)
	separateJob := os.Getenv("GITHUB_ACTIONS") == "true"
	if separateJob {
		require.Equal(t, "github-hosted", os.Getenv("RUNNER_ENVIRONMENT"))
		require.NotEmpty(t, fixture.RunID)
		require.Equal(t, os.Getenv("GITHUB_RUN_ID"), fixture.RunID)
		require.Equal(t, os.Getenv("GITHUB_RUN_ATTEMPT"), fixture.RunAttempt)
		require.NotEmpty(t, fixture.Job)
		require.NotEmpty(t, os.Getenv("GITHUB_JOB"))
		require.NotEqual(t, os.Getenv("GITHUB_JOB"), fixture.Job)
	}
	snapshot := filepath.Join(source, "instance")
	require.NoError(t, verifySnapshot(snapshot, fixture.Files), "verify before activation")
	destination := filepath.Join(root, "迁移目标")
	copyTree(t, snapshot, destination)
	configuration := filepath.Join(destination, "config.yaml")
	cfg := config.DefaultConfig()
	require.NoError(t, cfg.Load(configuration))
	require.True(t, cfg.Security.AuthenticationEnabled)
	require.True(t, cfg.Security.AuthorizationEnabled)
	require.True(t, cfg.FsyncEnabled())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	cfg.Network.Address, cfg.Network.Port = fmt.Sprintf("127.0.0.1:%d", port), port
	cfg.Storage.Path = filepath.Join(destination, "store")
	cfg.Security.AuthenticationFilePath = filepath.Join(destination, "auth.json")
	require.NoError(t, cfg.Save(configuration))
	url := fmt.Sprintf("amqp://upgrade:%s@127.0.0.1:%d/", fixture.Password, port)
	broker, conn := launch(t, candidate, configuration, url, filepath.Join(root, "import.log"))
	verifyMessages(t, conn, "upgrade.original", "old", 640)
	publish(t, conn, "upgrade.migrated", "migrated", 16)
	verifyMessages(t, conn, "upgrade.migrated", "migrated", 16)
	require.NoError(t, conn.Close())
	require.NoError(t, broker.stop(false))
	restarted, conn := launch(t, candidate, configuration, url, filepath.Join(root, "import-restart.log"))
	verifyMessages(t, conn, "upgrade.original", "old", 640)
	verifyMessages(t, conn, "upgrade.migrated", "migrated", 16)
	require.NoError(t, conn.Close())
	require.NoError(t, restarted.stop(false))
	require.NoError(t, verifySnapshot(snapshot, fixture.Files), "source remains immutable")
	report["baselineSha256"] = fixture.BaselineSHA
	report["sourceRunId"], report["sourceJob"] = fixture.RunID, fixture.Job
	report["targetJob"] = os.Getenv("GITHUB_JOB")
	report["separateMachineTested"] = separateJob
	report["samePlatformRestore"] = true
	report["verifiedOriginalMessages"], report["verifiedNewMessagesAfterRestart"] = 640, 16
	report["status"] = "passed"
}

func TestSnapshotVerificationRejectsChanges(t *testing.T) {
	for _, scenario := range []string{"modified", "missing", "extra"} {
		t.Run(scenario, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "data.wal")
			require.NoError(t, os.WriteFile(path, []byte("original"), 0600))
			expected := treeHashes(t, root)
			require.NoError(t, verifySnapshot(root, expected))
			switch scenario {
			case "modified":
				require.NoError(t, os.WriteFile(path, []byte("modified"), 0600))
			case "missing":
				require.NoError(t, os.Remove(path))
			case "extra":
				require.NoError(t, os.WriteFile(filepath.Join(root, "unexpected.wal"), []byte("extra"), 0600))
			}
			require.Error(t, verifySnapshot(root, expected))
		})
	}
}

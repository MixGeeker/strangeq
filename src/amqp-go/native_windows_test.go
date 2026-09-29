//go:build windows

package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

// 验证实际 Windows exe：确认后强杀进程，再从相同数据目录恢复。
func TestWindowsNativeConfirmedMessagesSurviveProcessKill(t *testing.T) {
	root := t.TempDir()
	executable := filepath.Join(root, "amqp-server.exe")
	build := exec.Command("go", "build", "-o", executable, "./cmd/amqp-server")
	build.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	output, err := build.CombinedOutput()
	require.NoError(t, err, "%s", output)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	cfg := config.DefaultConfig()
	cfg.Network.Address = fmt.Sprintf("127.0.0.1:%d", port)
	cfg.Network.Port = port
	cfg.Storage.Path = filepath.Join(root, "数据")
	cfg.Server.Daemonize = false
	file := filepath.Join(root, "config.yaml")
	require.NoError(t, cfg.Save(file))

	start := func() (*exec.Cmd, *amqp.Connection) {
		cmd := exec.Command(executable, "--config", file)
		cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
		for _, value := range os.Environ() {
			if !strings.HasPrefix(strings.ToUpper(value), "AMQP_") {
				cmd.Env = append(cmd.Env, value)
			}
		}
		var log bytes.Buffer
		cmd.Stdout, cmd.Stderr = &log, &log
		require.NoError(t, cmd.Start())
		stopped := false
		t.Cleanup(func() {
			if !stopped {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
			}
		})
		for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
			conn, err := amqp.DialConfig(fmt.Sprintf("amqp://guest:guest@127.0.0.1:%d/", port), amqp.Config{Dial: amqp.DefaultDial(time.Second)})
			if err == nil {
				// Wait 由调用方或 cleanup 负责，避免重复回收同一进程。
				t.Cleanup(func() {
					_ = conn.Close()
					if cmd.ProcessState != nil {
						stopped = true
					}
				})
				return cmd, conn
			}
			time.Sleep(50 * time.Millisecond)
		}
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		stopped = true
		t.Fatalf("Windows broker 启动失败: %s", log.String())
		return nil, nil
	}
	child, conn := start()
	channel, err := conn.Channel()
	require.NoError(t, err)
	_, err = channel.QueueDeclare("windows-durable", true, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, channel.ExchangeDeclare("windows-exchange", "topic", true, false, false, false, nil))
	require.NoError(t, channel.QueueBind("windows-durable", "work", "windows-exchange", false, nil))
	require.NoError(t, channel.Confirm(false))
	confirmations := channel.NotifyPublish(make(chan amqp.Confirmation, 8))
	for i := 0; i < 8; i++ {
		require.NoError(t, channel.Publish("windows-exchange", "work", true, false, amqp.Publishing{DeliveryMode: amqp.Persistent, Body: []byte(fmt.Sprint(i))}))
		select {
		case result := <-confirmations:
			require.True(t, result.Ack)
		case <-time.After(5 * time.Second):
			t.Fatal("确认超时")
		}
	}
	require.NoError(t, conn.Close())
	require.NoError(t, child.Process.Kill())
	require.Error(t, child.Wait())

	_, restored := start()
	channel, err = restored.Channel()
	require.NoError(t, err)
	queue, err := channel.QueueDeclarePassive("windows-durable", true, false, false, false, nil)
	require.NoError(t, err)
	require.Equal(t, 8, queue.Messages)
	require.NoError(t, channel.ExchangeDeclarePassive("windows-exchange", "topic", true, false, false, false, nil))
	require.NoError(t, channel.Confirm(false))
	recoveredConfirm := channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	require.NoError(t, channel.Publish("windows-exchange", "work", true, false, amqp.Publishing{DeliveryMode: amqp.Persistent, Body: []byte("8")}))
	select {
	case result := <-recoveredConfirm:
		require.True(t, result.Ack)
	case <-time.After(5 * time.Second):
		t.Fatal("恢复后确认超时")
	}
	queue, err = channel.QueueInspect("windows-durable")
	require.NoError(t, err)
	require.Equal(t, 9, queue.Messages, "持久 binding 必须在重启后继续路由")
	for i := 0; i < 9; i++ {
		message, ok, err := channel.Get("windows-durable", false)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, fmt.Sprint(i), string(message.Body))
		require.NoError(t, message.Ack(false))
	}
}

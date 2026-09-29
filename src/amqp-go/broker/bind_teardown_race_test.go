package broker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// 通过既有存储接口暂停真实删除操作：绑定已清理，队列元数据仍在。
// 装饰器只在测试中存在，不向 broker 注入调度钩子。
type bindTeardownStorage struct {
	interfaces.Storage
	deleting    chan struct{}
	resume      chan struct{}
	bindEntered chan struct{}
	probe       atomic.Bool
	once        sync.Once
}

func (s *bindTeardownStorage) DeleteQueue(name string) error {
	close(s.deleting)
	<-s.resume
	return s.Storage.DeleteQueue(name)
}

func (s *bindTeardownStorage) GetExchange(name string) (*protocol.Exchange, error) {
	exchange, err := s.Storage.GetExchange(name)
	if s.probe.Load() {
		s.once.Do(func() { close(s.bindEntered) })
	}
	return exchange, err
}

// 旧测试用 300 次微秒错峰推测是否跨过删除窗口。在托管 Windows 上，
// 所有绑定都可能落在删除之后，使测试根本没有建立待验证的竞态。
// 这里直接固定窗口，确认并发 BindQueue 已进入，再验证删除后的实际记录。
// 去掉 BindQueue 的互斥时，并发绑定在窗口内成功写入，留下可观察的幽灵记录。
func TestBindQueueRacingDeleteLeavesNoGhostBinding(t *testing.T) {
	b, cleanup := createTestBroker(t)
	t.Cleanup(cleanup)
	pinned := &bindTeardownStorage{Storage: b.storage,
		deleting: make(chan struct{}), resume: make(chan struct{}), bindEntered: make(chan struct{})}
	// 在任何队列或 broker 后台任务创建之前安装装饰器。
	b.storage = pinned
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(pinned.resume) }) }
	var workers sync.WaitGroup
	t.Cleanup(func() {
		release()
		done := make(chan struct{})
		go func() { workers.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("bind/delete workers did not finish")
		}
	})
	require.NoError(t, b.DeclareExchange("ex", "direct", true, false, false, nil))
	_, err := b.DeclareQueue("queue", true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b.BindQueue("queue", "ex", "before", nil))
	before, err := pinned.Storage.GetQueueBindings("queue")
	require.NoError(t, err)
	require.Len(t, before, 1, "fixture must start with a real binding")

	deleted := make(chan error, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		_, err := b.DeleteQueue("queue", false, false)
		deleted <- err
	}()
	select {
	case <-pinned.deleting:
	case <-time.After(10 * time.Second):
		t.Fatal("delete never reached the pinned storage boundary")
	}
	// 用真实存储直接确认窗口，而非依赖延迟或执行顺序推测。
	_, err = pinned.Storage.GetQueue("queue")
	require.NoError(t, err, "queue metadata must still exist inside the pinned window")
	swept, err := pinned.Storage.GetQueueBindings("queue")
	require.NoError(t, err)
	require.Empty(t, swept, "binding sweep must have finished before starting the concurrent bind")

	pinned.probe.Store(true)
	bound := make(chan error, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		bound <- b.BindQueue("queue", "ex", "during", nil)
	}()
	select {
	case <-pinned.bindEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent bind never entered exchange validation")
	}
	var bindErr error
	early := false
	select {
	case bindErr = <-bound:
		early = true
	case <-time.After(time.Second):
		// 给已经进入的调用一次完整调度机会；正确实现等待删除持有的互斥锁。
	}
	release()
	select {
	case err := <-deleted:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("delete did not finish after releasing the storage boundary")
	}
	if !early {
		select {
		case bindErr = <-bound:
		case <-time.After(10 * time.Second):
			t.Fatal("bind did not finish after deletion")
		}
	}
	_, err = pinned.Storage.GetQueue("queue")
	require.ErrorIs(t, err, interfaces.ErrQueueNotFound)
	remaining, err := pinned.Storage.GetQueueBindings("queue")
	require.NoError(t, err)
	require.Empty(t, remaining, "deleted queue retains a ghost binding")
	require.Error(t, bindErr, "bind racing the completed sweep must be refused")
	require.False(t, early, "bind must not finish while deletion still owns the queue")
	require.Error(t, b.BindQueue("queue", "ex", "after", nil))
}

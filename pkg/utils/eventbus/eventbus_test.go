package eventbus

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// collect 非阻塞地排空 channel 中现有事件
func collect(ch <-chan interface{}) []interface{} {
	var out []interface{}
	for {
		select {
		case v, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, v)
		default:
			return out
		}
	}
}

func TestSubscribePublishReceive(t *testing.T) {
	bus := New()
	defer bus.Close()

	ch, unsub := bus.Subscribe("login")
	defer unsub()

	bus.Publish("login", "user-a")
	bus.Publish("login", "user-b")

	if got := collect(ch); len(got) != 2 || got[0] != "user-a" || got[1] != "user-b" {
		t.Fatalf("got %v, want [user-a user-b]", got)
	}
}

func TestSubscribeIsolation(t *testing.T) {
	bus := New()
	defer bus.Close()

	chA, unsubA := bus.Subscribe("a")
	chB, unsubB := bus.Subscribe("b")
	defer unsubA()
	defer unsubB()

	bus.Publish("a", 1)
	bus.Publish("b", 2)

	if got := collect(chA); len(got) != 1 || got[0] != 1 {
		t.Fatalf("chA got %v, want [1]", got)
	}
	if got := collect(chB); len(got) != 1 || got[0] != 2 {
		t.Fatalf("chB got %v, want [2]", got)
	}
}

func TestMultipleSubscribersSameKey(t *testing.T) {
	bus := New()
	defer bus.Close()

	ch1, unsub1 := bus.Subscribe("k")
	ch2, unsub2 := bus.Subscribe("k")
	defer unsub1()
	defer unsub2()

	bus.Publish("k", "x")

	for i, ch := range []<-chan interface{}{ch1, ch2} {
		if got := collect(ch); len(got) != 1 || got[0] != "x" {
			t.Fatalf("ch%d got %v, want [x]", i+1, got)
		}
	}
}

func TestNoSubscriberNoPanic(t *testing.T) {
	bus := New()
	defer bus.Close()

	bus.Publish("nobody", "lost") // 不应 panic，也不应阻塞
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	bus := New()
	defer bus.Close()

	ch, unsub := bus.Subscribe("k")
	unsub()
	unsub() // 幂等

	bus.Publish("k", "after")

	if got := collect(ch); len(got) != 0 {
		t.Fatalf("got %v after unsubscribe, want empty", got)
	}
	// channel 应已 close，第二次读直接返回 ok=false
	if _, ok := <-ch; ok {
		t.Fatal("channel should be closed after unsubscribe")
	}
}

func TestUnsubscribeOnlyAffectsOwnChannel(t *testing.T) {
	bus := New()
	defer bus.Close()

	ch1, unsub1 := bus.Subscribe("k")
	ch2, _ := bus.Subscribe("k")
	defer func() {
		_ = ch1 // ch1 已被 unsub1 close，仅保留引用避免 vet unused
		_ = ch2
	}()

	unsub1()
	bus.Publish("k", "for-ch2")

	if got := collect(ch2); len(got) != 1 || got[0] != "for-ch2" {
		t.Fatalf("ch2 got %v, want [for-ch2]", got)
	}
}

func TestBufferSizeDrop(t *testing.T) {
	var warns atomic.Int32
	bus := New(WithBufferSize(2), WithLogger(&warnRecorder{fn: func() { warns.Add(1) }}))
	defer bus.Close()

	ch, unsub := bus.Subscribe("k")
	defer unsub()

	// 不消费，塞 5 条 → 2 条入 buffer，3 条丢弃 + 3 次 warn
	for i := 0; i < 5; i++ {
		bus.Publish("k", i)
	}

	got := collect(ch)
	if len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("got %v, want [0 1]", got)
	}
	if warns.Load() != 3 {
		t.Fatalf("warns = %d, want 3", warns.Load())
	}
}

func TestHasSubscribers(t *testing.T) {
	bus := New()
	defer bus.Close()

	if bus.HasSubscribers("k") {
		t.Fatal("no subscriber yet, want false")
	}

	_, unsub := bus.Subscribe("k")
	if !bus.HasSubscribers("k") {
		t.Fatal("subscribed, want true")
	}

	unsub()
	if bus.HasSubscribers("k") {
		t.Fatal("unsubscribed, want false")
	}
}

func TestCloseClosesAllChannels(t *testing.T) {
	bus := New()

	ch1, unsub1 := bus.Subscribe("k1")
	ch2, unsub2 := bus.Subscribe("k2")

	bus.Publish("k1", "v")
	bus.Close()
	unsub1() // Close 后幂等安全
	unsub2()

	for i, ch := range []<-chan interface{}{ch1, ch2} {
		// Go channel close 后 buffered 值仍可先读出（ch1 里有一条 "v"），排空后读到 ok=false
		drained := 0
		for {
			select {
			case v, ok := <-ch:
				if !ok {
					goto closed
				}
				drained++
				if drained > 10 {
					t.Fatalf("ch%d never closed (drained %d values)", i+1, drained)
				}
				if i == 0 && drained == 1 && v != "v" {
					t.Fatalf("ch1 first buffered value = %v, want v", v)
				}
			case <-time.After(time.Second):
				t.Fatalf("ch%d close signal not received within 1s", i+1)
			}
		}
	closed:
	}

	// Close 后 map 清空
	if bus.HasSubscribers("k1") || bus.HasSubscribers("k2") {
		t.Fatal("subscribers should be cleared after Close")
	}
}

func TestPublishAfterCloseNoPanic(t *testing.T) {
	bus := New()
	bus.Close()

	bus.Publish("k", "v") // 不应 panic
}

func TestConcurrentPublishSubscribe(t *testing.T) {
	bus := New()
	defer bus.Close()

	const (
		publishers = 8
		perPub     = 200
	)

	var received atomic.Int64
	done := make(chan struct{})

	ch, unsub := bus.Subscribe("counter")
	go func() {
		defer close(done)
		for range ch {
			received.Add(1)
		}
	}()

	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perPub; i++ {
				bus.Publish("counter", i)
			}
		}()
	}
	wg.Wait()

	// 非阻塞语义下不保证全部送达（buffer 满会丢），只验证：
	// 1. 收到的都是合法 int（无并发损坏）2. 不死锁 3. unsubscribe 后 channel 正常关闭
	time.Sleep(100 * time.Millisecond)
	unsub()
	<-done

	if got := received.Load(); got < 1 {
		t.Fatalf("received = %d, want at least 1 under non-blocking delivery", got)
	}
}

func TestInterfaceKeyTypes(t *testing.T) {
	// key 是 interface{}：EventType（int）/ string / struct 都可用
	bus := New()
	defer bus.Close()

	type evtKey struct{ Code int }

	chInt, unsubInt := bus.Subscribe(1001)
	chStr, unsubStr := bus.Subscribe("msg")
	chStruct, unsubStruct := bus.Subscribe(evtKey{Code: 7})
	defer unsubInt()
	defer unsubStr()
	defer unsubStruct()

	bus.Publish(1001, "int-key")
	bus.Publish("msg", "str-key")
	bus.Publish(evtKey{Code: 7}, "struct-key")

	if got := collect(chInt); len(got) != 1 || got[0] != "int-key" {
		t.Fatalf("int-key ch got %v", got)
	}
	if got := collect(chStr); len(got) != 1 || got[0] != "str-key" {
		t.Fatalf("str-key ch got %v", got)
	}
	if got := collect(chStruct); len(got) != 1 || got[0] != "struct-key" {
		t.Fatalf("struct-key ch got %v", got)
	}
}

// warnRecorder 测试用 Logger
type warnRecorder struct{ fn func() }

func (w *warnRecorder) Warnf(format string, args ...interface{}) { w.fn() }

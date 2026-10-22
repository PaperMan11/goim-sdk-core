package transport

import (
	"sync"
	"testing"
	"time"

	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
)

func newTestRouter() *MessageRouter {
	return NewMessageRouter(&logger.NopLogger{})
}

func TestRouter_PushHandler(t *testing.T) {
	r := newTestRouter()

	got := make(chan *Response, 1)
	r.RegisterPush(WSPushMsg, func(resp *Response) {
		got <- resp
	})

	want := &Response{ReqIdentifier: WSPushMsg, Data: []byte("push-data")}
	r.Route(want)

	select {
	case resp := <-got:
		if string(resp.Data) != "push-data" {
			t.Fatalf("unexpected data: %s", resp.Data)
		}
	case <-time.After(time.Second):
		t.Fatal("push handler not called")
	}
}

func TestRouter_UnregisterPush(t *testing.T) {
	r := newTestRouter()

	called := false
	r.RegisterPush(WSPushMsg, func(resp *Response) { called = true })
	r.UnregisterPush(WSPushMsg)

	// 未命中 handler 只打 warn，不应调用
	r.Route(&Response{ReqIdentifier: WSPushMsg})
	if called {
		t.Fatal("handler should not be called after unregister")
	}
}

func TestRouter_PendingHit(t *testing.T) {
	r := newTestRouter()

	ch := r.waitResponse("op-1")
	defer r.cancelWait("op-1")

	want := &Response{ReqIdentifier: WSSendMsg, OperationID: "op-1", Data: []byte("ok")}
	r.Route(want)

	select {
	case resp := <-ch:
		if resp == nil || string(resp.Data) != "ok" {
			t.Fatalf("unexpected resp: %+v", resp)
		}
	case <-time.After(time.Second):
		t.Fatal("pending not delivered")
	}
}

func TestRouter_PendingDeliveredOnce(t *testing.T) {
	r := newTestRouter()

	r.waitResponse("op-1")
	if !r.deliverPending("op-1", &Response{}) {
		t.Fatal("first deliver should hit")
	}
	if r.deliverPending("op-1", &Response{}) {
		t.Fatal("second deliver should miss (already removed)")
	}
}

func TestRouter_PendingMissFallsToPush(t *testing.T) {
	r := newTestRouter()

	// 没有 pending，但 OperationID 非空且有 push handler → 落到 push
	got := make(chan *Response, 1)
	r.RegisterPush(WSPushMsg, func(resp *Response) { got <- resp })

	r.Route(&Response{ReqIdentifier: WSPushMsg, OperationID: "no-such-pending"})

	select {
	case <-got:
	case <-time.After(time.Second):
		t.Fatal("should fall through to push handler")
	}
}

func TestRouter_FailAllPending(t *testing.T) {
	r := newTestRouter()

	ch1 := r.waitResponse("op-1")
	ch2 := r.waitResponse("op-2")

	r.failAllPending()

	for _, ch := range []chan *Response{ch1, ch2} {
		select {
		case resp := <-ch:
			if resp != nil {
				t.Fatal("failAllPending should deliver nil")
			}
		case <-time.After(time.Second):
			t.Fatal("pending not woken up")
		}
	}
}

func TestRouter_CancelWaitIdempotent(t *testing.T) {
	r := newTestRouter()
	r.cancelWait("not-exist")
	r.cancelWait("not-exist")
}

func TestRouter_RouteNil(t *testing.T) {
	r := newTestRouter()
	r.Route(nil) // 不应 panic
}

func TestRouter_ConcurrentRouteAndRegister(t *testing.T) {
	r := newTestRouter()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func(id int) {
			defer wg.Done()
			r.RegisterPush(id, func(resp *Response) {})
		}(i)
		go func(id int) {
			defer wg.Done()
			r.Route(&Response{ReqIdentifier: id})
		}(i)
	}
	wg.Wait()
}

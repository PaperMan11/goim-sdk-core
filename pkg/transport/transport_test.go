package transport

import (
	"context"
	"testing"

	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/timex"
)

func TestConnectParams_WithDefaults(t *testing.T) {
	p := ConnectParams{}.withDefaults()
	if p.SDKType != GoSDK {
		t.Fatalf("SDKType default = %q, want %q", p.SDKType, GoSDK)
	}
	if p.Compression != "none" {
		t.Fatalf("Compression default = %q, want none", p.Compression)
	}

	p = ConnectParams{SDKType: JsSDK, Compression: "gzip"}.withDefaults()
	if p.SDKType != JsSDK || p.Compression != "gzip" {
		t.Fatal("withDefaults should not overwrite explicit values")
	}
}

func TestTransportStatus_String(t *testing.T) {
	cases := map[TransportStatus]string{
		TransportStatusDisconnected: "Disconnected",
		TransportStatusConnecting:   "Connecting",
		TransportStatusConnected:    "Connected",
		TransportStatus(99):         "Unknown",
	}
	for s, want := range cases {
		if got := s.String(); got != want {
			t.Fatalf("Status(%d).String() = %q, want %q", int(s), got, want)
		}
	}
}

func TestNotConnectedError(t *testing.T) {
	err := newNotConnectedError("websocket")
	if err.Error() != "websocket transport: not connected" {
		t.Fatalf("unexpected error message: %s", err.Error())
	}
}

// 一个 actor 主动退出 → interrupt 应触发其余 actor 的 ctx 取消
func TestBaseTransport_ActorExitTriggersOthers(t *testing.T) {
	b := newBaseTransport(&logger.NopLogger{})
	b.setState(TransportStatusConnected)

	exited := make(chan string, 2)
	b.startActors(
		func(ctx context.Context) {
			<-ctx.Done()
			exited <- "blocking"
		},
		func(ctx context.Context) {
			exited <- "immediate" // 立即返回，触发 interrupt
		},
	)

	b.groupWg.Wait()

	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case name := <-exited:
			got[name] = true
		case <-timex.After(2 * timex.Second):
			t.Fatal("actors not all exited")
		}
	}
	if !got["blocking"] || !got["immediate"] {
		t.Fatalf("missing exits: %v", got)
	}
}

// 主动 stopActors → 所有 actor 退出
func TestBaseTransport_StopActors(t *testing.T) {
	b := newBaseTransport(&logger.NopLogger{})
	b.setState(TransportStatusConnected)

	started := make(chan struct{}, 1)
	exited := make(chan struct{}, 1)
	b.startActors(func(ctx context.Context) {
		started <- struct{}{}
		<-ctx.Done()
		exited <- struct{}{}
	})

	<-started
	b.stopActors()

	select {
	case <-exited:
	case <-timex.After(2 * timex.Second):
		t.Fatal("actor not stopped")
	}
}

// 被动断开（status 仍为 Connected）应触发重连循环；
// reconnectDial 为 nil 时循环立即返回，不 panic、不泄漏
func TestBaseTransport_ActorsExited_ReconnectGuard(t *testing.T) {
	b := newBaseTransport(&logger.NopLogger{})
	b.setState(TransportStatusConnected)

	b.startActors(func(ctx context.Context) {}) // 立即退出
	b.groupWg.Wait()

	// actorsExited 已执行；reconnectDial=nil → runReconnectLoop 直接返回
	// 等一小段时间确认没有重连发生
	timex.Sleep(50 * timex.Millisecond)
	if b.isReconnecting() {
		t.Fatal("should not be reconnecting without reconnectDial")
	}
}

// 主动断开（status=Disconnected）后 actor 退出不应触发重连
func TestBaseTransport_ActorsExited_NoReconnectWhenDisconnected(t *testing.T) {
	b := newBaseTransport(&logger.NopLogger{})
	// 保持默认 Disconnected 状态

	b.startActors(func(ctx context.Context) {
		<-ctx.Done()
	})
	// startActors 末尾检测到非 Connected 会立即 stopActors
	b.groupWg.Wait()

	if b.isReconnecting() {
		t.Fatal("should not reconnect after manual disconnect")
	}
}

// actorsExited 应唤醒所有 pending 等待方
func TestBaseTransport_ActorsExited_FailsPending(t *testing.T) {
	b := newBaseTransport(&logger.NopLogger{})
	b.setState(TransportStatusConnected)

	ch := b.router.waitResponse("op-x")
	defer b.router.cancelWait("op-x")

	b.startActors(func(ctx context.Context) {}) // 立即退出 → actorsExited
	b.groupWg.Wait()

	select {
	case resp := <-ch:
		if resp != nil {
			t.Fatal("pending should receive nil on disconnect")
		}
	case <-timex.After(timex.Second):
		t.Fatal("pending not woken up")
	}
}

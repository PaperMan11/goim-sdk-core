//go:build js && wasm

package transport

// 浏览器（js/wasm）WebSocket 传输层实现。
//
// 为什么需要单独实现：
// Go 的 GOOS=js GOARCH=wasm 里 net 是进程内 fake 网络，gorilla/websocket 依赖
// net.Dial 创建真实 TCP 连接，在浏览器中必然失败（lookup localhost 走 fake DNS）。
// 浏览器唯一可用的是原生 WebSocket，因此 wasm 构建直接调用 syscall/js 封装的
// WebSocket 对象，读写流程与原生实现保持一致：
//
//	写：Request → encoder.Marshal → compressor.Compress → Uint8Array → ws.send
//	读：onmessage(ArrayBuffer) → compressor.Decompress → encoder.Unmarshal(Response) → route
//
// 心跳：浏览器原生 WebSocket 会自动回复服务端 Ping 帧，客户端无需再发 Ping。

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"syscall/js"

	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport/compressor"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport/encoder"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/timex"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/uuidx"
)

const (
	jsWSSendBufferSize  = 256
	jsWSRecvBufferSize  = 128
	jsWSSendTimeout     = 5 * timex.Second
	jsWSResponseTimeout = 15 * timex.Second
	jsWSDialTimeout     = 10 * timex.Second
)

// jsWSReadyStateOpen 浏览器 WebSocket.readyState 的 OPEN 值
const jsWSReadyStateOpen = 1

// jsWSConn 浏览器原生 WebSocket 的 Go 侧封装
//
// 生命周期：newJSWSConn 创建并挂载事件回调 → dialWS 等 opened → 使用
// send/msgCh → Close 摘掉回调并关闭连接。
type jsWSConn struct {
	ws js.Value

	opened chan struct{} // 握手成功
	done   chan struct{} // 连接结束（onclose 触发）
	msgCh  chan []byte   // 收到的原始帧（二进制）
	errCh  chan error    // 握手阶段的错误（onerror / 提前 onclose）

	openOnce   sync.Once
	doneOnce   sync.Once
	errOnce    sync.Once
	closeOnce  sync.Once
	funcs      []js.Func
	funcsValid bool
}

// newJSWSConn 创建 WebSocket 并挂载回调（不阻塞，握手结果通过 opened / errCh / done 通知）
func newJSWSConn(rawURL string) (conn *jsWSConn, err error) {
	c := &jsWSConn{
		opened: make(chan struct{}),
		done:   make(chan struct{}),
		msgCh:  make(chan []byte, jsWSRecvBufferSize),
		errCh:  make(chan error, 1),
	}

	onOpen := js.FuncOf(func(js.Value, []js.Value) any {
		c.openOnce.Do(func() { close(c.opened) })
		return nil
	})

	onMessage := js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) == 0 {
			return nil
		}
		raw := jsToBytes(args[0].Get("data"))
		if len(raw) == 0 {
			return nil
		}
		select {
		case c.msgCh <- raw:
		default:
			// 接收缓冲满（正常不会发生，浏览器读循环足够快）：丢弃该帧避免阻塞事件循环
		}
		return nil
	})

	onError := js.FuncOf(func(_ js.Value, args []js.Value) any {
		msg := "websocket error"
		if len(args) > 0 {
			if m := args[0].Get("message"); m.Type() == js.TypeString && m.String() != "" {
				msg = "websocket error: " + m.String()
			}
		}
		c.fail(errors.New(msg))
		return nil
	})

	onClose := js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) > 0 {
			code := args[0].Get("code").Int()
			reason := args[0].Get("reason").String()
			c.fail(fmt.Errorf("websocket closed, code=%d reason=%s", code, reason))
		}
		c.markDone()
		return nil
	})

	c.funcs = []js.Func{onOpen, onMessage, onError, onClose}
	c.funcsValid = true

	// WebSocket 构造可能抛异常（非法 URL 等），转成 error 返回
	defer func() {
		if r := recover(); r != nil {
			c.releaseFuncs()
			conn, err = nil, fmt.Errorf("new websocket: %v", r)
		}
	}()

	c.ws = js.Global().Get("WebSocket").New(rawURL)
	c.ws.Set("binaryType", "arraybuffer")
	c.ws.Set("onopen", onOpen)
	c.ws.Set("onmessage", onMessage)
	c.ws.Set("onerror", onError)
	c.ws.Set("onclose", onClose)

	return c, nil
}

// fail 记录握手阶段错误（仅第一次生效；握手完成后不再有人消费，直接丢弃）
func (c *jsWSConn) fail(err error) {
	c.errOnce.Do(func() {
		select {
		case <-c.opened:
			return // 已握手成功，错误只体现在 onclose → done
		default:
		}
		select {
		case c.errCh <- err:
		default:
		}
	})
}

func (c *jsWSConn) markDone() {
	c.doneOnce.Do(func() { close(c.done) })
}

// send 发送二进制帧（Uint8Array）
func (c *jsWSConn) send(payload []byte) (err error) {
	if c.ws.Type() == js.TypeUndefined {
		return newNotConnectedError("websocket")
	}
	if rs := c.ws.Get("readyState").Int(); rs != jsWSReadyStateOpen {
		return fmt.Errorf("websocket not open, readyState=%d", rs)
	}

	// JS 侧异常不能直接 panic 掉 Go 运行时
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("websocket send: %v", r)
		}
	}()

	u8 := js.Global().Get("Uint8Array").New(len(payload))
	js.CopyBytesToJS(u8, payload)
	c.ws.Call("send", u8)
	return nil
}

// Close 关闭连接（幂等）：先摘掉回调，再关闭 socket，最后释放 js.Func
func (c *jsWSConn) Close() {
	c.closeOnce.Do(func() {
		if c.funcsValid {
			c.ws.Set("onopen", js.Null())
			c.ws.Set("onmessage", js.Null())
			c.ws.Set("onerror", js.Null())
			c.ws.Set("onclose", js.Null())
			c.releaseFuncs()
		}

		c.markDone()

		if c.ws.Type() != js.TypeUndefined {
			func() {
				defer func() { _ = recover() }()
				c.ws.Call("close")
			}()
		}
	})
}

func (c *jsWSConn) releaseFuncs() {
	if !c.funcsValid {
		return
	}
	c.funcsValid = false
	for _, f := range c.funcs {
		f.Release()
	}
	c.funcs = nil
}

// jsToBytes 把 JS 侧帧数据（ArrayBuffer / TypedArray / string）转成 Go []byte
func jsToBytes(v js.Value) []byte {
	switch v.Type() {
	case js.TypeString:
		return []byte(v.String())
	case js.TypeObject:
		u8 := js.Global().Get("Uint8Array").New(v)
		n := u8.Get("length").Int()
		if n == 0 {
			return nil
		}
		buf := make([]byte, n)
		js.CopyBytesToGo(buf, u8)
		return buf
	default:
		return nil
	}
}

// DefaultWSClient js/wasm 下的 WebSocket 传输层实现（对外行为与 ws.go 原生实现一致）
type DefaultWSClient struct {
	baseTransport

	wsURL  string
	params ConnectParams

	// 写通道：SendRequest 只投递压缩后的 raw bytes，由 writeLoop 串行写入浏览器 WebSocket
	sendCh chan []byte

	connMu sync.RWMutex
	conn   *jsWSConn
}

var _ MessageTransport = (*DefaultWSClient)(nil)

// NewWSClient 创建 DefaultWSClient（js/wasm 版本）
func NewWSClient(wsURL string, params ConnectParams, l logger.Logger) *DefaultWSClient {
	params = params.withDefaults()
	if params.OperationID == "" {
		params.OperationID = uuidx.NewOperationID("connect")
	}

	w := &DefaultWSClient{
		baseTransport: newBaseTransport(l),
		wsURL:         wsURL,
		params:        params,
		sendCh:        make(chan []byte, jsWSSendBufferSize),
	}
	w.onActorExit = w.closeConn
	return w
}

// SetConnectParams 运行时更新 ConnectParams（实现 MessageTransport interface）
func (w *DefaultWSClient) SetConnectParams(p ConnectParams) {
	w.params = p.withDefaults()
}

func (w *DefaultWSClient) getEncoder() encoder.Encoder {
	return encoder.GetEncoder(w.params.SDKType)
}

func (w *DefaultWSClient) getCompressor() compressor.Compressor {
	return compressor.GetCompressor(w.params.Compression)
}

// Connect 建立 WebSocket 连接（实现 MessageTransport）
func (w *DefaultWSClient) Connect(ctx context.Context) error {
	if w.IsConnected() {
		return nil
	}
	w.setState(TransportStatusConnecting)

	// 注入重连所需的 dial 函数和 onConnected 回调（与原生实现一致）
	w.reconnectTarget = w.wsURL
	w.reconnectDial = func(ctx context.Context) error {
		conn, err := w.dialWS(ctx)
		if err != nil {
			return err
		}
		w.setConn(conn)
		return nil
	}
	w.reconnectOnConnected = func() {
		w.startActors(w.readLoop, w.writeLoop)
	}

	conn, err := w.dialWS(ctx)
	if err != nil {
		w.setState(TransportStatusDisconnected)
		return err
	}
	w.setConn(conn)
	w.setState(TransportStatusConnected)

	w.logger.Infof("websocket connected, url=%s", w.wsURL)

	w.startActors(w.readLoop, w.writeLoop)
	return nil
}

// Disconnect 断开连接
func (w *DefaultWSClient) Disconnect() error {
	if w.Status() == TransportStatusDisconnected && !w.isReconnecting() {
		return nil
	}

	w.mu.RLock()
	wasConnected := w.status == TransportStatusConnected
	w.mu.RUnlock()

	// 先标记断开状态 → actorsExited 会看到 Disconnected 状态，跳过重连
	w.setState(TransportStatusDisconnected)
	w.stopReconnect()
	// stopActors 内部会通过 onActorExit 关闭连接，解除 readLoop 的阻塞
	w.stopActors()

	if wasConnected && w.OnDisconnected != nil {
		w.OnDisconnected()
	}

	w.logger.Infof("websocket disconnected")
	return nil
}

// SendRequestAndWait 发送请求并同步等待响应（对齐服务端请求-响应模型）
func (w *DefaultWSClient) SendRequestAndWait(ctx context.Context, req *Request) (*Response, error) {
	if req == nil {
		return nil, fmt.Errorf("request is nil")
	}
	if req.OperationID == "" {
		req.OperationID = uuidx.NewOperationID(reqIDOpName(req.ReqIdentifier))
	}

	ch := w.router.waitResponse(req.OperationID)
	defer w.router.cancelWait(req.OperationID)

	if err := w.SendRequest(ctx, req); err != nil {
		return nil, err
	}

	select {
	case resp := <-ch:
		if resp == nil {
			return nil, newNotConnectedError("websocket")
		}
		if resp.ErrCode != 0 {
			return nil, model.NewSDKError(int32(resp.ErrCode), resp.ErrMsg)
		}
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timex.After(jsWSResponseTimeout):
		return nil, fmt.Errorf("wait response timeout, operationID=%s", req.OperationID)
	}
}

// SendRequest 发送 Request 协议包：encoder.Marshal → compressor.Compress → sendCh
func (w *DefaultWSClient) SendRequest(ctx context.Context, req *Request) error {
	if req == nil {
		return fmt.Errorf("request is nil")
	}
	if !w.IsConnected() {
		return newNotConnectedError("websocket")
	}

	encodeData, err := w.getEncoder().Marshal(req)
	if err != nil {
		w.logger.Errorf("websocket marshal request error: %v", err)
		return fmt.Errorf("marshal request: %w", err)
	}

	compressed, err := w.getCompressor().Compress(encodeData)
	if err != nil {
		w.logger.Errorf("websocket compress request error: %v", err)
		return fmt.Errorf("compress request: %w", err)
	}

	return w.deliver(ctx, compressed)
}

// deliver 投递压缩后的 bytes 到写通道，带 ctx 取消和超时
func (w *DefaultWSClient) deliver(ctx context.Context, payload []byte) error {
	select {
	case w.sendCh <- payload:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timex.After(jsWSSendTimeout):
		w.logger.Errorf("websocket send timeout")
		return fmt.Errorf("send message timeout")
	}
}

// === WebSocket 私有实现 ===

// dialWS 用浏览器原生 WebSocket 拨号，认证参数拼到 URL query
func (w *DefaultWSClient) dialWS(ctx context.Context) (*jsWSConn, error) {
	u, err := url.Parse(w.wsURL)
	if err != nil {
		return nil, fmt.Errorf("parse ws url: %w", err)
	}
	u.RawQuery = encodeQuery(w.params).Encode()

	conn, err := newJSWSConn(u.String())
	if err != nil {
		return nil, err
	}

	// 等待握手结果：成功 / 错误 / 提前关闭 / ctx 取消 / 超时
	select {
	case <-conn.opened:
		w.closeConn()
		return conn, nil
	case err := <-conn.errCh:
		conn.Close()
		return nil, err
	case <-conn.done:
		conn.Close()
		return nil, fmt.Errorf("websocket closed before open")
	case <-ctx.Done():
		conn.Close()
		return nil, ctx.Err()
	case <-timex.After(jsWSDialTimeout):
		conn.Close()
		return nil, fmt.Errorf("websocket connect timeout")
	}
}

func (w *DefaultWSClient) setConn(conn *jsWSConn) {
	w.connMu.Lock()
	w.conn = conn
	w.connMu.Unlock()
}

func (w *DefaultWSClient) currentConn() *jsWSConn {
	w.connMu.RLock()
	defer w.connMu.RUnlock()
	return w.conn
}

// closeConn 关闭当前连接（幂等，可被多个 goroutine 并发调用）
func (w *DefaultWSClient) closeConn() {
	w.connMu.Lock()
	conn := w.conn
	w.conn = nil
	w.connMu.Unlock()

	if conn != nil {
		conn.Close()
	}
}

// readLoop 读循环：msgCh → Decompress → Unmarshal(Response) → route
func (w *DefaultWSClient) readLoop(ctx context.Context) {
	conn := w.currentConn()
	if conn == nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-conn.done:
			// 连接结束：把剩余已入队的帧处理完再退出
			for {
				select {
				case raw := <-conn.msgCh:
					w.handleRaw(raw)
				default:
					return
				}
			}
		case raw := <-conn.msgCh:
			w.handleRaw(raw)
		}
	}
}

func (w *DefaultWSClient) handleRaw(raw []byte) {
	decompressed, err := w.getCompressor().Decompress(raw)
	if err != nil {
		w.logger.Warnf("websocket decompress failed, error=%v", err)
		return
	}

	resp := &Response{}
	if err := w.getEncoder().Unmarshal(decompressed, resp); err != nil {
		w.logger.Warnf("websocket unmarshal response failed, error=%v, raw=%s", err, decompressed)
		return
	}

	w.routeMessage(resp)
}

// writeLoop 写循环 —— 唯一允许写 socket 的 actor
//
// 浏览器会自动回复服务端 Ping，这里不需要自发心跳。
func (w *DefaultWSClient) writeLoop(ctx context.Context) {
	conn := w.currentConn()
	if conn == nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case payload := <-w.sendCh:
			if err := conn.send(payload); err != nil {
				if ctx.Err() == nil {
					w.logger.Warnf("websocket write failed, error=%v", err)
				}
				return // actor 退出 → 关闭连接 → 重连
			}
		}
	}
}

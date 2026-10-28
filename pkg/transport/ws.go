package transport

import (
	"context"
	"fmt"
	"net/url"
	"sync"

	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport/compressor"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport/encoder"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/convert"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/timex"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/uuidx"
	"github.com/gorilla/websocket"
)

const (
	wsHeartbeatInterval = 30 * timex.Second
	wsPongWait          = 10 * timex.Second
	wsWriteTimeout      = 10 * timex.Second
	wsSendBufferSize    = 256
	wsSendTimeout       = 5 * timex.Second
	wsResponseTimeout   = 15 * timex.Second
)

// encodeQuery 把认证参数编码为 URL query（WS 特有，与服务端字段名逐一对齐）
func encodeQuery(p ConnectParams) url.Values {
	q := url.Values{}
	q.Set("userID", p.UserID)
	q.Set("token", p.Token)
	q.Set("platformID", convert.ToString(p.PlatformID))
	q.Set("deviceID", p.DeviceID)
	q.Set("operationID", p.OperationID)
	q.Set("sendResponse", convert.ToString(p.SendResponse))
	q.Set("isBackground", convert.ToString(p.IsBackground))
	q.Set("compression", p.Compression)
	q.Set("sdkType", p.SDKType)
	q.Set("sdkVersion", p.SDKVersion)
	return q
}

// DefaultWSClient WebSocket 传输层实现（读写分离 + encoder/compressor + 消息路由）
//
// 写流程（完全对齐服务端 SendResponse）：
//
//	proto.Marshal(business) → Request.Data → encoder.Marshal(Request) → compressor.Compress → sendCh
//
// 读流程（完全对齐服务端 WsConnection 读流程）：
//
//	conn.ReadMessage → compressor.Decompress → encoder.Unmarshal(Response) → MessageRouter.Route
//
// 两条流程中间的协议包（Request / Response）和服务端定义逐字段对应。
type DefaultWSClient struct {
	baseTransport

	wsURL  string
	params ConnectParams
	dialer *websocket.Dialer

	// 写通道：SendRequest 只投递压缩后的 raw bytes，不做实际网络写
	sendCh chan []byte

	connMu sync.RWMutex
	conn   *websocket.Conn
}

var _ MessageTransport = (*DefaultWSClient)(nil)

// NewWSClient 创建 DefaultWSClient
// wsURL 为基础地址（如 ws://host:10001/ws），认证参数由 params 提供
func NewWSClient(wsURL string, params ConnectParams, l logger.Logger) *DefaultWSClient {
	params = params.withDefaults()
	if params.OperationID == "" {
		params.OperationID = uuidx.NewOperationID("connect")
	}

	w := &DefaultWSClient{
		baseTransport: newBaseTransport(l),
		wsURL:         wsURL,
		params:        params,
		dialer:        websocket.DefaultDialer,
		sendCh:        make(chan []byte, wsSendBufferSize),
	}
	w.onActorExit = w.closeConn
	return w
}

// SetConnectParams 运行时更新 ConnectParams（实现 MessageTransport interface）
// 建议在 Connect 之前调用；连接过程中调用可能导致认证参数不一致
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

	// 注入重连所需的 dial 函数和 onConnected 回调
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
	// 先标记断开状态 → actorsExited 会看到 Disconnected 状态，跳过重连
	w.setState(TransportStatusDisconnected)
	// 取消可能正在进行中的重连循环
	w.stopReconnect()
	// stopActors 内部会通过 onActorExit 关闭连接，解除 readLoop 的阻塞读
	w.stopActors()

	w.logger.Infof("websocket disconnected")
	return nil
}

// SendRequestAndWait 发送请求并同步等待响应（对齐服务端请求-响应模型）
//
// 流程：注册 pending（按 OperationID）→ SendRequest → 等 Response / ctx 取消 / 超时 / 连接断开。
// 返回的 *Response 由 GC 管理，调用方无需释放。
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
			// 连接断开被 failAllPending 唤醒
			return nil, newNotConnectedError("websocket")
		}
		if resp.ErrCode != 0 {
			return nil, model.NewSDKError(int32(resp.ErrCode), resp.ErrMsg)
		}
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timex.After(wsResponseTimeout):
		return nil, fmt.Errorf("wait response timeout, operationID=%s", req.OperationID)
	}
}

// SendRequest 发送 Request 协议包（完全对齐服务端 SendResponse）
//
//	request → encoder.Marshal → compressor.Compress → sendCh → writeLoop → conn.WriteMessage
//
// 调用方负责把 proto.Marshal(业务对象) 的结果放进 Request.Data。
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
	case <-timex.After(wsSendTimeout):
		w.logger.Errorf("websocket send timeout")
		return fmt.Errorf("send message timeout")
	}
}

// === WebSocket 私有实现 ===

// dialWS gorilla/websocket 拨号，认证参数拼到 URL query（新连接建立后关闭旧连接）
func (w *DefaultWSClient) dialWS(ctx context.Context) (*websocket.Conn, error) {
	u, err := url.Parse(w.wsURL)
	if err != nil {
		return nil, fmt.Errorf("parse ws url: %w", err)
	}
	u.RawQuery = encodeQuery(w.params).Encode()

	conn, _, err := w.dialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		return nil, err
	}

	w.closeConn()
	return conn, nil
}

func (w *DefaultWSClient) setConn(conn *websocket.Conn) {
	w.connMu.Lock()
	w.conn = conn
	w.connMu.Unlock()
}

func (w *DefaultWSClient) currentConn() *websocket.Conn {
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
		_ = conn.Close()
	}
}

// readLoop 读循环（对齐服务端反向流程：ReadMessage → Decompress → Unmarshal(Response) → resolver → dispatch）
func (w *DefaultWSClient) readLoop(ctx context.Context) {
	conn := w.currentConn()
	if conn == nil {
		return
	}

	// 收到服务端 ping → 回复 pong + 续期读超时
	conn.SetPingHandler(func(appData string) error {
		_ = conn.SetReadDeadline(timex.Now().Add(wsHeartbeatInterval + wsPongWait))
		_ = conn.SetWriteDeadline(timex.Now().Add(wsWriteTimeout))
		return conn.WriteControl(websocket.PongMessage, convert.ToBytes(appData), timex.Now().Add(wsWriteTimeout))
	})
	// 收到服务端 pong（对我们 ping 的回复） → 续期读超时
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(timex.Now().Add(wsHeartbeatInterval + wsPongWait))
	})

	_ = conn.SetReadDeadline(timex.Now().Add(wsHeartbeatInterval + wsPongWait))

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, raw, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() == nil {
				w.logger.Warnf("websocket read failed, error=%v", err)
			}
			return
		}

		// 反向流程（完全对齐服务端 SendResponse 逆操作）：
		// Decompress → Unmarshal(Response) → resolver(Response.Data) → Event → dispatch
		decompressed, err := w.getCompressor().Decompress(raw)
		if err != nil {
			w.logger.Warnf("websocket decompress failed, error=%v", err)
			continue
		}

		resp := &Response{}
		if err := w.getEncoder().Unmarshal(decompressed, resp); err != nil {
			w.logger.Warnf("websocket unmarshal response failed, error=%v, raw=%s", err, decompressed)
			continue
		}

		// 路由：pending 匹配（同步响应）/ push handler（服务端推送）
		w.routeMessage(resp)
	}
}

// writeLoop 写循环 —— 唯一允许写 socket 的 goroutine
// 串行处理业务消息与心跳 Ping，避免并发写
func (w *DefaultWSClient) writeLoop(ctx context.Context) {
	conn := w.currentConn()
	if conn == nil {
		return
	}

	ticker := timex.NewTicker(wsHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case payload := <-w.sendCh:
			if err := w.writeMessage(conn, payload); err != nil {
				return // actor 退出 → 关闭连接 → 重连
			}
		case <-ticker.C:
			if err := conn.WriteControl(
				websocket.PingMessage,
				convert.ToBytes("ping"),
				timex.Now().Add(wsWriteTimeout),
			); err != nil {
				if ctx.Err() == nil {
					w.logger.Warnf("websocket ping failed, error=%v", err)
				}
				return
			}
		}
	}
}

func (w *DefaultWSClient) writeMessage(conn *websocket.Conn, payload []byte) error {
	_ = conn.SetWriteDeadline(timex.Now().Add(wsWriteTimeout))
	if err := conn.WriteMessage(websocket.BinaryMessage, payload); err != nil {
		w.logger.Warnf("websocket write failed, error=%v", err)
		return err
	}
	return nil
}

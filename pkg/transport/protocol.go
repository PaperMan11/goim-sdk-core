// Package protocol 对齐服务端传输层协议包定义。
//
// 链路（外 → 内）：
//
//	ReadMessage → compressor.Decompress → encoder.Unmarshal(Request/Response) → proto.Unmarshal(Data)
//	WriteMessage ← compressor.Compress ← encoder.Marshal(Request/Response) ← proto.Marshal(业务对象) → Data
package transport

// SDK类型常量定义
const (
	GoSDK = "go" // Go语言SDK
	JsSDK = "js" // JavaScript SDK
)

// WebSocket协议消息类型常量定义
const (
	// WSGetNewestSeq 获取最新消息序列号
	WSGetNewestSeq = 1001
	// WSPullMsgBySeqList 按序列号列表拉取消息
	WSPullMsgBySeqList = 1002
	// WSSendMsg 发送消息
	WSSendMsg = 1003
	// WSSendSignalMsg 发送信号消息
	WSSendSignalMsg = 1004
	// WSPullMsg 拉取消息
	WSPullMsg = 1005
	// WSGetConvMaxReadSeq 获取会话最大已读序列号
	WSGetConvMaxReadSeq = 1006
	// WsPullConvLastMessage 拉取会话最后一条消息
	WsPullConvLastMessage = 1007
	// WSPushMsg 推送消息（服务端主动推送）
	WSPushMsg = 2001
	// WSKickOnlineMsg 踢下线消息
	WSKickOnlineMsg = 2002
	// WSLogoutMsg 登出消息
	WSLogoutMsg = 2003
	// WSSetBackgroundStatus 设置后台状态
	WSSetBackgroundStatus = 2004
	// WSSubUserOnlineStatus 订阅用户在线状态
	WSSubUserOnlineStatus = 2005
	// WSDataError 数据错误消息
	WSDataError = 3001
)

// Request 客户端向服务端发送的请求包（对齐服务端定义）
type Request struct {
	ReqIdentifier int    `json:"reqIdentifier"`
	Token         string `json:"token"`
	SenderID      string `json:"senderID"`
	OperationID   string `json:"operationID"`
	MsgIncr       string `json:"msgIncr"`
	Data          []byte `json:"data"` // proto.Marshal(业务对象) 的结果
}

// Response 服务端返回的响应包 / 服务端主动推送的事件包（对齐服务端定义）
//
// 注意：Response 既用于同步 Response（ErrCode != 0 表示失败），
// 也用于推送事件（ReqIdentifier 为 0，ErrCode 为 0，Data 为事件 proto 消息）。
//
// 生命周期：普通 GC 管理，无对象池。客户端单连接消息量小，池化收益为零，
// 反而引入所有权 / use-after-free 风险，故不用 sync.Pool。
type Response struct {
	ReqIdentifier int    `json:"reqIdentifier"`
	MsgIncr       string `json:"msgIncr"`
	OperationID   string `json:"operationID"`
	ErrCode       int    `json:"errCode"`
	ErrMsg        string `json:"errMsg"`
	Data          []byte `json:"data"` // proto.Marshal(业务对象/事件消息) 的结果
}

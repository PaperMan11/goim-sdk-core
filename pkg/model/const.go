package model

// SessionType
const (
	SessionTypeC2C   = 1 // 单聊
	SessionTypeGroup = 3 // 读扩散群聊
)

// MsgFrom
const (
	MsgFromUser   = 100 // 用户消息
	MsgFromSystem = 200 // 系统通知
)

// RecvMsgOpt / GlobalRecvMsgOpt
const (
	RecvMsg          = 0 // 接收
	NotRecvMsg       = 1 // 不接收
	RecvMsgNotNotify = 2 // 接收不推送
)

// RoleLevel (群成员角色)
const (
	RoleMember = 10
	RoleAdmin  = 20
	RoleOwner  = 50
)

// PullOrder
const (
	PullOrderAsc  = 0 // 升序
	PullOrderDesc = 1 // 降序
)

// TokenStatus
const (
	NormalToken  = 0
	InValidToken = 1
	KickedToken  = 2
	ExpiredToken = 3
)

// HandleResult (好友申请/入群申请)
const (
	HandleResultUnprocessed = 0
	HandleResultApproved    = 1
	HandleResultRejected    = -1
)

// Message status
const (
	MsgStatusSuccess = 1
	MsgStatusSending = 2
	MsgStatusFailed  = 3
)

// DeleteSyncOpt flag
const (
	SyncSelf  = true
	SyncOther = false
)

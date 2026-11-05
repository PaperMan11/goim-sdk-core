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

// RoleLevel (群成员角色，对齐服务端 constant.GroupOrdinaryUsers/GroupAdmin/GroupOwner)
const (
	RoleMember = 20  // GroupOrdinaryUsers 群普通成员
	RoleAdmin  = 60  // GroupAdmin 群管理员
	RoleOwner  = 100 // GroupOwner 群主
)

// GroupStatus (群状态，对应 GroupInfo.Status)
const (
	GroupOk              = 0 // 正常
	GroupBanChat         = 1 // 禁止发言
	GroupStatusDismissed = 2 // 已解散
	GroupStatusMuted     = 3 // 已禁言
	GroupBaned           = 3 // 已封禁（服务端历史字段，与 Muted 值相同）
	GroupBanPrivateChat  = 4 // 禁止私聊
)

// GroupIsBanChat 检查群组是否被禁言
func GroupIsBanChat(status int32) bool {
	return status == GroupStatusMuted
}

// GroupIsBanPrivateChat 检查是否禁止私聊
func GroupIsBanPrivateChat(status int32) bool {
	return status == GroupBanPrivateChat
}

// GroupType (群类型，对应 GroupInfo.GroupType)
const (
	NormalGroup  = 0 // 普通群
	SuperGroup   = 1 // 超级群
	WorkingGroup = 2 // 工作群
)

// JoinSource (入群来源，对应 GroupMemberFullInfo.JoinSource / JoinGroup 请求 joinSource)
const (
	JoinByAdmin      = 1 // 管理员添加
	JoinByInvitation = 2 // 邀请加入
	JoinBySearch     = 3 // 搜索加入
	JoinByQRCode     = 4 // 扫码加入
)

// GroupResponse (群组申请响应)
const (
	GroupResponseAgree  = 1  // 同意
	GroupResponseRefuse = -1 // 拒绝
)

// GroupSearchPosition (群搜索位置)
const (
	GroupSearchPositionHead = 1 // 头部
	GroupSearchPositionAny  = 2 // 任意位置
)

// GroupRPCBatchSize (群组 RPC 消息批量大小)
const (
	GroupRPCRecvSize = 30
	GroupRPCSendSize = 30
)

// NeedVerification (群入群验证方式，对应 GroupInfo.NeedVerification)
const (
	ApplyNeedVerificationInviteDirectly = 0 // 申请需要验证，邀请直接加入
	AllNeedVerification                 = 1 // 所有人都需要验证
	Directly                            = 2 // 直接加入，无需验证
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
	MsgStatusSuccess int32 = 1
	MsgStatusSending int32 = 2
	MsgStatusFailed  int32 = 3
	MsgStatusRevoked int32 = 4 // 已撤回（2101 撤回通知的本地缓存标记）
)

// DeleteSyncOpt flag
const (
	SyncSelf  = true
	SyncOther = false
)

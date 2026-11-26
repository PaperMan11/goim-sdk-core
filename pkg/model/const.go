package model

import pbconstant "github.com/PaperMan11/goim/pkg/protocol/constant"

// SessionType（引用协议 constant.SingleChatType / ReadGroupChatType）
const (
	SessionTypeC2C   = pbconstant.SingleChatType    // 单聊
	SessionTypeGroup = pbconstant.ReadGroupChatType // 读扩散群聊
)

// MsgFrom（引用协议 constant.UserMsgType / SysMsgType）
const (
	MsgFromUser   = pbconstant.UserMsgType // 用户消息
	MsgFromSystem = pbconstant.SysMsgType  // 系统通知
)

// RecvMsgOpt / GlobalRecvMsgOpt（引用协议 constant.ReceiveMessage 等）
const (
	RecvMsg          = pbconstant.ReceiveMessage          // 接收
	NotRecvMsg       = pbconstant.NotReceiveMessage       // 不接收
	RecvMsgNotNotify = pbconstant.ReceiveNotNotifyMessage // 接收不推送
)

// RoleLevel（群成员角色，引用协议 constant.GroupOrdinaryUsers/GroupAdmin/GroupOwner）
const (
	RoleMember = pbconstant.GroupOrdinaryUsers // 群普通成员
	RoleAdmin  = pbconstant.GroupAdmin         // 群管理员
	RoleOwner  = pbconstant.GroupOwner         // 群主
)

// GroupStatus（群状态，引用协议 constant.Group*）
const (
	GroupOk              = pbconstant.GroupOk              // 正常
	GroupBanChat         = pbconstant.GroupBanChat         // 禁止发言
	GroupStatusDismissed = pbconstant.GroupStatusDismissed // 已解散
	GroupStatusMuted     = pbconstant.GroupStatusMuted     // 已禁言
	GroupBaned           = pbconstant.GroupBaned           // 已封禁（服务端历史字段，与 Muted 值相同）
	GroupBanPrivateChat  = pbconstant.GroupBanPrivateChat  // 禁止私聊
)

// GroupIsBanChat 检查群组是否被禁言
func GroupIsBanChat(status int32) bool {
	return status == GroupStatusMuted
}

// GroupIsBanPrivateChat 检查是否禁止私聊
func GroupIsBanPrivateChat(status int32) bool {
	return status == GroupBanPrivateChat
}

// GroupType（群类型，引用协议 constant.NormalGroup 等）
const (
	NormalGroup  = pbconstant.NormalGroup  // 普通群
	SuperGroup   = pbconstant.SuperGroup   // 超级群
	WorkingGroup = pbconstant.WorkingGroup // 工作群
)

// JoinSource（入群来源，引用协议 constant.JoinBy*）
const (
	JoinByAdmin      = pbconstant.JoinByAdmin      // 管理员添加
	JoinByInvitation = pbconstant.JoinByInvitation // 邀请加入
	JoinBySearch     = pbconstant.JoinBySearch     // 搜索加入
	JoinByQRCode     = pbconstant.JoinByQRCode     // 扫码加入
)

// GroupResponse（群组申请响应，引用协议 constant.GroupResponseAgree/Refuse）
const (
	GroupResponseAgree  = pbconstant.GroupResponseAgree  // 同意
	GroupResponseRefuse = pbconstant.GroupResponseRefuse // 拒绝
)

// GroupSearchPosition（群搜索位置，引用协议 constant.GroupSearchPosition*）
const (
	GroupSearchPositionHead = pbconstant.GroupSearchPositionHead // 头部
	GroupSearchPositionAny  = pbconstant.GroupSearchPositionAny  // 任意位置
)

// GroupRPCBatchSize（群组 RPC 消息批量大小，引用协议 constant.GroupRPC*Size）
const (
	GroupRPCRecvSize = pbconstant.GroupRPCRecvSize
	GroupRPCSendSize = pbconstant.GroupRPCSendSize
)

// NeedVerification（群入群验证方式，引用协议 constant.ApplyNeedVerificationInviteDirectly 等）
const (
	ApplyNeedVerificationInviteDirectly = pbconstant.ApplyNeedVerificationInviteDirectly // 申请需要验证，邀请直接加入
	AllNeedVerification                 = pbconstant.AllNeedVerification                 // 所有人都需要验证
	Directly                            = pbconstant.Directly                            // 直接加入，无需验证
)

// PullOrder（SDK 拉取排序，协议无对应常量）
const (
	PullOrderAsc  = 0 // 升序
	PullOrderDesc = 1 // 降序
)

// TokenStatus（引用协议 constant.NormalToken 等）
const (
	NormalToken  = pbconstant.NormalToken  // 正常
	InValidToken = pbconstant.InValidToken // 无效
	KickedToken  = pbconstant.KickedToken  // 被踢
	ExpiredToken = pbconstant.ExpiredToken // 过期
)

// HandleResult（好友/入群申请处理结果，引用协议 constant.FriendResponse*）
const (
	HandleResultUnprocessed = pbconstant.FriendResponseNotHandle // 未处理
	HandleResultApproved    = pbconstant.FriendResponseAgree     // 同意
	HandleResultRejected    = pbconstant.FriendResponseRefuse    // 拒绝
)

// Message status（引用协议 constant.MsgStatus*，对齐 docs/protocol-message.md §1）
const (
	MsgStatusSending  int32 = pbconstant.MsgStatusSending     // 发送中
	MsgStatusSuccess  int32 = pbconstant.MsgStatusSendSuccess // 发送成功
	MsgStatusFailed   int32 = pbconstant.MsgStatusSendFailed  // 发送失败
	MsgStatusDeleted  int32 = pbconstant.MsgStatusHasDeleted  // 已删除
	MsgStatusFiltered int32 = pbconstant.MsgStatusFiltered    // 已过滤
)

// MsgStatusRevoked 撤回通知的本地缓存标记，语义等同于已删除（UI 展示「已撤回」）
const MsgStatusRevoked = MsgStatusDeleted

// ContentTypeRevoke 撤回消息（引用协议常量 constant.Revoke，协议调整时自动跟随）。
// 仅用于会话摘要：最新一条被撤回时摘要改写为该类型，宿主据此展示「已撤回」。
const ContentTypeRevoke int32 = pbconstant.Revoke

// DeleteSyncOpt flag（SDK 本地开关，协议无对应常量）
const (
	SyncSelf  = true
	SyncOther = false
)

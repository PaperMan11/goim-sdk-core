package events

// EventType WS 推送事件类型
type EventType int32

const (
	EventNewMsg                   EventType = iota + 1 // 新消息
	EventRevokeMsg                                     // 消息撤回
	EventDeleteMsgs                                    // 消息删除
	EventMarkAsRead                                    // 已读回执
	EventConversationUpdate                            // 会话属性更新
	EventConversationDelete                            // 会话删除
	EventFriendApplication                             // 好友申请
	EventFriendAdded                                   // 好友添加成功
	EventFriendDeleted                                 // 好友删除
	EventFriendInfoChanged                             // 好友信息变更
	EventBlackAdded                                    // 黑名单添加
	EventBlackDeleted                                  // 黑名单删除
	EventGroupCreated                                  // 群组创建
	EventGroupInfoSet                                  // 群信息变更
	EventJoinGroupApplication                          // 入群申请
	EventGroupApplicationAccepted                      // 入群申请通过
	EventGroupApplicationRejected                      // 入群申请拒绝
	EventMemberEnter                                   // 成员主动加入
	EventMemberInvited                                 // 成员被邀请
	EventMemberQuit                                    // 成员主动退出
	EventMemberKicked                                  // 成员被踢出
	EventGroupOwnerTransferred                         // 群主转让
	EventGroupDismissed                                // 群组解散
	EventGroupMemberMuted                              // 群成员禁言
	EventGroupMemberCancelMuted                        // 群成员解禁
	EventGroupMuted                                    // 全员禁言
	EventGroupCancelMuted                              // 取消全员禁言
	EventUserInfoUpdated                               // 用户信息更新
	EventUserStatusChange                              // 在线状态变更
	EventForceLogout                                   // Token 被踢下线
)

// Event WS 事件通用封装
//
// 完整的三层链路：
//  ProtoResp.Data（proto bytes） → ProtoResolver 判定 EventType → SDK 分发
type Event struct {
	Type       EventType   `json:"type"`
	Payload    interface{} `json:"-"`       // 可选：已反序列化的 proto.Message（上层注入 resolver 后可自动填）
	RawPayload []byte      `json:"payload"` // 原始 proto bytes（Response.Data 内容）
	Timestamp  int64       `json:"timestamp"`
}

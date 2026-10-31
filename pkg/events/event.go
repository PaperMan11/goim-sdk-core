package events

// EventType 事件类型
//
// 三类事件共用这个枚举：
//  1. WS 推送（30 个，由 MessageRouter.PushHandler → Publish）
//  2. 传输层生命周期（EventConnect / EventDisconnect / EventReconnect）
//  3. Manager 操作结果（EventLoginSuccess / EventMsgSent / EventGroupCreated ...）
type EventType int32

const (
	// —— WS 推送 ——
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
	EventBackgroundStatusChanged                       // 前后台状态变更

	// —— 传输层生命周期 ——
	EventConnect    // WS 连接成功
	EventConnecting // WS 连接中（首次连接 / 重连循环 dial 前）
	EventDisconnect // WS 连接断开（被动，含重连前）
	EventReconnect  // 自动重连成功

	// —— 认证 ——
	EventLoginSuccess   // 登录成功（Token 获取 + WS 连接）
	EventLoginFailed    // 登录失败
	EventTokenRefreshed // Token 刷新成功

	// —— 同步引擎 ——
	EventSyncStart            // 登录/重连触发的后台数据同步开始（同步与登录解耦，独立于登录结果）
	EventSyncFinish           // 后台数据同步完成（本地数据就绪，UI 可全量刷新）
	EventSyncFailed           // 后台数据同步失败（Payload: error；下轮重连或通知触发自动重试）
	EventSeqSynced            // 消息 Seq gap 拉取完成
	EventFriendListSynced     // 好友列表本地缓存已更新（增量/全量同步完成，可刷新 UI）
	EventBlackListSynced      // 黑名单本地缓存已更新
	EventGroupListSynced      // 加入群列表本地缓存已更新
	EventGroupMemberSynced    // 群成员本地缓存已更新（Payload: *model.IncrementalMembers 携带 GroupID）
	EventConversationSynced   // 会话列表本地缓存已更新

	// —— Manager 操作结果 ——
	EventMsgSent             // 消息发送成功
	EventMsgRevoked          // 撤回操作成功
	EventGroupMemberInvited  // 邀请入群成功
	EventConversationCreated // 会话创建成功
)

// Event WS 事件通用封装
//
// 完整链路：
//
//	ProtoResp.Data (proto bytes) → PushHandler 里 Unmarshal → eventbus.Publish(EventType, Event)
//	Subscribe(EventType) → chan Event → 上层业务处理
type Event struct {
	Type       EventType   `json:"type"`
	Payload    interface{} `json:"-"`       // 可选：已反序列化的 proto.Message（PushHandler 里填充）
	RawPayload []byte      `json:"payload"` // 原始 proto bytes（Response.Data 内容，WS 推送有值）
	Timestamp  int64       `json:"timestamp"`
}

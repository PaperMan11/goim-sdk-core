package model

// Conversation 会话完整信息（字段与 JSON tag 对齐服务端 conversation.Conversation）
// 带「SDK 本地」标注的字段为客户端缓存扩展，服务端不返回
type Conversation struct {
	OwnerUserID      string `json:"ownerUserID,omitempty"`
	ConversationID   string `json:"conversationID,omitempty"`
	RecvMsgOpt       int32  `json:"recvMsgOpt,omitempty"`
	ConversationType int32  `json:"conversationType,omitempty"` // SessionTypeC2C / SessionTypeGroup
	UserID           string `json:"userID,omitempty"`           // 对方用户ID（单聊）
	GroupID          string `json:"groupID,omitempty"`          // 群组ID（群聊）
	IsPinned         bool   `json:"isPinned,omitempty"`
	AttachedInfo     string `json:"attachedInfo,omitempty"`
	IsPrivateChat    bool   `json:"isPrivateChat,omitempty"`
	GroupAtType      int32  `json:"groupAtType,omitempty"`
	Ex               string `json:"ex,omitempty"`
	BurnDuration     int32  `json:"burnDuration,omitempty"`
	MinSeq           int64  `json:"minSeq,omitempty"`
	MaxSeq           int64  `json:"maxSeq,omitempty"`
	MsgDestructTime  int64  `json:"msgDestructTime,omitempty"`
	LatestMsgDestructTime int64 `json:"latestMsgDestructTime,omitempty"`
	IsMsgDestruct    bool   `json:"isMsgDestruct,omitempty"`

	// —— SDK 本地扩展（服务端 Conversation 无此字段，由客户端计算/维护）——
	UnreadCount       int32    `json:"unreadCount,omitempty"`
	LatestMsgSendTime int64    `json:"latestMsgSendTime,omitempty"`
	DraftText         string   `json:"draftText,omitempty"`
	MsgInfo           *MsgInfo `json:"msgInfo,omitempty"`
}

// PeerID 对端标识：单聊返回 userID，群聊返回 groupID
func (c *Conversation) PeerID() string {
	if c.ConversationType == SessionTypeGroup {
		return c.GroupID
	}
	return c.UserID
}

// MsgInfo 会话最新消息摘要（字段与 JSON tag 对齐服务端 conversation.MsgInfo）
type MsgInfo struct {
	ServerMsgID      string `json:"serverMsgID,omitempty"`
	ClientMsgID      string `json:"clientMsgID,omitempty"`
	SessionType      int32  `json:"sessionType,omitempty"`
	SendID           string `json:"sendID,omitempty"`
	RecvID           string `json:"recvID,omitempty"`
	SenderName       string `json:"senderName,omitempty"`
	FaceURL          string `json:"faceURL,omitempty"`
	GroupID          string `json:"groupID,omitempty"`
	GroupName        string `json:"groupName,omitempty"`
	GroupFaceURL     string `json:"groupFaceURL,omitempty"`
	GroupType        int32  `json:"groupType,omitempty"`
	GroupMemberCount uint32 `json:"groupMemberCount,omitempty"`
	LatestMsgRecvTime int64 `json:"LatestMsgRecvTime,omitempty"` // 注意：服务端 tag 首字母大写
	MsgFrom          int32  `json:"msgFrom,omitempty"`
	ContentType      int32  `json:"contentType,omitempty"`
	Content          string `json:"content,omitempty"`
	Ex               string `json:"ex,omitempty"`
}

// ConversationElem 会话列表元素（对齐服务端 conversation.ConversationElem；IsPinned tag 服务端为首字母大写）
type ConversationElem struct {
	ConversationID string   `json:"conversationID,omitempty"`
	RecvMsgOpt     int32    `json:"recvMsgOpt,omitempty"`
	UnreadCount    int64    `json:"unreadCount,omitempty"`
	IsPinned       bool     `json:"IsPinned,omitempty"`
	MsgInfo        *MsgInfo `json:"msgInfo,omitempty"`
}

// SortedConversationList 排序后的会话列表（对齐服务端 GetSortedConversationListResp）
type SortedConversationList struct {
	ConversationTotal int64               `json:"conversationTotal,omitempty"`
	UnreadTotal       int64               `json:"unreadTotal,omitempty"`
	ConversationElems []*ConversationElem `json:"conversationElems,omitempty"`
}

// SeqInfo 批量 seq 信息（对齐服务端 msg.Seqs，均为 int64）
type SeqInfo struct {
	MaxSeq     int64 `json:"maxSeq,omitempty"`
	HasReadSeq int64 `json:"hasReadSeq,omitempty"`
	MaxSeqTime int64 `json:"maxSeqTime,omitempty"`
}

// IncrementalConversations 增量会话
type IncrementalConversations struct {
	VersionID string          `json:"versionID"`
	Version   uint64          `json:"version"`
	Delete    []string        `json:"delete"`
	Insert    []*Conversation `json:"insert"`
	Update    []*Conversation `json:"update"`
	Full      bool            `json:"full"`
}

// FullConversationIDs 会话 ID 全量
type FullConversationIDs struct {
	VersionID       string   `json:"versionID"`
	Version         uint64   `json:"version"`
	Equal           bool     `json:"equal"`
	ConversationIDs []string `json:"conversationIDs"` // 服务端 GetFullOwnerConversationIDsResp 字段名
}

// DeleteSyncOpt 删除同步选项
type DeleteSyncOpt struct {
	IsSyncSelf  bool `json:"isSyncSelf"`
	IsSyncOther bool `json:"isSyncOther"`
}

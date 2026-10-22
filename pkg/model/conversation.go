package model

// Conversation 会话完整信息
type Conversation struct {
	ConversationID string `json:"conversationID"`
	ConversationType int32 `json:"conversationType"` // SessionTypeC2C / SessionTypeGroup

	SendID string `json:"sendID"` // 发起方 userID / 群主ID
	RecvID string `json:"recvID"` // 对方 userID / 群ID

	RecvMsgOpt int32 `json:"recvMsgOpt"`

	UnreadCount int32  `json:"unreadCount"`
	IsPinned    bool   `json:"isPinned"`
	PinnedTime  int64  `json:"pinnedTime"`
	MsgInfo     *MsgInfo `json:"msgInfo,omitempty"`

	LatestMsgSendTime int64  `json:"latestMsgSendTime"`
	DraftText         string `json:"draftText,omitempty"`

	AtSeqList []string `json:"atSeqList,omitempty"`
	IsGroup  bool     `json:"isGroup,omitempty"`

	Ex          string `json:"ex,omitempty"`
	GroupMemberCount int32 `json:"groupMemberCount,omitempty"`
	ShowName    string `json:"showName,omitempty"`
	FaceURL     string `json:"faceURL,omitempty"`

	CreateTime int64 `json:"createTime"`
}

// MsgInfo 会话摘要消息
type MsgInfo struct {
	ConversationID    string `json:"conversationID"`
	ClientMsgID       string `json:"clientMsgID"`
	ServerMsgID       string `json:"serverMsgID"`
	SendID            string `json:"sendID"`
	RecvID            string `json:"recvID"`
	SenderUserID      string `json:"senderUserID"`
	SenderNickname    string `json:"senderNickname,omitempty"`
	SenderFaceURL     string `json:"senderFaceURL,omitempty"`
	MsgType           int32  `json:"msgType"`
	Content           string `json:"content"`
	CreateTime        int64  `json:"createTime"`
	Status            int32  `json:"status"`
	IsRead            bool   `json:"isRead"`
	AttachedInfo      string `json:"attachedInfo,omitempty"`
	Ex                string `json:"ex,omitempty"`
}

// ConversationElem 会话列表元素
type ConversationElem struct {
	ConversationID string   `json:"conversationID"`
	RecvMsgOpt     int32    `json:"recvMsgOpt"`
	UnreadCount    int32    `json:"unreadCount"`
	IsPinned       bool     `json:"isPinned"`
	MsgInfo        *MsgInfo `json:"msgInfo,omitempty"`
}

// SortedConversationList 排序后的会话列表
type SortedConversationList struct {
	ConversationTotal int32              `json:"conversationTotal"`
	UnreadTotal       int32              `json:"unreadTotal"`
	ConversationElems []*ConversationElem `json:"conversationElems"`
}

// SeqInfo 批量 seq 信息
type SeqInfo struct {
	MaxSeq               string   `json:"maxSeq"`
	HasReadSeq           string   `json:"hasReadSeq"`
	MaxSeqTime           int64    `json:"maxSeqTime"`
	PinnedConversationIDs []string `json:"pinnedConversationIDs,omitempty"`
}

// IncrementalConversations 增量会话
type IncrementalConversations struct {
	Delete    []string        `json:"delete"`
	Insert    []*Conversation `json:"insert"`
	Update    []*Conversation `json:"update"`
	Full      bool            `json:"full"`
}

// FullConversationIDs 会话 ID 全量
type FullConversationIDs struct {
	UserIDs []string `json:"userIDs"`
}

// DeleteSyncOpt 删除同步选项
type DeleteSyncOpt struct {
	IsSyncSelf  bool `json:"isSyncSelf"`
	IsSyncOther bool `json:"isSyncOther"`
}

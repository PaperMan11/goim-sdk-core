package model

// 本文件定义本地数据能力（Local Data API）的分页结果与派生结构。
// 设计见 docs/LOCAL_DATA_API.md。

// ConversationPage 本地会话分页结果（置顶优先 + LatestMsgSendTime 倒序）
type ConversationPage struct {
	Items   []*Conversation `json:"items"`
	Total   int64           `json:"total"`
	HasMore bool            `json:"hasMore"`
}

// MsgPage 本地消息分页结果（seq 游标，OldestSeq/NewestSeq 为本页实际边界）
type MsgPage struct {
	Items     []*MsgData `json:"items"`
	OldestSeq int64      `json:"oldestSeq"`
	NewestSeq int64      `json:"newestSeq"`
	HasMore   bool       `json:"hasMore"`
}

// FriendPage 本地好友分页结果（置顶优先）
type FriendPage struct {
	Items   []*FriendInfo `json:"items"`
	Total   int64         `json:"total"`
	HasMore bool          `json:"hasMore"`
}

// BlackPage 本地黑名单分页结果
type BlackPage struct {
	Items   []*BlackInfo `json:"items"`
	Total   int64        `json:"total"`
	HasMore bool         `json:"hasMore"`
}

// GroupPage 本地群分页结果
type GroupPage struct {
	Items   []*GroupInfo `json:"items"`
	Total   int64        `json:"total"`
	HasMore bool         `json:"hasMore"`
}

// GroupMemberPage 本地群成员分页结果（角色优先 + 入群时间升序）
type GroupMemberPage struct {
	Items   []*GroupMemberFullInfo `json:"items"`
	Total   int64                  `json:"total"`
	HasMore bool                   `json:"hasMore"`
}

// ConversationSeqs 单会话的本地/服务端 seq 边界（全部来自本地缓存与同步元数据）
type ConversationSeqs struct {
	ConversationID string `json:"conversationID"`
	// LocalMinSeq / LocalMaxSeq 本地消息缓存中实际存在的 seq 边界（无缓存为 0）
	LocalMinSeq int64 `json:"localMinSeq"`
	LocalMaxSeq int64 `json:"localMaxSeq"`
	// HasReadSeq 本地已读消息的最大 seq（无已读消息为 0）
	HasReadSeq int64 `json:"hasReadSeq"`
	// ServerMinSeq / ServerMaxSeq 最近一次同步记录的服务端 seq 边界（缓存值，可能滞后）
	ServerMinSeq int64 `json:"serverMinSeq"`
	ServerMaxSeq int64 `json:"serverMaxSeq"`
}

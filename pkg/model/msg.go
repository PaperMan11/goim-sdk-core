package model

// MsgData 完整消息数据（字段与 JSON tag 对齐服务端 sdkws.MsgData）
type MsgData struct {
	SendID           string `json:"sendID,omitempty"`
	RecvID           string `json:"recvID,omitempty"`
	GroupID          string `json:"groupID,omitempty"`
	ClientMsgID      string `json:"clientMsgID,omitempty"`
	ServerMsgID      string `json:"serverMsgID,omitempty"`
	SenderPlatformID int32  `json:"senderPlatformID,omitempty"`
	SenderNickname   string `json:"senderNickname,omitempty"`
	SenderFaceURL    string `json:"senderFaceURL,omitempty"`

	SessionType int32  `json:"sessionType,omitempty"` // SessionTypeC2C / SessionTypeGroup
	MsgFrom     int32  `json:"msgFrom,omitempty"`     // MsgFromUser / MsgFromSystem
	ContentType int32  `json:"contentType,omitempty"`
	Content     string `json:"content,omitempty"`

	Seq        int64 `json:"seq,omitempty"`
	SendTime   int64 `json:"sendTime,omitempty"`
	CreateTime int64 `json:"createTime,omitempty"`
	Status     int32 `json:"status,omitempty"` // MsgStatus*
	IsRead     bool  `json:"isRead,omitempty"`

	// ConversationID 为 SDK 本地字段：服务端 MsgData 无此字段，
	// 由推送 map key / PullMsgs 归属计算后填充，供存储与同步使用
	ConversationID string `json:"conversationID,omitempty"`

	Options         map[string]bool  `json:"options,omitempty"`
	OfflinePushInfo *OfflinePushInfo `json:"offlinePushInfo,omitempty"`
	AtUserIDList    []string         `json:"atUserIDList,omitempty"`
	AttachedInfo    string           `json:"attachedInfo,omitempty"`
	Ex              string           `json:"ex,omitempty"`
}

// OfflinePushInfo 离线推送信息（对齐服务端 sdkws.OfflinePushInfo）
type OfflinePushInfo struct {
	Title         string `json:"title,omitempty"`
	Desc          string `json:"desc,omitempty"`
	Ex            string `json:"ex,omitempty"`
	IOSPushSound  string `json:"iOSPushSound,omitempty"`
	IOSBadgeCount bool   `json:"iOSBadgeCount,omitempty"`
}

// SendMsgResp 发送消息返回
type SendMsgResp struct {
	ClientMsgID string `json:"clientMsgID"`
	ServerMsgID string `json:"serverMsgID"`
	SendTime    int64  `json:"sendTime"`
	ModifyTime  int64  `json:"modifyTime"`
}

// PullMsgsResult 拉取消息结果
type PullMsgsResult struct {
	Msgs   []*MsgData `json:"msgs"`
	IsEnd  bool       `json:"isEnd"`
	EndSeq string     `json:"endSeq"`
}

// SeqRange 消息 seq 范围查询
type SeqRange struct {
	ConversationID string `json:"conversationID"`
	Begin          string `json:"begin"`
	End            string `json:"end"`
	Num            int32  `json:"num"`
	Order          int32  `json:"order"` // PullOrderAsc / PullOrderDesc
}

// SearchMsgResult 搜索消息结果（对齐服务端 SearchMessageResp）
type SearchMsgResult struct {
	ChatLogsNum int32            `json:"chatLogsNum"`
	ChatLogs    []*SearchChatLog `json:"chatLogs"`
}

// SearchChatLog 搜索结果条目（对齐服务端 SearchChatLog）
type SearchChatLog struct {
	ChatLog   *ChatLog `json:"chatLog"`
	IsRevoked bool     `json:"isRevoked"`
}

// ChatLog 会话日志（对齐服务端 msg.ChatLog，搜索/漫游场景的扁平消息）
type ChatLog struct {
	ServerMsgID      string `json:"serverMsgID,omitempty"`
	ClientMsgID      string `json:"clientMsgID,omitempty"`
	SendID           string `json:"sendID,omitempty"`
	RecvID           string `json:"recvID,omitempty"`
	GroupID          string `json:"groupID,omitempty"`
	RecvNickname     string `json:"recvNickname,omitempty"`
	SenderPlatformID int32  `json:"senderPlatformID,omitempty"`
	SenderNickname   string `json:"senderNickname,omitempty"`
	SenderFaceURL    string `json:"senderFaceURL,omitempty"`
	GroupName        string `json:"groupName,omitempty"`
	SessionType      int32  `json:"sessionType,omitempty"`
	MsgFrom          int32  `json:"msgFrom,omitempty"`
	ContentType      int32  `json:"contentType,omitempty"`
	Content          string `json:"content,omitempty"`
	Status           int32  `json:"status,omitempty"`
	SendTime         int64  `json:"sendTime,omitempty"`
	CreateTime       int64  `json:"createTime,omitempty"`
	Ex               string `json:"ex,omitempty"`
	GroupFaceURL     string `json:"groupFaceURL,omitempty"`
	GroupMemberCount uint32 `json:"groupMemberCount,omitempty"`
	Seq              int64  `json:"seq,omitempty"`
}

// MsgSendStatus 消息发送状态回执
type MsgSendStatus struct {
	ClientMsgID string `json:"clientMsgID"`
	ServerMsgID string `json:"serverMsgID"`
	SendID      string `json:"sendID"`
	RecvID      string `json:"recvID"`
	Status      int32  `json:"status"` // MsgStatus*
	Reason      string `json:"reason,omitempty"`
}

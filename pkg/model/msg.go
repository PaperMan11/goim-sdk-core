package model

// MsgData 完整消息数据
type MsgData struct {
	ClientMsgID      string `json:"clientMsgID"`
	ServerMsgID      string `json:"serverMsgID"`
	SenderUserID     string `json:"senderUserID"`
	SenderNickname   string `json:"senderNickname,omitempty"`
	SenderFaceURL    string `json:"senderFaceURL,omitempty"`
	SenderPlatformID int32  `json:"senderPlatformID,omitempty"`

	SessionType int32  `json:"sessionType"` // SessionTypeC2C / SessionTypeGroup
	MsgFrom     int32  `json:"msgFrom"`     // MsgFromUser / MsgFromSystem
	MsgType     int32  `json:"msgType"`
	Content     string `json:"content"`

	CreateTime int64 `json:"createTime"`
	ModifyTime int64 `json:"modifyTime"`
	Seqs       string `json:"seqs"`
	// 会话相关
	ConversationID string `json:"conversationID"`
	SendID         string `json:"sendID"`
	RecvID         string `json:"recvID"`

	Status int32 `json:"status"` // MsgStatus*

	IsRead bool `json:"isRead"`
	IsReadAndDel bool `json:"isReadAndDel,omitempty"`

	OfflinePushInfo *OfflinePushInfo `json:"offlinePushInfo,omitempty"`

	AttachedInfo string `json:"attachedInfo,omitempty"`
	Ex           string `json:"ex,omitempty"`

	MsgIDInMessageCache string `json:"msgIDInMessageCache,omitempty"`
}

// OfflinePushInfo 离线推送信息
type OfflinePushInfo struct {
	Title         string `json:"title,omitempty"`
	Desc          string `json:"desc,omitempty"`
	Content       string `json:"content,omitempty"`
	IOSPushSound  string `json:"iOSPushSound,omitempty"`
	IOSBadgeCount bool   `json:"iOSBadgeCount,omitempty"`
	Ex            string `json:"ex,omitempty"`
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

// SearchMsgResult 搜索消息结果
type SearchMsgResult struct {
	TotalCount int32      `json:"totalCount"`
	Msgs       []*MsgData `json:"msgs"`
}

// ChatLog 会话日志（返回带会话上下文的消息）
type ChatLog struct {
	ConversationID string  `json:"conversationID"`
	Msg            *MsgData `json:"msg"`
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

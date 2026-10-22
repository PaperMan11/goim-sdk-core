package events

// ConversationUpdateTips 会话属性更新
type ConversationUpdateTips struct {
	ConversationID string `json:"conversationID"`
	UserID         string `json:"userID"`
	RecvMsgOpt     int32  `json:"recvMsgOpt,omitempty"`
	IsPinned       int32  `json:"isPinned,omitempty"`
	AttachedInfo   string `json:"attachedInfo,omitempty"`
	Ex             string `json:"ex,omitempty"`
	MaxSeq         int64  `json:"maxSeq,omitempty"`
}

// ConversationDeleteTips 会话删除
type ConversationDeleteTips struct {
	ConversationID string `json:"conversationID"`
	UserID         string `json:"userID"`
}

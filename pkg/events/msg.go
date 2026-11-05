package events

// RevokeMsgTips 消息撤回提示 —— 对齐服务端 sdkws.RevokeMsgTips
type RevokeMsgTips struct {
	RevokerUserID  string `json:"revokerUserID,omitempty"`
	ClientMsgID    string `json:"clientMsgID,omitempty"`
	RevokeTime     int64  `json:"revokeTime,omitempty"`
	SesstionType   int32  `json:"sesstionType,omitempty"` // 注意服务端拼写为 sesstionType
	Seq            int64  `json:"seq,omitempty"`
	ConversationID string `json:"conversationID,omitempty"`
	IsAdminRevoke  bool   `json:"isAdminRevoke,omitempty"`
}

// DeleteMsgsTips 消息删除提示 —— 对齐服务端 sdkws.DeleteMsgsTips
type DeleteMsgsTips struct {
	UserID         string  `json:"userID,omitempty"`
	ConversationID string  `json:"conversationID,omitempty"`
	Seqs           []int64 `json:"seqs,omitempty"`
}

// MarkAsReadTips 已读回执提示 —— 对齐服务端 sdkws.MarkAsReadTips
type MarkAsReadTips struct {
	MarkAsReadUserID string  `json:"markAsReadUserID,omitempty"`
	ConversationID   string  `json:"conversationID,omitempty"`
	Seqs             []int64 `json:"seqs,omitempty"`
	HasReadSeq       int64   `json:"hasReadSeq,omitempty"`
}

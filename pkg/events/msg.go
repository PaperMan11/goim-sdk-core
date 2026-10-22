package events

// RevokeMsgTips 消息撤回提示
type RevokeMsgTips struct {
	ConversationID string `json:"conversationID"`
	Seq            int64  `json:"seq"`
	UserID         string `json:"userID"`
}

// DeleteMsgsTips 消息删除提示
type DeleteMsgsTips struct {
	ConversationID string  `json:"conversationID"`
	Seqs           []int64 `json:"seqs"`
	UserID         string  `json:"userID"`
}

// MarkAsReadTips 已读回执提示
type MarkAsReadTips struct {
	ConversationID string  `json:"conversationID"`
	Seqs           []int64 `json:"seqs"`
	UserID         string  `json:"userID"`
}

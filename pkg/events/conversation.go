package events

// ConversationUpdateTips 会话属性更新 —— 对齐服务端 sdkws.ConversationUpdateTips
//
// 服务端只下发 userID + conversationIDList（变更会话 ID 列表），
// 各会话具体属性由会话增量同步拉取。
type ConversationUpdateTips struct {
	UserID             string   `json:"userID"`
	ConversationIDList []string `json:"conversationIDList,omitempty"`
}

// ConversationDeleteTips 会话删除 —— 对齐服务端 sdkws.ConversationDeleteTips
type ConversationDeleteTips struct {
	UserID          string   `json:"userID"`
	ConversationIDs []string `json:"conversationIDs,omitempty"`
}

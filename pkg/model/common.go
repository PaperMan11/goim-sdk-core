package model

// RequestPagination 分页请求参数
type RequestPagination struct {
	PageNumber int32 `json:"pageNumber"`
	ShowNumber int32 `json:"showNumber"`
}

// ConversationReq 设置会话请求体
type ConversationReq struct {
	ConversationID string `json:"conversationID"`
	RecvMsgOpt     int32  `json:"recvMsgOpt,omitempty"`
	IsPinned       bool   `json:"isPinned,omitempty"`
	DraftText      string `json:"draftText,omitempty"`
	Ex             string `json:"ex,omitempty"`
}

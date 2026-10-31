package model

// RequestPagination 分页请求参数
type RequestPagination struct {
	PageNumber int32 `json:"pageNumber"`
	ShowNumber int32 `json:"showNumber"`
}

// —— wrapperspb 兼容类型 ——
// 服务端 proto 中可选字段用 google.protobuf.wrapperspb（StringValue/Int32Value 等），
// 其 JSON 形如 {"value": ...}；以下结构体序列化结果与之兼容。

// StringValue 对齐 wrapperspb.StringValue
type StringValue struct {
	Value string `json:"value"`
}

// Int32Value 对齐 wrapperspb.Int32Value
type Int32Value struct {
	Value int32 `json:"value"`
}

// Int64Value 对齐 wrapperspb.Int64Value
type Int64Value struct {
	Value int64 `json:"value"`
}

// BoolValue 对齐 wrapperspb.BoolValue
type BoolValue struct {
	Value bool `json:"value"`
}

// ConversationReq 设置会话请求体（字段与 JSON tag 对齐服务端 conversation.ConversationReq，
// 可选字段为 wrapperspb 指针形态）
type ConversationReq struct {
	ConversationID   string       `json:"conversationID,omitempty"`
	ConversationType int32        `json:"conversationType,omitempty"`
	UserID           string       `json:"userID,omitempty"`
	GroupID          string       `json:"groupID,omitempty"`
	RecvMsgOpt       *Int32Value  `json:"recvMsgOpt,omitempty"`
	IsPinned         *BoolValue   `json:"isPinned,omitempty"`
	AttachedInfo     *StringValue `json:"attachedInfo,omitempty"`
	IsPrivateChat    *BoolValue   `json:"isPrivateChat,omitempty"`
	Ex               *StringValue `json:"ex,omitempty"`
	BurnDuration     *Int32Value  `json:"burnDuration,omitempty"`
	MinSeq           *Int64Value  `json:"minSeq,omitempty"`
	MaxSeq           *Int64Value  `json:"maxSeq,omitempty"`
	GroupAtType      *Int32Value  `json:"groupAtType,omitempty"`
	MsgDestructTime  *Int64Value  `json:"msgDestructTime,omitempty"`
	IsMsgDestruct    *BoolValue   `json:"isMsgDestruct,omitempty"`
}

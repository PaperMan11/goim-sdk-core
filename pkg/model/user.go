package model

// UserInfo 用户完整信息
type UserInfo struct {
	UserID           string `json:"userID"`
	Nickname         string `json:"nickname"`
	FaceURL          string `json:"faceURL"`
	Ex               string `json:"ex"`
	CreateTime       int64  `json:"createTime"`
	AppManagerLevel  int32  `json:"appManagerLevel"`
	GlobalRecvMsgOpt int32  `json:"globalRecvMsgOpt"`
}

// UserInfoWithEx 更新用户资料（部分字段可选）
type UserInfoWithEx struct {
	UserID           string `json:"userID"`
	Nickname         string `json:"nickname,omitempty"`
	FaceURL          string `json:"faceURL,omitempty"`
	Ex               string `json:"ex,omitempty"`
	GlobalRecvMsgOpt int32  `json:"globalRecvMsgOpt,omitempty"`
}

// PublicUserInfo 简化用户信息（公开）
type PublicUserInfo struct {
	UserID   string `json:"userID"`
	Nickname string `json:"nickname"`
	FaceURL  string `json:"faceURL"`
	Gender   int32  `json:"gender"`
	Ex       string `json:"ex"`
}

// AccountStatus 账号状态
type AccountStatus struct {
	UserID        string `json:"userID"`
	AccountStatus int32  `json:"accountStatus"` // 0-正常 1-禁用
}

// OnlineStatus 在线状态
type OnlineStatus struct {
	UserID      string  `json:"userID"`
	Status      int32   `json:"status"`      // 0-离线 1-在线
	PlatformIDs []int32 `json:"platformIDs"`
}

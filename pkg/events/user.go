package events

// UserInfoUpdatedTips 用户信息更新
type UserInfoUpdatedTips struct {
	UserID     string `json:"userID"`
	Nickname   string `json:"nickname,omitempty"`
	FaceURL    string `json:"faceURL,omitempty"`
	Gender     int32  `json:"gender,omitempty"`
	Email      string `json:"email,omitempty"`
	Birth      int64  `json:"birth,omitempty"`
	AllowAdd   int32  `json:"allowAdd,omitempty"`
	GlobalRecvMsgOpt int32 `json:"globalRecvMsgOpt,omitempty"`
}

// UserStatusChangeTips 在线状态变更
type UserStatusChangeTips struct {
	UserID       string  `json:"userID"`
	Status       int32   `json:"status"`
	PlatformIDs  []int32 `json:"platformIDs,omitempty"`
}

// ForceLogoutTips Token 被踢下线
type ForceLogoutTips struct {
	UserID     string `json:"userID"`
	PlatformID int32  `json:"platformID"`
	Reason     string `json:"reason,omitempty"`
}

package events

// UserInfoUpdatedTips 用户信息更新 —— 对齐服务端 sdkws.UserInfoUpdatedTips
//
// 服务端只下发 userID（用户资料需另行拉取），因此此处不携带昵称/头像。
type UserInfoUpdatedTips struct {
	UserID string `json:"userID"`
}

// UserStatusChangeTips 在线状态变更 —— 对齐服务端 sdkws.UserStatusChangeTips
//
// 服务端字段为 fromUserID（状态变更者）/ toUserID（订阅者）/ status / platformID。
type UserStatusChangeTips struct {
	FromUserID string `json:"fromUserID"`
	ToUserID   string `json:"toUserID,omitempty"`
	Status     int32  `json:"status,omitempty"`     // 1-在线 0-离线
	PlatformID int32  `json:"platformID,omitempty"` // 平台 ID
}

// ForceLogoutTips Token 被踢下线（WS 控制消息，非 sdkws 通知）
type ForceLogoutTips struct {
	UserID     string `json:"userID"`
	PlatformID int32  `json:"platformID"`
	Reason     string `json:"reason,omitempty"`
}

package events

import "github.com/PaperMan11/goim-sdk-core/pkg/model"

// FromToUserID 用户 ID 对 —— 对齐服务端 sdkws.FromToUserID
type FromToUserID struct {
	FromUserID string `json:"fromUserID"`
	ToUserID   string `json:"toUserID"`
}

// FriendApplicationTips 好友申请 —— 对齐服务端 sdkws.FriendApplicationTips
type FriendApplicationTips struct {
	FromToUserID *FromToUserID        `json:"fromToUserID,omitempty"`
	Request      *model.FriendRequest `json:"request,omitempty"`
}

// FriendAddedTips 好友添加成功 —— 对齐服务端 sdkws.FriendAddedTips
type FriendAddedTips struct {
	Friend          *model.FriendInfo     `json:"friend,omitempty"`
	OperationTime   int64                 `json:"operationTime,omitempty"`
	OpUser          *model.PublicUserInfo `json:"opUser,omitempty"`
	FriendVersion   uint64                `json:"friendVersion,omitempty"`
	FriendVersionID string                `json:"friendVersionID,omitempty"`
}

// FriendDeletedTips 好友删除 —— 对齐服务端 sdkws.FriendDeletedTips
type FriendDeletedTips struct {
	FromToUserID    *FromToUserID `json:"fromToUserID,omitempty"`
	FriendVersion   uint64        `json:"friendVersion,omitempty"`
	FriendVersionID string        `json:"friendVersionID,omitempty"`
}

// FriendInfoChangedTips 好友信息变更 —— 对齐服务端 sdkws.FriendInfoChangedTips
type FriendInfoChangedTips struct {
	FromToUserID      *FromToUserID `json:"fromToUserID,omitempty"`
	FriendVersion     uint64        `json:"friendVersion,omitempty"`
	FriendVersionID   string        `json:"friendVersionID,omitempty"`
	FriendSortVersion uint64        `json:"friendSortVersion,omitempty"`
}

// BlackAddedTips 黑名单添加 —— 对齐服务端 sdkws.BlackAddedTips
type BlackAddedTips struct {
	FromToUserID *FromToUserID `json:"fromToUserID,omitempty"`
}

// BlackDeletedTips 黑名单删除 —— 对齐服务端 sdkws.BlackDeletedTips
type BlackDeletedTips struct {
	FromToUserID *FromToUserID `json:"fromToUserID,omitempty"`
}

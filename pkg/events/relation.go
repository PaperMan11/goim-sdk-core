package events

// FriendApplicationTips 好友申请
type FriendApplicationTips struct {
	FromUserID     string `json:"fromUserID"`
	ToUserID       string `json:"toUserID"`
	ReqMsg         string `json:"reqMsg,omitempty"`
	CreateTime     int64  `json:"createTime"`
	FromNickname   string `json:"fromNickname,omitempty"`
	FromFaceURL    string `json:"fromFaceURL,omitempty"`
	FromGender     int32  `json:"fromGender,omitempty"`
	ToNickname     string `json:"toNickname,omitempty"`
	ToFaceURL      string `json:"toFaceURL,omitempty"`
	ToGender       int32  `json:"toGender,omitempty"`
}

// FriendAddedTips 好友添加成功
type FriendAddedTips struct {
	OwnerUserID  string `json:"ownerUserID"`
	FriendUserID string `json:"friendUserID"`
}

// FriendDeletedTips 好友删除
type FriendDeletedTips struct {
	OwnerUserID  string `json:"ownerUserID"`
	FriendUserID string `json:"friendUserID"`
}

// FriendInfoChangedTips 好友信息变更
type FriendInfoChangedTips struct {
	OwnerUserID   string `json:"ownerUserID"`
	FriendUserID  string `json:"friendUserID"`
	Remark        string `json:"remark,omitempty"`
	IsPinned      int32  `json:"isPinned,omitempty"`
}

// BlackAddedTips 黑名单添加
type BlackAddedTips struct {
	OwnerUserID  string `json:"ownerUserID"`
	BlackUserID  string `json:"blackUserID"`
}

// BlackDeletedTips 黑名单删除
type BlackDeletedTips struct {
	OwnerUserID  string `json:"ownerUserID"`
	BlackUserID  string `json:"blackUserID"`
}

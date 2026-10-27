package model

// FriendInfo 好友关系完整信息
type FriendInfo struct {
	OwnerUserID          string `json:"ownerUserID"`
	FriendUserID         string `json:"friendUserID"`
	Nickname             string `json:"nickname,omitempty"`
	Remark               string `json:"remark,omitempty"`
	FaceURL              string `json:"faceURL,omitempty"`
	Gender               int32  `json:"gender,omitempty"`
	CreateTime           int64  `json:"createTime"`
	AddSource            int32  `json:"addSource,omitempty"`
	OperatorUserID       string `json:"operatorUserID,omitempty"`
	Ex                   string `json:"ex,omitempty"`
	IsPinned             bool   `json:"isPinned,omitempty"`
	IsHidden             bool   `json:"isHidden,omitempty"`
	BackgroundImagePath  string `json:"backgroundImagePath,omitempty"`
	RecvMsgOpt           int32  `json:"recvMsgOpt,omitempty"`
}

// BlackInfo 黑名单信息
type BlackInfo struct {
	OwnerUserID    string `json:"ownerUserID"`
	BlackUserID    string `json:"blackUserID"`
	BlackNickname  string `json:"blackNickname,omitempty"`
	BlackFaceURL   string `json:"blackFaceURL,omitempty"`
	CreateTime     int64  `json:"createTime"`
	OperatorUserID string `json:"operatorUserID,omitempty"`
}

// FriendRequest 好友申请
type FriendRequest struct {
	FromUserID        string `json:"fromUserID"`
	FromNickname      string `json:"fromNickname,omitempty"`
	FromFaceURL       string `json:"fromFaceURL,omitempty"`
	FromGender        int32  `json:"fromGender,omitempty"`
	ToUserID          string `json:"toUserID"`
	ToNickname        string `json:"toNickname,omitempty"`
	ToFaceURL         string `json:"toFaceURL,omitempty"`
	ReqMsg            string `json:"reqMsg"`
	HandleResult      int32  `json:"handleResult"` // HandleResult*
	HandledUserID     string `json:"handledUserID,omitempty"`
	HandledNickname   string `json:"handledNickname,omitempty"`
	HandledFaceURL    string `json:"handledFaceURL,omitempty"`
	Reason            string `json:"reason,omitempty"`
	CreateTime        int64  `json:"createTime"`
	HandleTime        int64  `json:"handleTime,omitempty"`
}

// FriendRelation 双向好友关系
type FriendRelation struct {
	User1            string `json:"user1"`
	User2            string `json:"user2"`
	InUser1Friends   bool   `json:"inUser1Friends"`
	InUser2Friends   bool   `json:"inUser2Friends"`
}

// IncrementalFriends 增量好友（对齐服务端 GetIncrementalFriendsResp：无分批，一次返回全部增量）
type IncrementalFriends struct {
	VersionID string       `json:"versionID"` // 当前版本ID（写回本地游标）
	Version   uint64       `json:"version"`   // 当前版本号
	Delete    []string     `json:"delete"`
	Insert    []FriendInfo `json:"insert"`
	Update    []FriendInfo `json:"update"`
	Full      bool         `json:"full"` // true：版本线变更，需走 GetFullFriendUserIDs 全量校验
}

// IncrementalBlacks 增量黑名单
type IncrementalBlacks struct {
	VersionID string      `json:"versionID"`
	Version   uint64      `json:"version"`
	Delete    []string    `json:"delete"`
	Insert    []BlackInfo `json:"insert"`
	Update    []BlackInfo `json:"update"`
	Full      bool        `json:"full"`
}

// FullFriendIDs 好友 ID 全量校验结果（对齐 GetFullFriendUserIDsResp）
type FullFriendIDs struct {
	VersionID string   `json:"versionID"`
	Version   uint64   `json:"version"`
	Equal     bool     `json:"equal"`  // 本地 idHash 与服务端是否一致
	UserIDs   []string `json:"userIDs"` // 不一致时的服务端全量好友 ID 列表
}

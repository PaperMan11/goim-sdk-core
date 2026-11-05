package model

// FriendUserInfo 对齐服务端 protobuf sdkws.UserInfo —— FriendInfo.friendUser 嵌套对象
//
// 服务端 friendUser 是 *sdkws.UserInfo 嵌套，SDK 原来把它展平成 FriendUserID / Nickname / FaceURL
// 三个独立扁平字段，导致 json.Unmarshal 无法自动映射。现直接定义同构嵌套结构，
// 让 model 层与服务端接口 1:1 对应，无任何自定义反序列化 trick。
type FriendUserInfo struct {
	UserID           string `json:"userID,omitempty"`
	Nickname         string `json:"nickname,omitempty"`
	FaceURL          string `json:"faceURL,omitempty"`
	Ex               string `json:"ex,omitempty"`
	CreateTime       int64  `json:"createTime,omitempty"`
	AppMangerLevel   int32  `json:"appMangerLevel,omitempty"`
	GlobalRecvMsgOpt int32  `json:"globalRecvMsgOpt,omitempty"`
}

// FriendInfo 对齐服务端 protobuf sdkws.FriendInfo
//
// JSON 结构与 sdkws.FriendInfo 完全一致，直接 json.Unmarshal 即可：
//
//	{
//	  "ownerUserID":    "u2",
//	  "remark":         "it-remark",
//	  "createTime":     1730000000,
//	  "friendUser":     { "userID": "u1", "nickname": "A", "faceURL": "..." },
//	  "addSource":      2,
//	  "operatorUserID": "u2",
//	  "ex":             "...",
//	  "isPinned":       true
//	}
//
// 服务端 protobuf 未定义的字段（Gender / IsHidden / BackgroundImagePath / RecvMsgOpt）
// 保留作 SDK 内部扩展，json tag 为 "-"，不参与 wire。
type FriendInfo struct {
	OwnerUserID    string          `json:"ownerUserID,omitempty"`
	Remark         string          `json:"remark,omitempty"`
	CreateTime     int64           `json:"createTime,omitempty"`
	FriendUser     *FriendUserInfo `json:"friendUser,omitempty"`
	AddSource      int32           `json:"addSource,omitempty"`
	OperatorUserID string          `json:"operatorUserID,omitempty"`
	Ex             string          `json:"ex,omitempty"`
	IsPinned       bool            `json:"isPinned,omitempty"`

	// —— 下面是 SDK 自己扩展的字段，服务端 protobuf FriendInfo 没有 ——
	// json tag 设为 "-"，不参与 HTTP wire 序列化/反序列化
	Gender              int32  `json:"-"`
	IsHidden            bool   `json:"-"`
	BackgroundImagePath string `json:"-"`
	RecvMsgOpt          int32  `json:"-"`
}

// FriendUserID 快捷访问 friendUser.userID（避免到处写 nil check）
func (f *FriendInfo) FriendUserID() string {
	if f == nil || f.FriendUser == nil {
		return ""
	}
	return f.FriendUser.UserID
}

// Nickname 快捷访问 friendUser.nickname
func (f *FriendInfo) Nickname() string {
	if f == nil || f.FriendUser == nil {
		return ""
	}
	return f.FriendUser.Nickname
}

// FaceURL 快捷访问 friendUser.faceURL
func (f *FriendInfo) FaceURL() string {
	if f == nil || f.FriendUser == nil {
		return ""
	}
	return f.FriendUser.FaceURL
}

// BlackInfo 对齐服务端 protobuf sdkws.BlackInfo
//
// 服务端 blackUserInfo 是 *PublicUserInfo 嵌套（PublicUserInfo 结构在 pkg/model/user.go），
// 对齐方式与 FriendInfo 完全一致：嵌套字段直接复用已有 PublicUserInfo type。
type BlackInfo struct {
	OwnerUserID    string          `json:"ownerUserID,omitempty"`
	CreateTime     int64           `json:"createTime,omitempty"`
	BlackUserInfo  *PublicUserInfo `json:"blackUserInfo,omitempty"`
	AddSource      int32           `json:"addSource,omitempty"`
	OperatorUserID string          `json:"operatorUserID,omitempty"`
	Ex             string          `json:"ex,omitempty"`
}

// BlackUserID 快捷访问 blackUserInfo.userID
func (b *BlackInfo) BlackUserID() string {
	if b == nil || b.BlackUserInfo == nil {
		return ""
	}
	return b.BlackUserInfo.UserID
}

// BlackNickname 快捷访问 blackUserInfo.nickname
func (b *BlackInfo) BlackNickname() string {
	if b == nil || b.BlackUserInfo == nil {
		return ""
	}
	return b.BlackUserInfo.Nickname
}

// BlackFaceURL 快捷访问 blackUserInfo.faceURL
func (b *BlackInfo) BlackFaceURL() string {
	if b == nil || b.BlackUserInfo == nil {
		return ""
	}
	return b.BlackUserInfo.FaceURL
}

// FriendRequest 好友申请 —— 对齐服务端 protobuf sdkws.FriendRequest
//
// 服务端字段：fromUserID / fromNickname / fromFaceURL / toUserID / toNickname /
// toFaceURL / handleResult / reqMsg / createTime / handlerUserID / handleMsg /
// handleTime / ex
// 注意处理人字段为 handlerUserID（不是 handledUserID）。
type FriendRequest struct {
	FromUserID    string `json:"fromUserID"`
	FromNickname  string `json:"fromNickname,omitempty"`
	FromFaceURL   string `json:"fromFaceURL,omitempty"`
	ToUserID      string `json:"toUserID,omitempty"`
	ToNickname    string `json:"toNickname,omitempty"`
	ToFaceURL     string `json:"toFaceURL,omitempty"`
	HandleResult  int32  `json:"handleResult,omitempty"` // 0-未处理 1-已同意 -1-已拒绝
	ReqMsg        string `json:"reqMsg,omitempty"`
	CreateTime    int64  `json:"createTime,omitempty"`
	HandlerUserID string `json:"handlerUserID,omitempty"`
	HandleMsg     string `json:"handleMsg,omitempty"`
	HandleTime    int64  `json:"handleTime,omitempty"`
	Ex            string `json:"ex,omitempty"`
}

// FriendRelation 双向好友关系
type FriendRelation struct {
	User1          string `json:"user1"`
	User2          string `json:"user2"`
	InUser1Friends bool   `json:"inUser1Friends"`
	InUser2Friends bool   `json:"inUser2Friends"`
}

// IncrementalFriends 增量好友 —— 对齐服务端 protobuf getIncrementalFriendsResp
//
// JSON 结构与 relation.proto 完全一致，Insert / Update 元素为 FriendInfo（已与
// sdkws.FriendInfo 同构），Delete 为 []string（好友 ID 列表）。
type IncrementalFriends struct {
	VersionID   string       `json:"versionID,omitempty"`
	Version     uint64       `json:"version,omitempty"`
	Delete      []string     `json:"delete,omitempty"`
	Insert      []FriendInfo `json:"insert,omitempty"`
	Update      []FriendInfo `json:"update,omitempty"`
	Full        bool         `json:"full,omitempty"`
	SortVersion uint64       `json:"sortVersion,omitempty"`
}

// IncrementalBlacks 增量黑名单
type IncrementalBlacks struct {
	VersionID string      `json:"versionID,omitempty"`
	Version   uint64      `json:"version,omitempty"`
	Delete    []string    `json:"delete,omitempty"`
	Insert    []BlackInfo `json:"insert,omitempty"`
	Update    []BlackInfo `json:"update,omitempty"`
	Full      bool        `json:"full,omitempty"`
}

// FullFriendIDs 好友 ID 全量校验结果 —— 对齐 getFullFriendUserIDsResp
type FullFriendIDs struct {
	VersionID string   `json:"versionID,omitempty"`
	Version   uint64   `json:"version,omitempty"`
	Equal     bool     `json:"equal,omitempty"`
	UserIDs   []string `json:"userIDs,omitempty"`
}

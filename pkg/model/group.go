package model

// GroupInfo 群组完整信息 —— 对齐服务端 protobuf sdkws.GroupInfo
//
// JSON 结构与 sdkws.GroupInfo 完全一致。
// 服务端无 MaxMemberCount / RecvMsgOpt / ApplyMemberCheck 字段；
// 群消息接收选项在会话维度（Conversation.RecvMsgOpt），不在群组维度。
type GroupInfo struct {
	GroupID                string `json:"groupID"`
	GroupName              string `json:"groupName"`
	Notification           string `json:"notification"`
	Introduction           string `json:"introduction"`
	FaceURL                string `json:"faceURL"`
	OwnerUserID            string `json:"ownerUserID"`
	CreateTime             int64  `json:"createTime"`
	MemberCount            uint32 `json:"memberCount"`
	Ex                     string `json:"ex"`
	Status                 int32  `json:"status"`
	CreatorUserID          string `json:"creatorUserID"`
	GroupType              int32  `json:"groupType"`
	NeedVerification       int32  `json:"needVerification"`
	LookMemberInfo         int32  `json:"lookMemberInfo"`
	ApplyMemberFriend      int32  `json:"applyMemberFriend"`
	NotificationUpdateTime int64  `json:"notificationUpdateTime"`
	NotificationUserID     string `json:"notificationUserID"`

	// —— SDK 本地扩展（服务端 sdkws.GroupInfo 无此字段，不入 wire）——
	// 保留供需要的地方使用，但不参与 JSON 序列化/反序列化
	MaxMemberCount int32 `json:"-"`
	RecvMsgOpt     int32 `json:"-"`
}

// GroupMemberFullInfo 群成员完整信息 —— 对齐服务端 protobuf sdkws.GroupMemberFullInfo
type GroupMemberFullInfo struct {
	GroupID        string `json:"groupID"`
	UserID         string `json:"userID"`
	RoleLevel      int32  `json:"roleLevel"`
	JoinTime       int64  `json:"joinTime"`
	Nickname       string `json:"nickname"`
	FaceURL        string `json:"faceURL"`
	AppMangerLevel int32  `json:"appMangerLevel"`
	JoinSource     int32  `json:"joinSource"`
	OperatorUserID string `json:"operatorUserID"`
	Ex             string `json:"ex"`
	MuteEndTime    int64  `json:"muteEndTime"`
	InviterUserID  string `json:"inviterUserID"`
}

// GroupInfoForSet 设置群组信息参数（对齐服务端 sdkws.GroupInfoForSet：
// 名称类字段为裸值，可选行为字段为 wrapperspb 指针形态）
type GroupInfoForSet struct {
	GroupID           string       `json:"groupID"`
	GroupName         string       `json:"groupName,omitempty"`
	Notification      string       `json:"notification,omitempty"`
	Introduction      string       `json:"introduction,omitempty"`
	FaceURL           string       `json:"faceURL,omitempty"`
	Ex                *StringValue `json:"ex,omitempty"`
	NeedVerification  *Int32Value  `json:"needVerification,omitempty"`
	LookMemberInfo    *Int32Value  `json:"lookMemberInfo,omitempty"`
	ApplyMemberFriend *Int32Value  `json:"applyMemberFriend,omitempty"`
}

// GroupInfoExOptions 设置群信息扩展（对齐服务端 group.SetGroupInfoExReq，
// 全部可选字段为 wrapperspb 指针形态；groupID 由 manager 层单独携带）
type GroupInfoExOptions struct {
	GroupName         *StringValue `json:"groupName,omitempty"`
	Notification      *StringValue `json:"notification,omitempty"`
	Introduction      *StringValue `json:"introduction,omitempty"`
	FaceURL           *StringValue `json:"faceURL,omitempty"`
	Ex                *StringValue `json:"ex,omitempty"`
	NeedVerification  *Int32Value  `json:"needVerification,omitempty"`
	LookMemberInfo    *Int32Value  `json:"lookMemberInfo,omitempty"`
	ApplyMemberFriend *Int32Value  `json:"applyMemberFriend,omitempty"`
}

// SetGroupMemberInfo 设置群成员信息（对齐服务端 group.SetGroupMemberInfo，可选字段为 wrapperspb 形态）
type SetGroupMemberInfo struct {
	GroupID   string       `json:"groupID"`
	UserID    string       `json:"userID"`
	Nickname  *StringValue `json:"nickname,omitempty"`
	FaceURL   *StringValue `json:"faceURL,omitempty"`
	RoleLevel *Int32Value  `json:"roleLevel,omitempty"`
	Ex        *StringValue `json:"ex,omitempty"`
}

// GroupRequest 入群申请 —— 对齐服务端 protobuf sdkws.GroupRequest
//
// 服务端为嵌套结构：userInfo（申请人）、groupInfo（群）、joinSource、inviterUserID 等。
type GroupRequest struct {
	UserInfo      *PublicUserInfo `json:"userInfo,omitempty"`
	GroupInfo     *GroupInfo      `json:"groupInfo,omitempty"`
	HandleResult  int32           `json:"handleResult,omitempty"` // 0-未处理 1-已同意 -1-已拒绝
	ReqMsg        string          `json:"reqMsg,omitempty"`
	HandleMsg     string          `json:"handleMsg,omitempty"`
	ReqTime       int64           `json:"reqTime,omitempty"`
	HandleUserID  string          `json:"handleUserID,omitempty"`
	HandleTime    int64           `json:"handleTime,omitempty"`
	Ex            string          `json:"ex,omitempty"`
	JoinSource    int32           `json:"joinSource,omitempty"`
	InviterUserID string          `json:"inviterUserID,omitempty"`
}

// IncrementalGroups 增量群组列表
type IncrementalGroups struct {
	VersionID string      `json:"versionID"`
	Version   uint64      `json:"version"`
	Delete    []string    `json:"delete"`
	Insert    []GroupInfo `json:"insert"`
	Update    []GroupInfo `json:"update"`
	Full      bool        `json:"full"`
}

// IncrementalMembers 增量群成员列表（群维度游标）
type IncrementalMembers struct {
	VersionID string                `json:"versionID"`
	Version   uint64                `json:"version"`
	GroupID   string                `json:"groupID"`
	Delete    []string              `json:"delete"`
	Insert    []GroupMemberFullInfo `json:"insert"`
	Update    []GroupMemberFullInfo `json:"update"`
	Full      bool                  `json:"full"`
}

// FullMemberIDs 群成员 ID 全量
type FullMemberIDs struct {
	VersionID string   `json:"versionID"`
	Version   uint64   `json:"version"`
	GroupID   string   `json:"groupID"`
	Equal     bool     `json:"equal"`
	UserIDs   []string `json:"userIDs"`
}

// FullGroupIDs 加入群 ID 全量校验结果（对齐 GetFullJoinGroupIDsResp）
type FullGroupIDs struct {
	VersionID string   `json:"versionID"`
	Version   uint64   `json:"version"`
	Equal     bool     `json:"equal"`
	GroupIDs  []string `json:"groupIDs"`
}

// GroupAbstractInfo 群摘要（对齐服务端 group.GroupAbstractInfo）
type GroupAbstractInfo struct {
	GroupID             string `json:"groupID"`
	GroupMemberNumber   uint32 `json:"groupMemberNumber"`
	GroupMemberListHash uint64 `json:"groupMemberListHash"`
}

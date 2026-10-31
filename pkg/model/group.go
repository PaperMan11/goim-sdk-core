package model

// GroupInfo 群组完整信息
type GroupInfo struct {
	GroupID          string `json:"groupID"`
	GroupName        string `json:"groupName"`
	Notification     string `json:"notification"`
	Introduction     string `json:"introduction"`
	FaceURL          string `json:"faceURL"`
	OwnerUserID      string `json:"ownerUserID"`
	CreateTime       int64  `json:"createTime"`
	MemberCount      int32  `json:"memberCount"`
	MaxMemberCount   int32  `json:"maxMemberCount"`
	Status           int32  `json:"status"`           // 0-正常 1-被解散
	RecvMsgOpt       int32  `json:"recvMsgOpt"`       // 群消息接收选项
	LookMemberInfo   int32  `json:"lookMemberInfo"`   // 是否允许查看群成员信息
	ApplyMemberCheck int32  `json:"applyMemberCheck"` // 是否允许入群需申请
	Ex               string `json:"ex"`
}

// GroupMemberFullInfo 群成员完整信息
type GroupMemberFullInfo struct {
	GroupID        string `json:"groupID"`
	UserID         string `json:"userID"`
	Nickname       string `json:"nickname"`
	FaceURL        string `json:"faceURL"`
	RoleLevel      int32  `json:"roleLevel"` // RoleOwner / RoleAdmin / RoleMember
	JoinTime       int64  `json:"joinTime"`
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

// GroupRequest 入群申请
type GroupRequest struct {
	GroupID         string `json:"groupID"`
	GroupName       string `json:"groupName"`
	GroupFaceURL    string `json:"groupFaceURL,omitempty"`
	FromUserID      string `json:"fromUserID"`
	Nickname        string `json:"nickname"`
	FaceURL         string `json:"faceURL"`
	ReqMsg          string `json:"reqMsg"`
	HandleResult    int32  `json:"handleResult"` // HandleResult*
	HandledUserID   string `json:"handledUserID,omitempty"`
	HandledNickname string `json:"handledNickname,omitempty"`
	Reason          string `json:"reason,omitempty"`
	CreateTime      int64  `json:"createTime"`
	HandleTime      int64  `json:"handleTime,omitempty"`
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

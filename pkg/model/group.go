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

// GroupInfoForSet 设置群组信息参数
type GroupInfoForSet struct {
	GroupID          string `json:"groupID"`
	GroupName        string `json:"groupName,omitempty"`
	Notification     string `json:"notification,omitempty"`
	Introduction     string `json:"introduction,omitempty"`
	FaceURL          string `json:"faceURL,omitempty"`
	RecvMsgOpt       int32  `json:"recvMsgOpt,omitempty"`
	LookMemberInfo   int32  `json:"lookMemberInfo,omitempty"`
	ApplyMemberCheck int32  `json:"applyMemberCheck,omitempty"`
	Ex               string `json:"ex,omitempty"`
}

// GroupInfoEx 扩展群信息（带额外字段）
type GroupInfoEx struct {
	GroupInfo
	NotificationUserInfo *PublicUserInfo `json:"notificationUserInfo,omitempty"`
}

// SetGroupMemberInfo 设置群成员信息
type SetGroupMemberInfo struct {
	GroupID  string `json:"groupID"`
	UserID   string `json:"userID"`
	Nickname string `json:"nickname,omitempty"`
	FaceURL  string `json:"faceURL,omitempty"`
	Ex       string `json:"ex,omitempty"`
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

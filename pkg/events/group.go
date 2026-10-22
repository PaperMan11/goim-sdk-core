package events

// GroupCreatedTips 群组创建
type GroupCreatedTips struct {
	GroupID     string `json:"groupID"`
	GroupName   string `json:"groupName"`
	OwnerUserID string `json:"ownerUserID"`
	MemberCount int32  `json:"memberCount"`
	Notification string `json:"notification,omitempty"`
	FaceURL     string `json:"faceURL,omitempty"`
}

// GroupInfoSetTips 群信息变更
type GroupInfoSetTips struct {
	GroupID         string `json:"groupID"`
	OperatorUserID  string `json:"operatorUserID"`
	GroupName       string `json:"groupName,omitempty"`
	Notification    string `json:"notification,omitempty"`
	FaceURL         string `json:"faceURL,omitempty"`
	Introduction    string `json:"introduction,omitempty"`
	NeedInvite      *int32 `json:"needInvite,omitempty"`
	CanViewMember   *int32 `json:"canViewMember,omitempty"`
	JoinSource      *int32 `json:"joinSource,omitempty"`
}

// JoinGroupApplicationTips 入群申请
type JoinGroupApplicationTips struct {
	GroupID        string `json:"groupID"`
	FromUserID     string `json:"fromUserID"`
	ReqMsg         string `json:"reqMsg,omitempty"`
	OperationID    string `json:"operationID,omitempty"`
	JoinSource     int32  `json:"joinSource"`
	InviterUserID  string `json:"inviterUserID,omitempty"`
	InviterNickname string `json:"inviterNickname,omitempty"`
	InviterFaceURL string `json:"inviterFaceURL,omitempty"`
	FromNickname   string `json:"fromNickname,omitempty"`
	FromFaceURL    string `json:"fromFaceURL,omitempty"`
	CreateTime     int64  `json:"createTime"`
}

// GroupApplicationAcceptedTips 入群申请通过
type GroupApplicationAcceptedTips struct {
	GroupID     string `json:"groupID"`
	FromUserID  string `json:"fromUserID"`
	OpUserID    string `json:"opUserID,omitempty"`
	OpReason    string `json:"opReason,omitempty"`
	OpTime      int64  `json:"opTime"`
	OpUserNickname string `json:"opUserNickname,omitempty"`
	OpUserFaceURL  string `json:"opUserFaceURL,omitempty"`
}

// GroupApplicationRejectedTips 入群申请拒绝
type GroupApplicationRejectedTips struct {
	GroupID     string `json:"groupID"`
	FromUserID  string `json:"fromUserID"`
	OpUserID    string `json:"opUserID,omitempty"`
	OpReason    string `json:"opReason,omitempty"`
	OpTime      int64  `json:"opTime"`
	OpUserNickname string `json:"opUserNickname,omitempty"`
	OpUserFaceURL  string `json:"opUserFaceURL,omitempty"`
}

// MemberEnterTips 成员主动加入
type MemberEnterTips struct {
	GroupID     string `json:"groupID"`
	UserID      string `json:"userID"`
	JoinTime    int64  `json:"joinTime"`
	InviterUserID string `json:"inviterUserID,omitempty"`
}

// MemberInvitedTips 成员被邀请
type MemberInvitedTips struct {
	GroupID        string `json:"groupID"`
	UserID         string `json:"userID"`
	InviterUserID  string `json:"inviterUserID"`
	JoinTime       int64  `json:"joinTime"`
}

// MemberQuitTips 成员主动退出
type MemberQuitTips struct {
	GroupID string `json:"groupID"`
	UserID  string `json:"userID"`
}

// MemberKickedTips 成员被踢出
type MemberKickedTips struct {
	GroupID        string   `json:"groupID"`
	UserIDs        []string `json:"userIDs"`
	OperatorUserID string   `json:"operatorUserID"`
}

// GroupOwnerTransferredTips 群主转让
type GroupOwnerTransferredTips struct {
	GroupID         string `json:"groupID"`
	NewOwnerUserID  string `json:"newOwnerUserID"`
	OldOwnerUserID  string `json:"oldOwnerUserID"`
}

// GroupDismissedTips 群组解散
type GroupDismissedTips struct {
	GroupID        string `json:"groupID"`
	OperatorUserID string `json:"operatorUserID"`
}

// GroupMemberMutedTips 群成员禁言
type GroupMemberMutedTips struct {
	GroupID        string `json:"groupID"`
	UserID         string `json:"userID"`
	MutedSeconds   int32  `json:"mutedSeconds"`
	OperatorUserID string `json:"operatorUserID,omitempty"`
}

// GroupMemberCancelMutedTips 群成员解禁
type GroupMemberCancelMutedTips struct {
	GroupID        string `json:"groupID"`
	UserID         string `json:"userID"`
	OperatorUserID string `json:"operatorUserID,omitempty"`
}

// GroupMutedTips 全员禁言
type GroupMutedTips struct {
	GroupID        string `json:"groupID"`
	OperatorUserID string `json:"operatorUserID"`
}

// GroupCancelMutedTips 取消全员禁言
type GroupCancelMutedTips struct {
	GroupID        string `json:"groupID"`
	OperatorUserID string `json:"operatorUserID"`
}

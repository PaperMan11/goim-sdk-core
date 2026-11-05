package events

import "github.com/PaperMan11/goim-sdk-core/pkg/model"

// —— 群通知 tips ——
// 对齐服务端 sdkws 各 Tips proto 定义。
// 注意：服务端 Tips 内部字段均为嵌套对象（group / opUser / invitedUserList 等），
// 不是扁平的 groupID / userID。此处 model.GroupInfo / model.GroupMemberFullInfo 已与服务端对齐。

// GroupCreatedTips 群组创建
type GroupCreatedTips struct {
	Group                *model.GroupInfo             `json:"group,omitempty"`
	OpUser               *model.GroupMemberFullInfo   `json:"opUser,omitempty"`
	MemberList           []*model.GroupMemberFullInfo `json:"memberList,omitempty"`
	OperationTime        int64                        `json:"operationTime,omitempty"`
	GroupOwnerUser       *model.GroupMemberFullInfo   `json:"groupOwnerUser,omitempty"`
	GroupMemberVersion   uint64                       `json:"groupMemberVersion,omitempty"`
	GroupMemberVersionID string                       `json:"groupMemberVersionID,omitempty"`
}

// GroupInfoSetTips 群信息变更
type GroupInfoSetTips struct {
	OpUser               *model.GroupMemberFullInfo `json:"opUser,omitempty"`
	MuteTime             int64                      `json:"muteTime,omitempty"`
	Group                *model.GroupInfo           `json:"group,omitempty"`
	GroupMemberVersion   uint64                     `json:"groupMemberVersion,omitempty"`
	GroupMemberVersionID string                     `json:"groupMemberVersionID,omitempty"`
}

// JoinGroupApplicationTips 入群申请
type JoinGroupApplicationTips struct {
	Group     *model.GroupInfo      `json:"group,omitempty"`
	Applicant *model.PublicUserInfo `json:"applicant,omitempty"`
	ReqMsg    string                `json:"reqMsg,omitempty"`
	Uuid      string                `json:"uuid,omitempty"`
	Request   *model.GroupRequest   `json:"request,omitempty"`
}

// GroupApplicationAcceptedTips 入群申请通过
type GroupApplicationAcceptedTips struct {
	Group      *model.GroupInfo           `json:"group,omitempty"`
	OpUser     *model.GroupMemberFullInfo `json:"opUser,omitempty"`
	HandleMsg  string                     `json:"handleMsg,omitempty"`
	ReceiverAs int32                      `json:"receiverAs,omitempty"`
	Uuid       string                     `json:"uuid,omitempty"`
	Request    *model.GroupRequest        `json:"request,omitempty"`
}

// GroupApplicationRejectedTips 入群申请拒绝
type GroupApplicationRejectedTips struct {
	Group      *model.GroupInfo           `json:"group,omitempty"`
	OpUser     *model.GroupMemberFullInfo `json:"opUser,omitempty"`
	HandleMsg  string                     `json:"handleMsg,omitempty"`
	ReceiverAs int32                      `json:"receiverAs,omitempty"`
	Uuid       string                     `json:"uuid,omitempty"`
	Request    *model.GroupRequest        `json:"request,omitempty"`
}

// MemberEnterTips 成员主动加入
type MemberEnterTips struct {
	Group                *model.GroupInfo           `json:"group,omitempty"`
	EntrantUser          *model.GroupMemberFullInfo `json:"entrantUser,omitempty"`
	OperationTime        int64                      `json:"operationTime,omitempty"`
	GroupMemberVersion   uint64                     `json:"groupMemberVersion,omitempty"`
	GroupMemberVersionID string                     `json:"groupMemberVersionID,omitempty"`
}

// MemberInvitedTips 成员被邀请
type MemberInvitedTips struct {
	Group                *model.GroupInfo             `json:"group,omitempty"`
	OpUser               *model.GroupMemberFullInfo   `json:"opUser,omitempty"`
	InvitedUserList      []*model.GroupMemberFullInfo `json:"invitedUserList,omitempty"`
	OperationTime        int64                        `json:"operationTime,omitempty"`
	GroupMemberVersion   uint64                       `json:"groupMemberVersion,omitempty"`
	GroupMemberVersionID string                       `json:"groupMemberVersionID,omitempty"`
	InviterUser          *model.GroupMemberFullInfo   `json:"inviterUser,omitempty"`
}

// MemberQuitTips 成员主动退出
type MemberQuitTips struct {
	Group                *model.GroupInfo           `json:"group,omitempty"`
	QuitUser             *model.GroupMemberFullInfo `json:"quitUser,omitempty"`
	OperationTime        int64                      `json:"operationTime,omitempty"`
	GroupMemberVersion   uint64                     `json:"groupMemberVersion,omitempty"`
	GroupMemberVersionID string                     `json:"groupMemberVersionID,omitempty"`
}

// MemberKickedTips 成员被踢出
type MemberKickedTips struct {
	Group                *model.GroupInfo             `json:"group,omitempty"`
	OpUser               *model.GroupMemberFullInfo   `json:"opUser,omitempty"`
	KickedUserList       []*model.GroupMemberFullInfo `json:"kickedUserList,omitempty"`
	OperationTime        int64                        `json:"operationTime,omitempty"`
	GroupMemberVersion   uint64                       `json:"groupMemberVersion,omitempty"`
	GroupMemberVersionID string                       `json:"groupMemberVersionID,omitempty"`
}

// GroupOwnerTransferredTips 群主转让
type GroupOwnerTransferredTips struct {
	Group                *model.GroupInfo           `json:"group,omitempty"`
	OpUser               *model.GroupMemberFullInfo `json:"opUser,omitempty"`
	NewGroupOwner        *model.GroupMemberFullInfo `json:"newGroupOwner,omitempty"`
	OldGroupOwner        string                     `json:"oldGroupOwner,omitempty"`
	OperationTime        int64                      `json:"operationTime,omitempty"`
	OldGroupOwnerInfo    *model.GroupMemberFullInfo `json:"oldGroupOwnerInfo,omitempty"`
	GroupMemberVersion   uint64                     `json:"groupMemberVersion,omitempty"`
	GroupMemberVersionID string                     `json:"groupMemberVersionID,omitempty"`
}

// GroupDismissedTips 群组解散
type GroupDismissedTips struct {
	Group         *model.GroupInfo           `json:"group,omitempty"`
	OpUser        *model.GroupMemberFullInfo `json:"opUser,omitempty"`
	OperationTime int64                      `json:"operationTime,omitempty"`
}

// GroupMemberMutedTips 群成员禁言
type GroupMemberMutedTips struct {
	Group                *model.GroupInfo           `json:"group,omitempty"`
	OpUser               *model.GroupMemberFullInfo `json:"opUser,omitempty"`
	OperationTime        int64                      `json:"operationTime,omitempty"`
	MutedUser            *model.GroupMemberFullInfo `json:"mutedUser,omitempty"`
	MutedSeconds         uint32                     `json:"mutedSeconds,omitempty"`
	GroupMemberVersion   uint64                     `json:"groupMemberVersion,omitempty"`
	GroupMemberVersionID string                     `json:"groupMemberVersionID,omitempty"`
}

// GroupMemberCancelMutedTips 群成员解禁
type GroupMemberCancelMutedTips struct {
	Group                *model.GroupInfo           `json:"group,omitempty"`
	OpUser               *model.GroupMemberFullInfo `json:"opUser,omitempty"`
	OperationTime        int64                      `json:"operationTime,omitempty"`
	MutedUser            *model.GroupMemberFullInfo `json:"mutedUser,omitempty"`
	GroupMemberVersion   uint64                     `json:"groupMemberVersion,omitempty"`
	GroupMemberVersionID string                     `json:"groupMemberVersionID,omitempty"`
}

// GroupMutedTips 全员禁言
type GroupMutedTips struct {
	Group                *model.GroupInfo           `json:"group,omitempty"`
	OpUser               *model.GroupMemberFullInfo `json:"opUser,omitempty"`
	OperationTime        int64                      `json:"operationTime,omitempty"`
	GroupMemberVersion   uint64                     `json:"groupMemberVersion,omitempty"`
	GroupMemberVersionID string                     `json:"groupMemberVersionID,omitempty"`
}

// GroupCancelMutedTips 取消全员禁言
type GroupCancelMutedTips struct {
	Group                *model.GroupInfo           `json:"group,omitempty"`
	OpUser               *model.GroupMemberFullInfo `json:"opUser,omitempty"`
	OperationTime        int64                      `json:"operationTime,omitempty"`
	GroupMemberVersion   uint64                     `json:"groupMemberVersion,omitempty"`
	GroupMemberVersionID string                     `json:"groupMemberVersionID,omitempty"`
}

package manager

import (
	"context"

	"github.com/PaperMan11/goim-sdk-core/pkg/model"
)

// GroupMgr 群组 CRUD / 成员管理 / 增量同步
//
// 实现要点:
//   - 22 个 HTTP 接口 + 增量同步接口
//   - GetIncrementalJoinGroups / GetIncrementalGroupMembers 需本地 versionID 配合
//   - GetFullGroupMemberUserIDs 在服务端返回 full=true 或 idHash 变化时触发
type GroupMgr interface {
	CreateGroup(ctx context.Context, groupInfo *model.GroupInfo, memberUserIDs, adminUserIDs []string, ownerUserID string, sendMessage bool) (*model.GroupInfo, error)
	SetGroupInfo(ctx context.Context, groupInfo *model.GroupInfoForSet) error
	SetGroupInfoEx(ctx context.Context, groupID string, groupInfoEx *model.GroupInfoEx) error
	JoinGroup(ctx context.Context, groupID, reqMessage string, joinSource int32, inviterUserID, ex string) error
	QuitGroup(ctx context.Context, groupID, userID string) error
	GroupApplicationResponse(ctx context.Context, groupID, fromUserID, handledMsg string, handleResult int32) error
	TransferGroup(ctx context.Context, groupID string, oldOwnerUserID, newOwnerUserID string) error
	GetGroupsInfo(ctx context.Context, groupIDs []string) ([]*model.GroupInfo, error)
	KickGroup(ctx context.Context, groupID string, kickedUserIDs []string, reason string, sendMessage *bool) error
	GetGroupMembersInfo(ctx context.Context, groupID string, userIDs []string) ([]*model.GroupMemberFullInfo, error)
	GetGroupMemberList(ctx context.Context, pagination *model.RequestPagination, groupID string, filter int32, keyword string) (total uint32, members []*model.GroupMemberFullInfo, err error)
	InviteUserToGroup(ctx context.Context, groupID, reason string, invitedUserIDs []string, sendMessage *bool) error
	GetJoinedGroupList(ctx context.Context, pagination *model.RequestPagination) (total int32, groups []*model.GroupInfo, err error)
	DismissGroup(ctx context.Context, groupID string, deleteMember bool, sendMessage *bool) error
	MuteGroupMember(ctx context.Context, groupID, userID string, mutedSeconds uint32) error
	MuteGroup(ctx context.Context, groupID string) error
	CancelMuteGroup(ctx context.Context, groupID string) error
	CancelMuteGroupMember(ctx context.Context, groupID, userID string) error
	SetGroupMemberInfo(ctx context.Context, members []*model.SetGroupMemberInfo) error
	GetGroupAbstractInfo(ctx context.Context, groupIDs []string) ([]*model.GroupAbstractInfo, error)
	GetIncrementalJoinGroups(ctx context.Context, userID, versionID string, version uint64) (*model.IncrementalGroups, error)
	GetIncrementalGroupMembers(ctx context.Context, groupID, versionID string, version uint64) (*model.IncrementalMembers, error)
	GetFullGroupMemberUserIDs(ctx context.Context, groupID string, idHash uint64) (*model.FullMemberIDs, error)
}

package manager

import (
	"context"
	"fmt"
	"time"

	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/eventbus"
)

// DefaultGroupMgr GroupMgr 默认实现（纯 HTTP）
type DefaultGroupMgr struct {
	http   transport.HTTPClient
	bus    *eventbus.EventBus
	logger logger.Logger
}

// NewGroupMgr 构造 GroupMgr
func NewGroupMgr(http transport.HTTPClient, bus *eventbus.EventBus, l logger.Logger) *DefaultGroupMgr {
	return &DefaultGroupMgr{http: http, bus: bus, logger: l}
}

func (m *DefaultGroupMgr) CreateGroup(ctx context.Context, groupInfo *model.GroupInfo, memberUserIDs, adminUserIDs []string, ownerUserID string, sendMessage bool) (*model.GroupInfo, error) {
	req := struct {
		GroupInfo    *model.GroupInfo `json:"groupInfo"`
		MemberUserIDs []string        `json:"memberUserIDs"`
		AdminUserIDs  []string        `json:"adminUserIDs"`
		OwnerUserID  string           `json:"ownerUserID"`
		SendMessage  bool             `json:"sendMessage"`
	}{GroupInfo: groupInfo, MemberUserIDs: memberUserIDs, AdminUserIDs: adminUserIDs, OwnerUserID: ownerUserID, SendMessage: sendMessage}
	var resp model.GroupInfo
	if err := m.http.Post(ctx, "/group/create_group", req, &resp); err != nil {
		return nil, fmt.Errorf("create group: %w", err)
	}
	if m.bus != nil {
		m.bus.Publish(events.EventGroupCreated, events.Event{
			Type:      events.EventGroupCreated,
			Timestamp: time.Now().UnixMilli(),
			Payload:   &resp,
		})
	}
	return &resp, nil
}

func (m *DefaultGroupMgr) SetGroupInfo(ctx context.Context, groupInfo *model.GroupInfoForSet) error {
	if err := m.http.Post(ctx, "/group/set_group_info", groupInfo, nil); err != nil {
		return fmt.Errorf("set group info: %w", err)
	}
	if m.bus != nil {
		m.bus.Publish(events.EventGroupInfoSet, events.Event{
			Type:      events.EventGroupInfoSet,
			Timestamp: time.Now().UnixMilli(),
			Payload:   groupInfo,
		})
	}
	return nil
}

func (m *DefaultGroupMgr) SetGroupInfoEx(ctx context.Context, groupID string, groupInfoEx *model.GroupInfoEx) error {
	req := struct {
		GroupID      string            `json:"groupID"`
		GroupInfoEx  *model.GroupInfoEx `json:"groupInfoEx"`
	}{GroupID: groupID, GroupInfoEx: groupInfoEx}
	if err := m.http.Post(ctx, "/group/set_group_info_ex", req, nil); err != nil {
		return fmt.Errorf("set group info ex: %w", err)
	}
	return nil
}

func (m *DefaultGroupMgr) JoinGroup(ctx context.Context, groupID, reqMessage string, joinSource int32, inviterUserID, ex string) error {
	req := struct {
		GroupID       string `json:"groupID"`
		ReqMessage    string `json:"reqMessage"`
		JoinSource    int32  `json:"joinSource"`
		InviterUserID string `json:"inviterUserID"`
		Ex            string `json:"ex"`
	}{GroupID: groupID, ReqMessage: reqMessage, JoinSource: joinSource, InviterUserID: inviterUserID, Ex: ex}
	if err := m.http.Post(ctx, "/group/join_group", req, nil); err != nil {
		return fmt.Errorf("join group: %w", err)
	}
	return nil
}

func (m *DefaultGroupMgr) QuitGroup(ctx context.Context, groupID, userID string) error {
	req := struct {
		GroupID string `json:"groupID"`
		UserID  string `json:"userID"`
	}{GroupID: groupID, UserID: userID}
	if err := m.http.Post(ctx, "/group/quit_group", req, nil); err != nil {
		return fmt.Errorf("quit group: %w", err)
	}
	return nil
}

func (m *DefaultGroupMgr) GroupApplicationResponse(ctx context.Context, groupID, fromUserID, handledMsg string, handleResult int32) error {
	req := struct {
		GroupID      string `json:"groupID"`
		FromUserID   string `json:"fromUserID"`
		HandledMsg   string `json:"handledMsg"`
		HandleResult int32  `json:"handleResult"`
	}{GroupID: groupID, FromUserID: fromUserID, HandledMsg: handledMsg, HandleResult: handleResult}
	if err := m.http.Post(ctx, "/group/group_application_response", req, nil); err != nil {
		return fmt.Errorf("group application response: %w", err)
	}
	return nil
}

func (m *DefaultGroupMgr) TransferGroup(ctx context.Context, groupID string, oldOwnerUserID, newOwnerUserID string) error {
	req := struct {
		GroupID        string `json:"groupID"`
		OldOwnerUserID string `json:"oldOwnerUserID"`
		NewOwnerUserID string `json:"newOwnerUserID"`
	}{GroupID: groupID, OldOwnerUserID: oldOwnerUserID, NewOwnerUserID: newOwnerUserID}
	if err := m.http.Post(ctx, "/group/transfer_group", req, nil); err != nil {
		return fmt.Errorf("transfer group: %w", err)
	}
	return nil
}

func (m *DefaultGroupMgr) GetGroupsInfo(ctx context.Context, groupIDs []string) ([]*model.GroupInfo, error) {
	req := struct {
		GroupIDs []string `json:"groupIDs"`
	}{GroupIDs: groupIDs}
	var resp []*model.GroupInfo
	if err := m.http.Post(ctx, "/group/get_groups_info", req, &resp); err != nil {
		return nil, fmt.Errorf("get groups info: %w", err)
	}
	return resp, nil
}

func (m *DefaultGroupMgr) KickGroup(ctx context.Context, groupID string, kickedUserIDs []string, reason string, sendMessage *bool) error {
	req := struct {
		GroupID       string   `json:"groupID"`
		KickedUserIDs []string `json:"kickedUserIDs"`
		Reason        string   `json:"reason"`
		SendMessage   *bool    `json:"sendMessage"`
	}{GroupID: groupID, KickedUserIDs: kickedUserIDs, Reason: reason, SendMessage: sendMessage}
	if err := m.http.Post(ctx, "/group/kick_group", req, nil); err != nil {
		return fmt.Errorf("kick group: %w", err)
	}
	return nil
}

func (m *DefaultGroupMgr) GetGroupMembersInfo(ctx context.Context, groupID string, userIDs []string) ([]*model.GroupMemberFullInfo, error) {
	req := struct {
		GroupID string   `json:"groupID"`
		UserIDs []string `json:"userIDs"`
	}{GroupID: groupID, UserIDs: userIDs}
	var resp []*model.GroupMemberFullInfo
	if err := m.http.Post(ctx, "/group/get_group_members_info", req, &resp); err != nil {
		return nil, fmt.Errorf("get group members info: %w", err)
	}
	return resp, nil
}

func (m *DefaultGroupMgr) GetGroupMemberList(ctx context.Context, pagination *model.RequestPagination, groupID string, filter int32, keyword string) (uint32, []*model.GroupMemberFullInfo, error) {
	req := struct {
		Pagination *model.RequestPagination `json:"pagination"`
		GroupID    string                   `json:"groupID"`
		Filter     int32                    `json:"filter"`
		Keyword    string                   `json:"keyword"`
	}{Pagination: pagination, GroupID: groupID, Filter: filter, Keyword: keyword}
	var resp struct {
		Total   uint32                       `json:"total"`
		Members []*model.GroupMemberFullInfo `json:"members"`
	}
	if err := m.http.Post(ctx, "/group/get_group_member_list", req, &resp); err != nil {
		return 0, nil, fmt.Errorf("get group member list: %w", err)
	}
	return resp.Total, resp.Members, nil
}

func (m *DefaultGroupMgr) InviteUserToGroup(ctx context.Context, groupID, reason string, invitedUserIDs []string, sendMessage *bool) error {
	req := struct {
		GroupID        string   `json:"groupID"`
		Reason         string   `json:"reason"`
		InvitedUserIDs []string `json:"invitedUserIDs"`
		SendMessage    *bool    `json:"sendMessage"`
	}{GroupID: groupID, Reason: reason, InvitedUserIDs: invitedUserIDs, SendMessage: sendMessage}
	if err := m.http.Post(ctx, "/group/invite_user_to_group", req, nil); err != nil {
		return fmt.Errorf("invite user to group: %w", err)
	}
	if m.bus != nil {
		m.bus.Publish(events.EventGroupMemberInvited, events.Event{
			Type:      events.EventGroupMemberInvited,
			Timestamp: time.Now().UnixMilli(),
		})
	}
	return nil
}

func (m *DefaultGroupMgr) GetJoinedGroupList(ctx context.Context, pagination *model.RequestPagination) (int32, []*model.GroupInfo, error) {
	req := struct {
		Pagination *model.RequestPagination `json:"pagination"`
	}{Pagination: pagination}
	var resp struct {
		Total  int32             `json:"total"`
		Groups []*model.GroupInfo `json:"groups"`
	}
	if err := m.http.Post(ctx, "/group/get_joined_group_list", req, &resp); err != nil {
		return 0, nil, fmt.Errorf("get joined group list: %w", err)
	}
	return resp.Total, resp.Groups, nil
}

func (m *DefaultGroupMgr) DismissGroup(ctx context.Context, groupID string, deleteMember bool, sendMessage *bool) error {
	req := struct {
		GroupID      string `json:"groupID"`
		DeleteMember bool   `json:"deleteMember"`
		SendMessage  *bool  `json:"sendMessage"`
	}{GroupID: groupID, DeleteMember: deleteMember, SendMessage: sendMessage}
	if err := m.http.Post(ctx, "/group/dismiss_group", req, nil); err != nil {
		return fmt.Errorf("dismiss group: %w", err)
	}
	return nil
}

func (m *DefaultGroupMgr) MuteGroupMember(ctx context.Context, groupID, userID string, mutedSeconds uint32) error {
	req := struct {
		GroupID      string `json:"groupID"`
		UserID       string `json:"userID"`
		MutedSeconds uint32 `json:"mutedSeconds"`
	}{GroupID: groupID, UserID: userID, MutedSeconds: mutedSeconds}
	if err := m.http.Post(ctx, "/group/mute_group_member", req, nil); err != nil {
		return fmt.Errorf("mute group member: %w", err)
	}
	return nil
}

func (m *DefaultGroupMgr) MuteGroup(ctx context.Context, groupID string) error {
	req := struct {
		GroupID string `json:"groupID"`
	}{GroupID: groupID}
	if err := m.http.Post(ctx, "/group/mute_group", req, nil); err != nil {
		return fmt.Errorf("mute group: %w", err)
	}
	return nil
}

func (m *DefaultGroupMgr) CancelMuteGroup(ctx context.Context, groupID string) error {
	req := struct {
		GroupID string `json:"groupID"`
	}{GroupID: groupID}
	if err := m.http.Post(ctx, "/group/cancel_mute_group", req, nil); err != nil {
		return fmt.Errorf("cancel mute group: %w", err)
	}
	return nil
}

func (m *DefaultGroupMgr) CancelMuteGroupMember(ctx context.Context, groupID, userID string) error {
	req := struct {
		GroupID string `json:"groupID"`
		UserID  string `json:"userID"`
	}{GroupID: groupID, UserID: userID}
	if err := m.http.Post(ctx, "/group/cancel_mute_group_member", req, nil); err != nil {
		return fmt.Errorf("cancel mute group member: %w", err)
	}
	return nil
}

func (m *DefaultGroupMgr) SetGroupMemberInfo(ctx context.Context, members []*model.SetGroupMemberInfo) error {
	req := struct {
		Members []*model.SetGroupMemberInfo `json:"members"`
	}{Members: members}
	if err := m.http.Post(ctx, "/group/set_group_member_info", req, nil); err != nil {
		return fmt.Errorf("set group member info: %w", err)
	}
	return nil
}

func (m *DefaultGroupMgr) GetGroupAbstractInfo(ctx context.Context, groupIDs []string) ([]*model.GroupAbstractInfo, error) {
	req := struct {
		GroupIDs []string `json:"groupIDs"`
	}{GroupIDs: groupIDs}
	var resp []*model.GroupAbstractInfo
	if err := m.http.Post(ctx, "/group/get_group_abstract_info", req, &resp); err != nil {
		return nil, fmt.Errorf("get group abstract info: %w", err)
	}
	return resp, nil
}

func (m *DefaultGroupMgr) GetIncrementalJoinGroups(ctx context.Context, userID, versionID string, version uint64) (*model.IncrementalGroups, error) {
	req := struct {
		UserID    string `json:"userID"`
		VersionID string `json:"versionID"`
		Version   uint64 `json:"version"`
	}{UserID: userID, VersionID: versionID, Version: version}
	var resp model.IncrementalGroups
	if err := m.http.Post(ctx, "/group/get_incremental_join_groups", req, &resp); err != nil {
		return nil, fmt.Errorf("get incremental join groups: %w", err)
	}
	return &resp, nil
}

func (m *DefaultGroupMgr) GetIncrementalGroupMembers(ctx context.Context, groupID, versionID string, version uint64) (*model.IncrementalMembers, error) {
	req := struct {
		GroupID   string `json:"groupID"`
		VersionID string `json:"versionID"`
		Version   uint64 `json:"version"`
	}{GroupID: groupID, VersionID: versionID, Version: version}
	var resp model.IncrementalMembers
	if err := m.http.Post(ctx, "/group/get_incremental_group_members", req, &resp); err != nil {
		return nil, fmt.Errorf("get incremental group members: %w", err)
	}
	return &resp, nil
}

func (m *DefaultGroupMgr) GetFullGroupMemberUserIDs(ctx context.Context, groupID string, idHash uint64) (*model.FullMemberIDs, error) {
	req := struct {
		GroupID string `json:"groupID"`
		IDHash  uint64 `json:"idHash"`
	}{GroupID: groupID, IDHash: idHash}
	var resp model.FullMemberIDs
	if err := m.http.Post(ctx, "/group/get_full_group_member_user_ids", req, &resp); err != nil {
		return nil, fmt.Errorf("get full group member user ids: %w", err)
	}
	return &resp, nil
}

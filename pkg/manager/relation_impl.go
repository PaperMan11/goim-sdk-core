package manager

import (
	"context"
	"fmt"

	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/eventbus"
)

// DefaultRelMgr RelMgr 默认实现（纯 HTTP）
type DefaultRelMgr struct {
	http   transport.HTTPClient
	bus    *eventbus.EventBus
	logger logger.Logger
}

// NewRelMgr 构造 RelMgr
func NewRelMgr(http transport.HTTPClient, bus *eventbus.EventBus, l logger.Logger) *DefaultRelMgr {
	return &DefaultRelMgr{http: http, bus: bus, logger: l}
}

func (m *DefaultRelMgr) AddFriend(ctx context.Context, fromUserID, toUserID, reqMsg, ex string) error {
	req := struct {
		FromUserID string `json:"fromUserID"`
		ToUserID   string `json:"toUserID"`
		ReqMsg     string `json:"reqMsg"`
		Ex         string `json:"ex"`
	}{FromUserID: fromUserID, ToUserID: toUserID, ReqMsg: reqMsg, Ex: ex}
	if err := m.http.Post(ctx, "/relation/add_friend", req, nil); err != nil {
		return fmt.Errorf("add friend: %w", err)
	}
	return nil
}

func (m *DefaultRelMgr) AddFriendResponse(ctx context.Context, fromUserID, toUserID, handleMsg string, handleResult int32) error {
	req := struct {
		FromUserID   string `json:"fromUserID"`
		ToUserID     string `json:"toUserID"`
		HandleMsg    string `json:"handleMsg"`
		HandleResult int32  `json:"handleResult"`
	}{FromUserID: fromUserID, ToUserID: toUserID, HandleMsg: handleMsg, HandleResult: handleResult}
	if err := m.http.Post(ctx, "/relation/add_friend_response", req, nil); err != nil {
		return fmt.Errorf("add friend response: %w", err)
	}
	return nil
}

func (m *DefaultRelMgr) DeleteFriend(ctx context.Context, ownerUserID, friendUserID string) error {
	req := struct {
		OwnerUserID  string `json:"ownerUserID"`
		FriendUserID string `json:"friendUserID"`
	}{OwnerUserID: ownerUserID, FriendUserID: friendUserID}
	if err := m.http.Post(ctx, "/relation/delete_friend", req, nil); err != nil {
		return fmt.Errorf("delete friend: %w", err)
	}
	return nil
}

func (m *DefaultRelMgr) GetFriendList(ctx context.Context, userID string, pagination *model.RequestPagination) (int32, []*model.FriendInfo, error) {
	req := struct {
		UserID     string                   `json:"userID"`
		Pagination *model.RequestPagination `json:"pagination"`
	}{UserID: userID, Pagination: pagination}
	var resp struct {
		Total   int32             `json:"total"`
		Friends []*model.FriendInfo `json:"friends"`
	}
	if err := m.http.Post(ctx, "/relation/get_friend_list", req, &resp); err != nil {
		return 0, nil, fmt.Errorf("get friend list: %w", err)
	}
	return resp.Total, resp.Friends, nil
}

func (m *DefaultRelMgr) GetFriendApplyList(ctx context.Context, userID string, pagination *model.RequestPagination, handleResults []int32) (int32, []*model.FriendRequest, error) {
	req := struct {
		UserID        string                   `json:"userID"`
		Pagination    *model.RequestPagination `json:"pagination"`
		HandleResults []int32                  `json:"handleResults"`
	}{UserID: userID, Pagination: pagination, HandleResults: handleResults}
	var resp struct {
		Total    int32                      `json:"total"`
		Requests []*model.FriendRequest     `json:"requests"`
	}
	if err := m.http.Post(ctx, "/relation/get_friend_apply_list", req, &resp); err != nil {
		return 0, nil, fmt.Errorf("get friend apply list: %w", err)
	}
	return resp.Total, resp.Requests, nil
}

func (m *DefaultRelMgr) GetSelfFriendApplyList(ctx context.Context, userID string, pagination *model.RequestPagination, handleResults []int32) (int32, []*model.FriendRequest, error) {
	req := struct {
		UserID        string                   `json:"userID"`
		Pagination    *model.RequestPagination `json:"pagination"`
		HandleResults []int32                  `json:"handleResults"`
	}{UserID: userID, Pagination: pagination, HandleResults: handleResults}
	var resp struct {
		Total    int32                      `json:"total"`
		Requests []*model.FriendRequest     `json:"requests"`
	}
	if err := m.http.Post(ctx, "/relation/get_self_friend_apply_list", req, &resp); err != nil {
		return 0, nil, fmt.Errorf("get self friend apply list: %w", err)
	}
	return resp.Total, resp.Requests, nil
}

func (m *DefaultRelMgr) SetFriendRemark(ctx context.Context, ownerUserID, friendUserID, remark string) error {
	req := struct {
		OwnerUserID  string `json:"ownerUserID"`
		FriendUserID string `json:"friendUserID"`
		Remark       string `json:"remark"`
	}{OwnerUserID: ownerUserID, FriendUserID: friendUserID, Remark: remark}
	if err := m.http.Post(ctx, "/relation/set_friend_remark", req, nil); err != nil {
		return fmt.Errorf("set friend remark: %w", err)
	}
	return nil
}

func (m *DefaultRelMgr) AddBlack(ctx context.Context, ownerUserID, blackUserID, ex string) error {
	req := struct {
		OwnerUserID  string `json:"ownerUserID"`
		BlackUserID  string `json:"blackUserID"`
		Ex           string `json:"ex"`
	}{OwnerUserID: ownerUserID, BlackUserID: blackUserID, Ex: ex}
	if err := m.http.Post(ctx, "/relation/add_black", req, nil); err != nil {
		return fmt.Errorf("add black: %w", err)
	}
	return nil
}

func (m *DefaultRelMgr) RemoveBlack(ctx context.Context, ownerUserID, blackUserID string) error {
	req := struct {
		OwnerUserID string `json:"ownerUserID"`
		BlackUserID string `json:"blackUserID"`
	}{OwnerUserID: ownerUserID, BlackUserID: blackUserID}
	if err := m.http.Post(ctx, "/relation/remove_black", req, nil); err != nil {
		return fmt.Errorf("remove black: %w", err)
	}
	return nil
}

func (m *DefaultRelMgr) GetBlackList(ctx context.Context, userID string, pagination *model.RequestPagination) (int32, []*model.BlackInfo, error) {
	req := struct {
		UserID     string                   `json:"userID"`
		Pagination *model.RequestPagination `json:"pagination"`
	}{UserID: userID, Pagination: pagination}
	var resp struct {
		Total  int32             `json:"total"`
		Blacks []*model.BlackInfo `json:"blacks"`
	}
	if err := m.http.Post(ctx, "/relation/get_black_list", req, &resp); err != nil {
		return 0, nil, fmt.Errorf("get black list: %w", err)
	}
	return resp.Total, resp.Blacks, nil
}

func (m *DefaultRelMgr) IsFriend(ctx context.Context, userID1, userID2 string) (*model.FriendRelation, error) {
	req := struct {
		UserID1 string `json:"userID1"`
		UserID2 string `json:"userID2"`
	}{UserID1: userID1, UserID2: userID2}
	var resp model.FriendRelation
	if err := m.http.Post(ctx, "/relation/is_friend", req, &resp); err != nil {
		return nil, fmt.Errorf("is friend: %w", err)
	}
	return &resp, nil
}

func (m *DefaultRelMgr) GetFriendIDs(ctx context.Context, userID string) ([]string, error) {
	req := struct {
		UserID string `json:"userID"`
	}{UserID: userID}
	var resp []string
	if err := m.http.Post(ctx, "/relation/get_friend_id", req, &resp); err != nil {
		return nil, fmt.Errorf("get friend ids: %w", err)
	}
	return resp, nil
}

func (m *DefaultRelMgr) GetIncrementalFriends(ctx context.Context, userID, versionID string, version uint64) (*model.IncrementalFriends, error) {
	req := struct {
		UserID    string `json:"userID"`
		VersionID string `json:"versionID"`
		Version   uint64 `json:"version"`
	}{UserID: userID, VersionID: versionID, Version: version}
	var resp model.IncrementalFriends
	if err := m.http.Post(ctx, "/relation/get_incremental_friends", req, &resp); err != nil {
		return nil, fmt.Errorf("get incremental friends: %w", err)
	}
	return &resp, nil
}

func (m *DefaultRelMgr) GetIncrementalBlacks(ctx context.Context, userID, versionID string, version uint64) (*model.IncrementalBlacks, error) {
	req := struct {
		UserID    string `json:"userID"`
		VersionID string `json:"versionID"`
		Version   uint64 `json:"version"`
	}{UserID: userID, VersionID: versionID, Version: version}
	var resp model.IncrementalBlacks
	if err := m.http.Post(ctx, "/relation/get_incremental_blacks", req, &resp); err != nil {
		return nil, fmt.Errorf("get incremental blacks: %w", err)
	}
	return &resp, nil
}

func (m *DefaultRelMgr) ImportFriend(ctx context.Context, ownerUserID string, friendUserIDs []string) error {
	req := struct {
		OwnerUserID   string   `json:"ownerUserID"`
		FriendUserIDs []string `json:"friendUserIDs"`
	}{OwnerUserID: ownerUserID, FriendUserIDs: friendUserIDs}
	if err := m.http.Post(ctx, "/relation/import_friend", req, nil); err != nil {
		return fmt.Errorf("import friend: %w", err)
	}
	return nil
}

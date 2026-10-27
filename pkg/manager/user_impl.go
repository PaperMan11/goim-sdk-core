package manager

import (
	"context"
	"fmt"

	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/eventbus"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/timex"
)

// DefaultUserMgr UserMgr 默认实现（纯 HTTP）
type DefaultUserMgr struct {
	http   transport.HTTPClient
	bus    *eventbus.EventBus
	logger logger.Logger
}

// NewUserMgr 构造 UserMgr
func NewUserMgr(http transport.HTTPClient, bus *eventbus.EventBus, l logger.Logger) *DefaultUserMgr {
	return &DefaultUserMgr{http: http, bus: bus, logger: l}
}

func (m *DefaultUserMgr) Register(ctx context.Context, users []*model.UserInfo) error {
	req := struct {
		Users []*model.UserInfo `json:"users"`
	}{Users: users}
	if err := m.http.Post(ctx, "/user/user_register", req, nil); err != nil {
		return fmt.Errorf("register users: %w", err)
	}
	return nil
}

func (m *DefaultUserMgr) UpdateUserInfo(ctx context.Context, userInfo *model.UserInfo) error {
	if err := m.http.Post(ctx, "/user/update_user_info", userInfo, nil); err != nil {
		return fmt.Errorf("update user info: %w", err)
	}
	if m.bus != nil {
		m.bus.Publish(events.EventUserInfoUpdated, events.Event{
			Type:      events.EventUserInfoUpdated,
			Timestamp: timex.UnixMilli(),
			Payload:   userInfo,
		})
	}
	return nil
}

func (m *DefaultUserMgr) UpdateUserInfoEx(ctx context.Context, userInfo *model.UserInfoWithEx) error {
	if err := m.http.Post(ctx, "/user/update_user_info_ex", userInfo, nil); err != nil {
		return fmt.Errorf("update user info ex: %w", err)
	}
	return nil
}

func (m *DefaultUserMgr) SetGlobalMsgRecvOpt(ctx context.Context, userID string, opt int32) error {
	req := struct {
		UserID string `json:"userID"`
		Opt    int32  `json:"opt"`
	}{UserID: userID, Opt: opt}
	if err := m.http.Post(ctx, "/user/set_global_msg_recv_opt", req, nil); err != nil {
		return fmt.Errorf("set global msg recv opt: %w", err)
	}
	return nil
}

func (m *DefaultUserMgr) GetUsersInfo(ctx context.Context, userIDs []string) ([]*model.UserInfo, error) {
	req := struct {
		UserIDs []string `json:"userIDs"`
	}{UserIDs: userIDs}
	var resp struct {
		UsersInfo []*model.UserInfo `json:"usersInfo"`
	}
	if err := m.http.Post(ctx, "/user/get_users_info", req, &resp); err != nil {
		return nil, fmt.Errorf("get users info: %w", err)
	}
	return resp.UsersInfo, nil
}

func (m *DefaultUserMgr) GetAllUsersUID(ctx context.Context, pagination *model.RequestPagination) (int32, []string, error) {
	req := struct {
		Pagination *model.RequestPagination `json:"pagination"`
	}{Pagination: pagination}
	var resp struct {
		Total   int32    `json:"total"`
		UserIDs []string `json:"userIDs"`
	}
	if err := m.http.Post(ctx, "/user/get_all_users_uid", req, &resp); err != nil {
		return 0, nil, fmt.Errorf("get all users uid: %w", err)
	}
	return resp.Total, resp.UserIDs, nil
}

func (m *DefaultUserMgr) AccountCheck(ctx context.Context, checkUserIDs []string) ([]*model.AccountStatus, error) {
	req := struct {
		CheckUserIDs []string `json:"checkUserIDs"`
	}{CheckUserIDs: checkUserIDs}
	var resp struct {
		Results []*model.AccountStatus `json:"results"`
	}
	if err := m.http.Post(ctx, "/user/account_check", req, &resp); err != nil {
		return nil, fmt.Errorf("account check: %w", err)
	}
	return resp.Results, nil
}

func (m *DefaultUserMgr) GetUsers(ctx context.Context, pagination *model.RequestPagination, userID, nickName string) (int32, []*model.UserInfo, error) {
	req := struct {
		Pagination *model.RequestPagination `json:"pagination"`
		UserID     string                   `json:"userID"`
		NickName   string                   `json:"nickName"`
	}{Pagination: pagination, UserID: userID, NickName: nickName}
	var resp struct {
		Total int32             `json:"total"`
		Users []*model.UserInfo `json:"users"`
	}
	if err := m.http.Post(ctx, "/user/get_users", req, &resp); err != nil {
		return 0, nil, fmt.Errorf("get users: %w", err)
	}
	return resp.Total, resp.Users, nil
}

func (m *DefaultUserMgr) GetUsersOnlineStatus(ctx context.Context, userID string, userIDs []string) ([]*model.OnlineStatus, error) {
	req := struct {
		UserID  string   `json:"userID"`
		UserIDs []string `json:"userIDs"`
	}{UserID: userID, UserIDs: userIDs}
	var resp struct {
		SuccessResult []*model.OnlineStatus `json:"successResult"`
	}
	if err := m.http.Post(ctx, "/user/get_users_online_status", req, &resp); err != nil {
		return nil, fmt.Errorf("get users online status: %w", err)
	}
	return resp.SuccessResult, nil
}

func (m *DefaultUserMgr) SubscribeUsersStatus(ctx context.Context, userID string, userIDs []string, genre int32) ([]*model.OnlineStatus, error) {
	req := struct {
		UserID  string   `json:"userID"`
		UserIDs []string `json:"userIDs"`
		Genre   int32    `json:"genre"`
	}{UserID: userID, UserIDs: userIDs, Genre: genre}
	var resp struct {
		StatusList []*model.OnlineStatus `json:"statusList"`
	}
	if err := m.http.Post(ctx, "/user/subscribe_users_status", req, &resp); err != nil {
		return nil, fmt.Errorf("subscribe users status: %w", err)
	}
	return resp.StatusList, nil
}

func (m *DefaultUserMgr) GetSubscribeUsersStatus(ctx context.Context, userID string) ([]*model.OnlineStatus, error) {
	req := struct {
		UserID string `json:"userID"`
	}{UserID: userID}
	var resp struct {
		StatusList []*model.OnlineStatus `json:"statusList"`
	}
	if err := m.http.Post(ctx, "/user/get_subscribe_users_status", req, &resp); err != nil {
		return nil, fmt.Errorf("get subscribe users status: %w", err)
	}
	return resp.StatusList, nil
}

func (m *DefaultUserMgr) GetUserClientConfig(ctx context.Context, userID string) (map[string]string, error) {
	req := struct {
		UserID string `json:"userID"`
	}{UserID: userID}
	var resp map[string]string
	if err := m.http.Post(ctx, "/user/get_user_client_config", req, &resp); err != nil {
		return nil, fmt.Errorf("get user client config: %w", err)
	}
	return resp, nil
}

func (m *DefaultUserMgr) SetUserClientConfig(ctx context.Context, userID string, configs map[string]string) error {
	req := struct {
		UserID  string            `json:"userID"`
		Configs map[string]string `json:"configs"`
	}{UserID: userID, Configs: configs}
	if err := m.http.Post(ctx, "/user/set_user_client_config", req, nil); err != nil {
		return fmt.Errorf("set user client config: %w", err)
	}
	return nil
}

func (m *DefaultUserMgr) DelUserClientConfig(ctx context.Context, userID string, keys []string) error {
	req := struct {
		UserID string   `json:"userID"`
		Keys   []string `json:"keys"`
	}{UserID: userID, Keys: keys}
	if err := m.http.Post(ctx, "/user/del_user_client_config", req, nil); err != nil {
		return fmt.Errorf("del user client config: %w", err)
	}
	return nil
}

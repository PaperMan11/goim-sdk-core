package manager

import (
	"context"

	"github.com/PaperMan11/goim-sdk-core/pkg/model"
)

// UserMgr 用户资料 / 在线状态 / 客户端配置
//
// 实现要点:
//   - 14 个接口全部走 HTTPClient
//   - GetUsersOnlineStatus 需配合 WS 推送（WSSubUserOnlineStatus / WSSetBackgroundStatus）
type UserMgr interface {
	Register(ctx context.Context, users []*model.UserInfo) error
	UpdateUserInfo(ctx context.Context, userInfo *model.UserInfo) error
	UpdateUserInfoEx(ctx context.Context, userInfo *model.UserInfoWithEx) error
	SetGlobalMsgRecvOpt(ctx context.Context, userID string, opt int32) error
	GetUsersInfo(ctx context.Context, userIDs []string) ([]*model.UserInfo, error)
	GetAllUsersUID(ctx context.Context, pagination *model.RequestPagination) (total int32, userIDs []string, err error)
	AccountCheck(ctx context.Context, checkUserIDs []string) ([]*model.AccountStatus, error)
	GetUsers(ctx context.Context, pagination *model.RequestPagination, userID, nickName string) (total int32, users []*model.UserInfo, err error)
	GetUsersOnlineStatus(ctx context.Context, userID string, userIDs []string) ([]*model.OnlineStatus, error)
	SubscribeUsersStatus(ctx context.Context, userID string, userIDs []string, genre int32) ([]*model.OnlineStatus, error)
	GetSubscribeUsersStatus(ctx context.Context, userID string) ([]*model.OnlineStatus, error)
	GetUserClientConfig(ctx context.Context, userID string) (map[string]string, error)
	SetUserClientConfig(ctx context.Context, userID string, configs map[string]string) error
	DelUserClientConfig(ctx context.Context, userID string, keys []string) error
}

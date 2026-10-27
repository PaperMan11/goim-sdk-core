package manager

import (
	"context"

	"github.com/PaperMan11/goim-sdk-core/pkg/model"
)

// RelMgr 好友 / 黑名单 / 增量同步
//
// 实现要点:
//   - 15 个 HTTP 接口
//   - GetIncrementalFriends / GetIncrementalBlacks 需本地 versionID 配合
type RelMgr interface {
	AddFriend(ctx context.Context, fromUserID, toUserID, reqMsg, ex string) error
	AddFriendResponse(ctx context.Context, fromUserID, toUserID, handleMsg string, handleResult int32) error
	DeleteFriend(ctx context.Context, ownerUserID, friendUserID string) error
	GetFriendList(ctx context.Context, userID string, pagination *model.RequestPagination) (total int32, friends []*model.FriendInfo, err error)
	GetFriendApplyList(ctx context.Context, userID string, pagination *model.RequestPagination, handleResults []int32) (total int32, requests []*model.FriendRequest, err error)
	GetSelfFriendApplyList(ctx context.Context, userID string, pagination *model.RequestPagination, handleResults []int32) (total int32, requests []*model.FriendRequest, err error)
	SetFriendRemark(ctx context.Context, ownerUserID, friendUserID, remark string) error
	AddBlack(ctx context.Context, ownerUserID, blackUserID, ex string) error
	RemoveBlack(ctx context.Context, ownerUserID, blackUserID string) error
	GetBlackList(ctx context.Context, userID string, pagination *model.RequestPagination) (total int32, blacks []*model.BlackInfo, err error)
	IsFriend(ctx context.Context, userID1, userID2 string) (*model.FriendRelation, error)
	GetFriendIDs(ctx context.Context, userID string) ([]string, error)
	GetIncrementalFriends(ctx context.Context, userID, versionID string, version uint64) (*model.IncrementalFriends, error)
	GetIncrementalBlacks(ctx context.Context, userID, versionID string, version uint64) (*model.IncrementalBlacks, error)
	// GetFullFriendUserIDs 好友 ID 全量校验（full=true 回退时调用；equal=true 表示本地 IDHash 一致无需重拉）
	GetFullFriendUserIDs(ctx context.Context, userID string, idHash uint64) (*model.FullFriendIDs, error)
	ImportFriend(ctx context.Context, ownerUserID string, friendUserIDs []string) error
}

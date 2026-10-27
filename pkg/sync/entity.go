package sync

import (
	"context"
	"fmt"

	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/storage"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/eventbus"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/hash"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/timex"
)

// 全量拉取分页大小
const fullPageSize = 500

// RelSource 好友/黑名单同步最小依赖（manager.RelMgr 子集）
type RelSource interface {
	GetIncrementalFriends(ctx context.Context, userID, versionID string, version uint64) (*model.IncrementalFriends, error)
	GetIncrementalBlacks(ctx context.Context, userID, versionID string, version uint64) (*model.IncrementalBlacks, error)
	GetFullFriendUserIDs(ctx context.Context, userID string, idHash uint64) (*model.FullFriendIDs, error)
	GetFriendList(ctx context.Context, userID string, pagination *model.RequestPagination) (total int32, friends []*model.FriendInfo, err error)
	GetBlackList(ctx context.Context, userID string, pagination *model.RequestPagination) (total int32, blacks []*model.BlackInfo, err error)
}

// GroupSource 群/群成员同步最小依赖（manager.GroupMgr 子集）
type GroupSource interface {
	GetIncrementalJoinGroups(ctx context.Context, userID, versionID string, version uint64) (*model.IncrementalGroups, error)
	GetIncrementalGroupMembers(ctx context.Context, groupID, versionID string, version uint64) (*model.IncrementalMembers, error)
	GetFullGroupMemberUserIDs(ctx context.Context, groupID string, idHash uint64) (*model.FullMemberIDs, error)
	GetFullJoinGroupIDs(ctx context.Context, userID string, idHash uint64) (*model.FullGroupIDs, error)
	GetJoinedGroupList(ctx context.Context, userID string, pagination *model.RequestPagination) (total int32, groups []*model.GroupInfo, err error)
	GetGroupMemberList(ctx context.Context, pagination *model.RequestPagination, groupID string, filter int32, keyword string) (total uint32, members []*model.GroupMemberFullInfo, err error)
}

// ConvSource 会话同步最小依赖（manager.ConvMgr 子集）
type ConvSource interface {
	GetIncrementalConversations(ctx context.Context, userID string, versionID string, version uint64) (*model.IncrementalConversations, error)
	GetFullConversationIDs(ctx context.Context, userID string, idHash uint64) (*model.FullConversationIDs, error)
	GetAllConversations(ctx context.Context, ownerUserID string) ([]*model.Conversation, error)
}

// EntitySyncEngine 五类实体同步引擎
//
// 同步顺序：好友 → 黑名单 → 群（含成员）→ 会话。
// 五类实体的增量应用与全量回退流程结构一致，公共骨架收敛为 runIncSync /
// runFullSync 模板，实体差异（接口、事件、载荷、附加步骤）由 spec 闭包声明。
type EntitySyncEngine struct {
	rel    RelSource
	group  GroupSource
	conv   ConvSource
	stor   storage.Storage
	owner  string
	bus    *eventbus.EventBus
	logger logger.Logger
}

// NewEntitySyncEngine 构造实体同步引擎
func NewEntitySyncEngine(rel RelSource, group GroupSource, conv ConvSource, stor storage.Storage, owner string, bus *eventbus.EventBus, l logger.Logger) *EntitySyncEngine {
	return &EntitySyncEngine{rel: rel, group: group, conv: conv, stor: stor, owner: owner, bus: bus, logger: l}
}

// SyncAll 顺序同步全部实体，返回第一个错误（已完成的实体游标已落盘，重试时增量）
func (e *EntitySyncEngine) SyncAll(ctx context.Context) error {
	if err := e.SyncFriends(ctx); err != nil {
		return fmt.Errorf("sync friends: %w", err)
	}
	if err := e.SyncBlacks(ctx); err != nil {
		return fmt.Errorf("sync blacks: %w", err)
	}
	if err := e.SyncGroups(ctx); err != nil {
		return fmt.Errorf("sync groups: %w", err)
	}
	if err := e.SyncConversations(ctx); err != nil {
		return fmt.Errorf("sync conversations: %w", err)
	}
	return nil
}

func (e *EntitySyncEngine) publish(et events.EventType, payload interface{}) {
	if e.bus == nil {
		return
	}
	e.bus.Publish(et, events.Event{
		Type:      et,
		Payload:   payload,
		Timestamp: timex.UnixMilli(),
	})
}

// === 同步流程模板 ===
//
// 包级泛型函数（Go 方法不能自带类型参数，engine 作为参数传入）。

// incMeta 增量响应公共元信息（五类 Incremental* 共有 cursor + full 字段）
type incMeta struct {
	versionID string
	version   uint64
	full      bool
	payload   interface{} // 同步完成事件载荷
}

// fullMeta 全量校验响应元信息（Full*IDs 响应共有 cursor + equal 字段）
type fullMeta struct {
	versionID string
	version   uint64
	equal     bool
}

// incChanges 增量变更集（值切片已转指针切片，可直接落库）
type incChanges[ITEM any] struct {
	insert []*ITEM
	update []*ITEM
	del    []string
}

// incSyncSpec 增量同步差异点
type incSyncSpec[ITEM any] struct {
	key   string           // storage 游标 key
	event events.EventType // 同步完成事件

	// fetchInc 拉取增量并解析变更（full 标记与事件载荷由 incMeta 上报）
	fetchInc func(ctx context.Context, vID string, ver uint64) (incMeta, incChanges[ITEM], error)
	upsert   func(owner string, items []*ITEM) error
	del      func(owner string, ids []string) error
	// afterSync 变更落库、游标写回后的附加步骤（群：逐群拉成员增量）；
	// 返回错误则中断同步、不发事件
	afterSync func(ctx context.Context, ch incChanges[ITEM]) error
}

// fullSyncSpec 全量回退差异点
type fullSyncSpec[ITEM any] struct {
	key   string
	event events.EventType

	// listLocalIDs 本地已缓存实体 ID（用于 IDHash 校验与清表）
	listLocalIDs func() ([]string, error)
	// verify 服务端 IDHash 校验；nil = 无校验接口（黑名单：沿用增量响应游标直接重拉）
	verify func(ctx context.Context, localHash uint64) (fullMeta, error)
	clear  func(localIDs []string) error
	// fetchAll 全量拉取（分页/整取差异封装在闭包内），仅收集不落库：
	// 拉取失败时本地旧数据与游标均不受影响，下次同步重新走全量流程
	fetchAll func(ctx context.Context) ([]*ITEM, error)
	upsert   func(owner string, items []*ITEM) error
	// afterFull 换表完成后的附加步骤（群：逐群拉成员），返回错误则不发实体事件
	afterFull func(ctx context.Context) error
}

// runIncSync 增量同步模板
//
// GetVersion → fetchInc → [full → runFullSync] → apply(insert/update/delete) →
// afterSync → SetVersion → publish（commit-last：附加步骤失败则游标不推进，下次重拉重试）
func runIncSync[ITEM any](e *EntitySyncEngine, ctx context.Context, s incSyncSpec[ITEM], f fullSyncSpec[ITEM]) error {
	vID, ver, err := e.stor.GetVersion(e.owner, s.key)
	if err != nil {
		e.logger.Errorf("runIncSync get version: %w", err)
		return err
	}
	meta, ch, err := s.fetchInc(ctx, vID, ver)
	if err != nil {
		return err
	}
	if meta.full {
		return runFullSync(e, ctx, f, meta)
	}
	if err := s.upsert(e.owner, ch.insert); err != nil {
		e.logger.Errorf("runIncSync upsert insert: %w", err)
		return err
	}
	if err := s.upsert(e.owner, ch.update); err != nil {
		e.logger.Errorf("runIncSync upsert update: %w", err)
		return err
	}
	if err := s.del(e.owner, ch.del); err != nil {
		e.logger.Errorf("runIncSync del: %w", err)
		return err
	}
	if s.afterSync != nil {
		if err := s.afterSync(ctx, ch); err != nil {
			e.logger.Errorf("runIncSync afterSync: %w", err)
			return err
		}
	}
	// 游标最后落盘（commit-last）：附加步骤（群成员同步）失败时游标不推进，
	// 下次同步重拉同批增量重试，落库均幂等
	if err := e.stor.SetVersion(e.owner, s.key, meta.versionID, meta.version); err != nil {
		e.logger.Errorf("runIncSync set version: %w", err)
		return err
	}
	e.publish(s.event, meta.payload)
	return nil
}

// runFullSync 全量回退模板
//
// listLocalIDs → [verify：equal → 仅对齐游标 + publish] → fetchAll(仅收集) →
// clear → upsert → afterFull → SetVersion → publish
//
// fetch-then-swap：全量数据收集完成才清表换新（清表→写入之间仅本地操作无网络）。
// 拉取中途失败时本地旧数据完好、游标未动，避免"清表后拉取失败 → 本地列表空窗"
// 的感知丢失；重试时 IDHash 基于完整旧数据重校验，自愈路径不变。
//
// inc 为触发回退的增量响应元信息：verify 为 nil 时沿用其游标（黑名单），
// 事件载荷始终取 inc.payload。
func runFullSync[ITEM any](e *EntitySyncEngine, ctx context.Context, s fullSyncSpec[ITEM], inc incMeta) error {
	localIDs, err := s.listLocalIDs()
	if err != nil {
		e.logger.Errorf("runFullSync listLocalIDs: %w", err)
		return err
	}
	meta := fullMeta{versionID: inc.versionID, version: inc.version}
	if s.verify != nil {
		meta, err = s.verify(ctx, hash.HashStringSet(localIDs))
		if err != nil {
			e.logger.Errorf("runFullSync verify: %w", err)
			return err
		}
	}
	if meta.equal {
		// 本地 ID 集合与服务端一致：内容可信（历史增量已应用），仅对齐游标
		if err := e.stor.SetVersion(e.owner, s.key, meta.versionID, meta.version); err != nil {
			e.logger.Errorf("runFullSync set version: %w", err)
			return err
		}
		e.publish(s.event, inc.payload)
		return nil
	}
	all, err := s.fetchAll(ctx)
	if err != nil {
		e.logger.Errorf("runFullSync fetchAll: %w", err)
		return err
	}
	if err := s.clear(localIDs); err != nil {
		e.logger.Errorf("runFullSync clear: %w", err)
		return err
	}
	if err := s.upsert(e.owner, all); err != nil {
		e.logger.Errorf("runFullSync upsert: %w", err)
		return err
	}
	if s.afterFull != nil {
		if err := s.afterFull(ctx); err != nil {
			e.logger.Errorf("runFullSync afterFull: %w", err)
			return err
		}
	}
	// 游标最后落盘（commit-last）：附加步骤失败时游标不推进，下次同步重新走全量流程
	if err := e.stor.SetVersion(e.owner, s.key, meta.versionID, meta.version); err != nil {
		e.logger.Errorf("runFullSync set version: %w", err)
		return err
	}
	e.publish(s.event, inc.payload)
	return nil
}

// toPtrSlice 值切片 → 指针切片（元素为副本指针）
func toPtrSlice[T any](items []T) []*T {
	out := make([]*T, 0, len(items))
	for _, it := range items {
		out = append(out, ptrOf(it))
	}
	return out
}

// ptrOf 值转指针（参数为副本，循环内取地址安全）
func ptrOf[T any](v T) *T { return &v }

// === 好友 ===

// SyncFriends 好友增量同步（full=true 时 IDHash 校验 + 全量回退）
func (e *EntitySyncEngine) SyncFriends(ctx context.Context) error {
	return runIncSync(e, ctx,
		incSyncSpec[model.FriendInfo]{
			key:   storage.EntityFriends,
			event: events.EventFriendListSynced,
			fetchInc: func(ctx context.Context, vID string, ver uint64) (incMeta, incChanges[model.FriendInfo], error) {
				inc, err := e.rel.GetIncrementalFriends(ctx, e.owner, vID, ver)
				if err != nil {
					return incMeta{}, incChanges[model.FriendInfo]{}, err
				}
				return incMeta{versionID: inc.VersionID, version: inc.Version, full: inc.Full},
					incChanges[model.FriendInfo]{
						insert: toPtrSlice(inc.Insert),
						update: toPtrSlice(inc.Update),
						del:    inc.Delete,
					}, nil
			},
			upsert: e.stor.UpsertFriends,
			del:    e.stor.DeleteFriends,
		},
		fullSyncSpec[model.FriendInfo]{
			key:   storage.EntityFriends,
			event: events.EventFriendListSynced,
			listLocalIDs: func() ([]string, error) {
				return e.stor.ListFriendIDs(e.owner)
			},
			verify: func(ctx context.Context, localHash uint64) (fullMeta, error) {
				full, err := e.rel.GetFullFriendUserIDs(ctx, e.owner, localHash)
				if err != nil {
					return fullMeta{}, err
				}
				return fullMeta{versionID: full.VersionID, version: full.Version, equal: full.Equal}, nil
			},
			clear: func(localIDs []string) error {
				return e.stor.DeleteFriends(e.owner, localIDs)
			},
			fetchAll: func(ctx context.Context) ([]*model.FriendInfo, error) {
				var all []*model.FriendInfo
				err := e.pageFetch(ctx, func(p *model.RequestPagination) (int32, int, error) {
					total, friends, err := e.rel.GetFriendList(ctx, e.owner, p)
					if err != nil {
						return 0, 0, err
					}
					all = append(all, friends...)
					return total, len(friends), nil
				})
				return all, err
			},
			upsert: e.stor.UpsertFriends,
		})
}

// === 黑名单 ===

// SyncBlacks 黑名单增量同步（无 IDHash 校验接口，full=true 时直接分页全量重拉）
func (e *EntitySyncEngine) SyncBlacks(ctx context.Context) error {
	return runIncSync(e, ctx,
		incSyncSpec[model.BlackInfo]{
			key:   storage.EntityBlacks,
			event: events.EventBlackListSynced,
			fetchInc: func(ctx context.Context, vID string, ver uint64) (incMeta, incChanges[model.BlackInfo], error) {
				inc, err := e.rel.GetIncrementalBlacks(ctx, e.owner, vID, ver)
				if err != nil {
					return incMeta{}, incChanges[model.BlackInfo]{}, err
				}
				return incMeta{versionID: inc.VersionID, version: inc.Version, full: inc.Full},
					incChanges[model.BlackInfo]{
						insert: toPtrSlice(inc.Insert),
						update: toPtrSlice(inc.Update),
						del:    inc.Delete,
					}, nil
			},
			upsert: e.stor.UpsertBlacks,
			del:    e.stor.DeleteBlacks,
		},
		fullSyncSpec[model.BlackInfo]{
			key:   storage.EntityBlacks,
			event: events.EventBlackListSynced,
			listLocalIDs: func() ([]string, error) {
				return e.stor.ListBlackIDs(e.owner)
			},
			// 无 IDHash 校验接口：verify 为 nil，游标沿用增量响应，直接清表重拉
			clear: func(localIDs []string) error {
				return e.stor.DeleteBlacks(e.owner, localIDs)
			},
			fetchAll: func(ctx context.Context) ([]*model.BlackInfo, error) {
				var all []*model.BlackInfo
				err := e.pageFetch(ctx, func(p *model.RequestPagination) (int32, int, error) {
					total, blacks, err := e.rel.GetBlackList(ctx, e.owner, p)
					if err != nil {
						return 0, 0, err
					}
					all = append(all, blacks...)
					return total, len(blacks), nil
				})
				return all, err
			},
			upsert: e.stor.UpsertBlacks,
		})
}

// === 群（含群成员） ===

// SyncGroups 加入群增量同步；insert/update 的群逐个做成员增量
func (e *EntitySyncEngine) SyncGroups(ctx context.Context) error {
	// 全量回退：先收集所有群再一次性入库，随后逐群拉成员
	var allGroups []*model.GroupInfo
	if err := runIncSync(e, ctx,
		incSyncSpec[model.GroupInfo]{
			key:   storage.EntityJoinGroups,
			event: events.EventGroupListSynced,
			fetchInc: func(ctx context.Context, vID string, ver uint64) (incMeta, incChanges[model.GroupInfo], error) {
				inc, err := e.group.GetIncrementalJoinGroups(ctx, e.owner, vID, ver)
				if err != nil {
					return incMeta{}, incChanges[model.GroupInfo]{}, err
				}
				return incMeta{versionID: inc.VersionID, version: inc.Version, full: inc.Full},
					incChanges[model.GroupInfo]{
						insert: toPtrSlice(inc.Insert),
						update: toPtrSlice(inc.Update),
						del:    inc.Delete,
					}, nil
			},
			upsert: e.stor.UpsertGroups,
			del:    e.stor.DeleteGroups,
			afterSync: func(ctx context.Context, ch incChanges[model.GroupInfo]) error {
				// 变更群逐个拉成员增量（insert 全是新群 → 全量成员；update → 带游标增量）
				for _, g := range ch.insert {
					if err := e.SyncGroupMembers(ctx, g.GroupID); err != nil {
						return fmt.Errorf("group %s members: %w", g.GroupID, err)
					}
				}
				for _, g := range ch.update {
					if err := e.SyncGroupMembers(ctx, g.GroupID); err != nil {
						return fmt.Errorf("group %s members: %w", g.GroupID, err)
					}
				}
				return nil
			},
		},
		fullSyncSpec[model.GroupInfo]{
			key:   storage.EntityJoinGroups,
			event: events.EventGroupListSynced,
			listLocalIDs: func() ([]string, error) {
				return e.stor.ListGroupIDs(e.owner)
			},
			verify: func(ctx context.Context, localHash uint64) (fullMeta, error) {
				full, err := e.group.GetFullJoinGroupIDs(ctx, e.owner, localHash)
				if err != nil {
					return fullMeta{}, err
				}
				return fullMeta{versionID: full.VersionID, version: full.Version, equal: full.Equal}, nil
			},
			clear: func(localIDs []string) error {
				return e.stor.DeleteGroups(e.owner, localIDs) // 级联删成员
			},
			fetchAll: func(ctx context.Context) ([]*model.GroupInfo, error) {
				err := e.pageFetch(ctx, func(p *model.RequestPagination) (int32, int, error) {
					total, groups, err := e.group.GetJoinedGroupList(ctx, e.owner, p)
					if err != nil {
						return 0, 0, err
					}
					allGroups = append(allGroups, groups...)
					return total, len(groups), nil
				})
				return allGroups, err
			},
			upsert: e.stor.UpsertGroups,
			afterFull: func(ctx context.Context) error {
				// 全量群都要成员
				for _, g := range allGroups {
					if err := e.SyncGroupMembers(ctx, g.GroupID); err != nil {
						return fmt.Errorf("group %s members: %w", g.GroupID, err)
					}
				}
				return nil
			},
		}); err != nil {
		return err
	}
	return e.reconcileGroupMembers(ctx)
}

// reconcileGroupMembers 成员游标补偿
//
// 不变量：本地群已入库 ⟹ 必有 groupMembers:<gid> 游标（SyncGroupMembers 内部游标落盘是最后一步）。
// 若上次同步"群信息已落库、成员同步失败"，群游标已推进，该群不会再次出现在增量响应中，
// 全量回退 equal 分支也会跳过成员循环 —— 这里按游标缺失 diff 出漏同步的群补一次同步。
// 健康状态下本地 diff 为空，零额外网络请求。
func (e *EntitySyncEngine) reconcileGroupMembers(ctx context.Context) error {
	versions, err := e.stor.ListVersions(e.owner)
	if err != nil {
		return err
	}
	groupIDs, err := e.stor.ListGroupIDs(e.owner)
	if err != nil {
		return err
	}
	for _, gid := range groupIDs {
		if _, ok := versions[fmt.Sprintf(storage.EntityGroupMembersFmt, gid)]; ok {
			continue
		}
		if err := e.SyncGroupMembers(ctx, gid); err != nil {
			return fmt.Errorf("group %s members: %w", gid, err)
		}
	}
	return nil
}

// SyncGroupMembers 单群成员增量同步（游标 key = groupMembers:<groupID>）
func (e *EntitySyncEngine) SyncGroupMembers(ctx context.Context, groupID string) error {
	entityKey := fmt.Sprintf(storage.EntityGroupMembersFmt, groupID)
	return runIncSync(e, ctx,
		incSyncSpec[model.GroupMemberFullInfo]{
			key:   entityKey,
			event: events.EventGroupMemberSynced,
			fetchInc: func(ctx context.Context, vID string, ver uint64) (incMeta, incChanges[model.GroupMemberFullInfo], error) {
				inc, err := e.group.GetIncrementalGroupMembers(ctx, groupID, vID, ver)
				if err != nil {
					return incMeta{}, incChanges[model.GroupMemberFullInfo]{}, err
				}
				return incMeta{versionID: inc.VersionID, version: inc.Version, full: inc.Full, payload: inc},
					incChanges[model.GroupMemberFullInfo]{
						insert: toPtrSlice(inc.Insert),
						update: toPtrSlice(inc.Update),
						del:    inc.Delete,
					}, nil
			},
			upsert: func(owner string, items []*model.GroupMemberFullInfo) error {
				return e.stor.UpsertGroupMembers(owner, groupID, items)
			},
			del: func(owner string, ids []string) error {
				return e.stor.DeleteGroupMembers(owner, groupID, ids)
			},
		},
		fullSyncSpec[model.GroupMemberFullInfo]{
			key:   entityKey,
			event: events.EventGroupMemberSynced,
			listLocalIDs: func() ([]string, error) {
				return e.stor.ListGroupMemberIDs(e.owner, groupID)
			},
			verify: func(ctx context.Context, localHash uint64) (fullMeta, error) {
				full, err := e.group.GetFullGroupMemberUserIDs(ctx, groupID, localHash)
				if err != nil {
					return fullMeta{}, err
				}
				return fullMeta{versionID: full.VersionID, version: full.Version, equal: full.Equal}, nil
			},
			clear: func(localIDs []string) error {
				return e.stor.DeleteGroupMembers(e.owner, groupID, localIDs)
			},
			fetchAll: func(ctx context.Context) ([]*model.GroupMemberFullInfo, error) {
				var all []*model.GroupMemberFullInfo
				err := e.pageFetchUint(ctx, func(p *model.RequestPagination) (uint32, int, error) {
					total, members, err := e.group.GetGroupMemberList(ctx, p, groupID, 0, "")
					if err != nil {
						return 0, 0, err
					}
					all = append(all, members...)
					return total, len(members), nil
				})
				return all, err
			},
			upsert: func(owner string, items []*model.GroupMemberFullInfo) error {
				return e.stor.UpsertGroupMembers(owner, groupID, items)
			},
		})
}

// === 会话 ===

// SyncConversations 会话增量同步
func (e *EntitySyncEngine) SyncConversations(ctx context.Context) error {
	return runIncSync(e, ctx,
		incSyncSpec[model.Conversation]{
			key:   storage.EntityConversations,
			event: events.EventConversationSynced,
			fetchInc: func(ctx context.Context, vID string, ver uint64) (incMeta, incChanges[model.Conversation], error) {
				inc, err := e.conv.GetIncrementalConversations(ctx, e.owner, vID, ver)
				if err != nil {
					return incMeta{}, incChanges[model.Conversation]{}, err
				}
				// insert 与 update 合并为一次 upsert（会话 DTO 本身就是指针切片）
				ch := incChanges[model.Conversation]{del: inc.Delete}
				ch.insert = append(append([]*model.Conversation{}, inc.Insert...), inc.Update...)
				return incMeta{versionID: inc.VersionID, version: inc.Version, full: inc.Full}, ch, nil
			},
			upsert: e.stor.UpsertConversations,
			del:    e.stor.DeleteConversations,
		},
		fullSyncSpec[model.Conversation]{
			key:   storage.EntityConversations,
			event: events.EventConversationSynced,
			listLocalIDs: func() ([]string, error) {
				return e.stor.ListConversationIDs(e.owner)
			},
			verify: func(ctx context.Context, localHash uint64) (fullMeta, error) {
				full, err := e.conv.GetFullConversationIDs(ctx, e.owner, localHash)
				if err != nil {
					return fullMeta{}, err
				}
				return fullMeta{versionID: full.VersionID, version: full.Version, equal: full.Equal}, nil
			},
			clear: func(localIDs []string) error {
				return e.stor.DeleteConversations(e.owner, localIDs)
			},
			fetchAll: func(ctx context.Context) ([]*model.Conversation, error) {
				return e.conv.GetAllConversations(ctx, e.owner)
			},
			upsert: e.stor.UpsertConversations,
		})
}

// === 分页全量拉取 helper ===

// pageFetch 通用分页循环：fetch 返回 (total, 本页条数, error)，直到拉完
func (e *EntitySyncEngine) pageFetch(ctx context.Context, fetch func(p *model.RequestPagination) (total int32, got int, err error)) error {
	pageNum := int32(1)
	for {
		p := &model.RequestPagination{PageNumber: pageNum, ShowNumber: fullPageSize}
		total, got, err := fetch(p)
		if err != nil {
			return err
		}
		if got < fullPageSize || int32(pageNum*fullPageSize) >= total {
			return nil
		}
		pageNum++
	}
}

// pageFetchUint total 为 uint32 的分页循环（GetGroupMemberList）
func (e *EntitySyncEngine) pageFetchUint(ctx context.Context, fetch func(p *model.RequestPagination) (total uint32, got int, err error)) error {
	pageNum := int32(1)
	for {
		p := &model.RequestPagination{PageNumber: pageNum, ShowNumber: fullPageSize}
		total, got, err := fetch(p)
		if err != nil {
			return err
		}
		if got < fullPageSize || uint32(pageNum*fullPageSize) >= total {
			return nil
		}
		pageNum++
	}
}

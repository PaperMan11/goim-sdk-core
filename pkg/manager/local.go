package manager

import (
	"errors"
	"fmt"
	"strings"

	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/storage"
)

// 本地数据能力（Local Data API）默认分页参数。
// 设计与边界见 docs/LOCAL_DATA_API.md。
const (
	// DefaultLocalPageSize 实体列表默认每页条数
	DefaultLocalPageSize int32 = 50
	// MaxLocalPageSize 实体列表每页上限
	MaxLocalPageSize int32 = 200
	// DefaultLocalMsgLimit 本地消息分页默认每页条数
	DefaultLocalMsgLimit int32 = 30
	// MaxLocalMsgLimit 本地消息分页每页上限
	MaxLocalMsgLimit int32 = 100
)

// ErrNotLoggedIn 本地数据查询未登录（owner 未确定）
var ErrNotLoggedIn = errors.New("local data: not logged in")

// LocalDataMgr 本地数据能力门面：只读本地缓存（不发网络请求），
// 数据由同步引擎与推送维护；owner 固定为当前登录用户。
type LocalDataMgr interface {
	// —— 会话 ——
	ListConversations(offset, limit int32) (*model.ConversationPage, error)
	GetConversation(conversationID string) (*model.Conversation, error)
	// GetConversations 批量直读（顺序与入参一致，不存在的跳过）
	GetConversations(conversationIDs []string) ([]*model.Conversation, error)
	// GetConversationSeqs 单会话本地/服务端 seq 边界快照
	GetConversationSeqs(conversationID string) (*model.ConversationSeqs, error)
	UnreadCount(conversationID string) (int64, error)
	UnreadTotal() (int64, error)

	// —— 消息 ——
	// ListMsgs 本地降序分页：beforeSeq<=0 取最新，否则取 <=beforeSeq；
	// 继续向更早翻页传 page.OldestSeq-1
	ListMsgs(conversationID string, beforeSeq int64, limit int32) (*model.MsgPage, error)
	// ListMsgsAfter 本地升序分页：afterSeq<=0 从头，否则取 >afterSeq
	ListMsgsAfter(conversationID string, afterSeq int64, limit int32) (*model.MsgPage, error)
	GetMsg(conversationID string, seq int64) (*model.MsgData, error)
	GetMsgByClientID(conversationID, clientMsgID string) (*model.MsgData, error)

	// —— 好友 / 黑名单 ——
	ListFriends(offset, limit int32) (*model.FriendPage, error)
	GetFriend(userID string) (*model.FriendInfo, error)
	ListBlacks(offset, limit int32) (*model.BlackPage, error)
	IsBlocked(userID string) (bool, error)

	// —— 群 / 群成员 ——
	ListGroups(offset, limit int32) (*model.GroupPage, error)
	GetGroup(groupID string) (*model.GroupInfo, error)
	ListGroupMembers(groupID string, offset, limit int32) (*model.GroupMemberPage, error)
	GetGroupMember(groupID, userID string) (*model.GroupMemberFullInfo, error)

	// —— 搜索 / 统计 ——
	// 搜索为大小写不敏感子串匹配，keyword 为空返回参数错误；
	// limit<=0 时实体搜索默认 50（上限 200）、消息搜索默认 30（上限 100）。
	SearchConversations(keyword string, limit int32) (*model.ConversationPage, error)
	SearchMsgs(conversationID, keyword string, limit int32) (*model.MsgPage, error)
	SearchFriends(keyword string, limit int32) (*model.FriendPage, error)
	SearchGroups(keyword string, limit int32) (*model.GroupPage, error)
	// Stats 当前 owner 本地缓存条数统计（不打网络请求）
	Stats() (*model.LocalDataStats, error)

	// —— 本地维护 ——
	// PurgeMsgs 删除本地 seq<=upToSeq 的消息并记录 floor（本机清理，不影响服务端）；
	// 返回删除条数。floor 防止自动同步把已清理的历史重新拉回，显式历史请求不受限。
	PurgeMsgs(conversationID string, upToSeq int64) (int64, error)
	// ClearLocalData 清空当前 owner 的全部本地缓存（不登出）；保留 SDK meta、同步游标与登录态，
	// 并记录各会话消息 floor，避免重连后回流。
	ClearLocalData() error
}

// DefaultLocalDataMgr LocalDataMgr 默认实现
type DefaultLocalDataMgr struct {
	stor    storage.Storage
	ownerFn func() string
}

// NewLocalDataMgr 构造本地数据门面；ownerFn 由 Client 注入（通常为 currentOwner）
func NewLocalDataMgr(stor storage.Storage, ownerFn func() string) *DefaultLocalDataMgr {
	return &DefaultLocalDataMgr{stor: stor, ownerFn: ownerFn}
}

func (m *DefaultLocalDataMgr) owner() (string, error) {
	if m.ownerFn == nil {
		return "", ErrNotLoggedIn
	}
	owner := m.ownerFn()
	if owner == "" {
		return "", ErrNotLoggedIn
	}
	return owner, nil
}

func normalizeLocalPage(offset, limit int32) (int32, int32) {
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = DefaultLocalPageSize
	} else if limit > MaxLocalPageSize {
		limit = MaxLocalPageSize
	}
	return offset, limit
}

func normalizeMsgLimit(limit int32) int32 {
	if limit <= 0 {
		return DefaultLocalMsgLimit
	}
	if limit > MaxLocalMsgLimit {
		return MaxLocalMsgLimit
	}
	return limit
}

// —— 会话 ——

func (m *DefaultLocalDataMgr) ListConversations(offset, limit int32) (*model.ConversationPage, error) {
	owner, err := m.owner()
	if err != nil {
		return nil, err
	}
	offset, limit = normalizeLocalPage(offset, limit)
	return m.stor.ListConversationsPage(owner, offset, limit)
}

func (m *DefaultLocalDataMgr) GetConversation(conversationID string) (*model.Conversation, error) {
	if conversationID == "" {
		return nil, fmt.Errorf("local data: get conversation: conversationID is empty")
	}
	owner, err := m.owner()
	if err != nil {
		return nil, err
	}
	return m.stor.GetConversation(owner, conversationID)
}

func (m *DefaultLocalDataMgr) GetConversations(conversationIDs []string) ([]*model.Conversation, error) {
	owner, err := m.owner()
	if err != nil {
		return nil, err
	}
	if len(conversationIDs) == 0 {
		return nil, nil
	}
	return m.stor.GetConversations(owner, conversationIDs)
}

func (m *DefaultLocalDataMgr) GetConversationSeqs(conversationID string) (*model.ConversationSeqs, error) {
	if conversationID == "" {
		return nil, fmt.Errorf("local data: get conversation seqs: conversationID is empty")
	}
	owner, err := m.owner()
	if err != nil {
		return nil, err
	}
	out := &model.ConversationSeqs{ConversationID: conversationID}
	msgs, err := m.stor.ListMsgsByConversation(owner, conversationID)
	if err != nil {
		return nil, fmt.Errorf("local data: get conversation seqs: list msgs: %w", err)
	}
	for _, mm := range msgs {
		if mm == nil {
			continue
		}
		if out.LocalMinSeq == 0 || mm.Seq < out.LocalMinSeq {
			out.LocalMinSeq = mm.Seq
		}
		if mm.Seq > out.LocalMaxSeq {
			out.LocalMaxSeq = mm.Seq
		}
		if mm.IsRead && mm.Seq > out.HasReadSeq {
			out.HasReadSeq = mm.Seq
		}
	}
	maxSeqs, minSeqs, err := m.stor.GetMaxSeqs(owner)
	if err != nil {
		return nil, fmt.Errorf("local data: get conversation seqs: get max seqs: %w", err)
	}
	out.ServerMaxSeq = maxSeqs[conversationID]
	out.ServerMinSeq = minSeqs[conversationID]
	return out, nil
}

// UnreadCount 本地推导的单会话未读数（离线可用，不发网络请求）。
//
// 推导规则：hasReadSeq 取本地缓存中已读（IsRead=true）消息的最大 seq；
// 未读数 = 缓存中 seq > hasReadSeq 且未撤回的条数。消息缓存不完整时为下界；
// 其他端已读而本端未收到 2200 回执时偏大。
func (m *DefaultLocalDataMgr) UnreadCount(conversationID string) (int64, error) {
	if conversationID == "" {
		return 0, fmt.Errorf("unread count: conversationID is empty")
	}
	owner, err := m.owner()
	if err != nil {
		return 0, err
	}
	conv, err := m.stor.GetConversation(owner, conversationID)
	if err != nil {
		return 0, fmt.Errorf("unread count: get conversation %s: %w", conversationID, err)
	}
	return m.unreadCountFor(owner, conversationID, conv)
}

// UnreadTotal 本地推导的全部业务会话未读总和（不含 n_ 通知会话）
func (m *DefaultLocalDataMgr) UnreadTotal() (int64, error) {
	owner, err := m.owner()
	if err != nil {
		return 0, err
	}
	convs, err := m.stor.ListConversations(owner)
	if err != nil {
		return 0, fmt.Errorf("unread total: list conversations: %w", err)
	}
	var total int64
	for _, cv := range convs {
		if cv == nil || IsNotificationConversation(cv.ConversationID) {
			continue
		}
		n, err := m.unreadCountFor(owner, cv.ConversationID, cv)
		if err != nil {
			// 单个会话失败不阻断总数（与 client 原实现一致，仅跳过）
			continue
		}
		total += n
	}
	return total, nil
}

// unreadCountFor 单会话未读推导；消息缓存为空时回落到会话缓存的 unreadCount 字段。
func (m *DefaultLocalDataMgr) unreadCountFor(owner, conversationID string, conv *model.Conversation) (int64, error) {
	msgs, err := m.stor.ListMsgsByConversation(owner, conversationID)
	if err != nil {
		return 0, fmt.Errorf("unread count: list msgs of %s: %w", conversationID, err)
	}
	if len(msgs) == 0 {
		if conv != nil && conv.UnreadCount > 0 {
			return int64(conv.UnreadCount), nil
		}
		return 0, nil
	}
	var hasReadSeq, unread int64
	for _, mm := range msgs {
		if mm == nil {
			continue
		}
		if mm.IsRead && mm.Seq > hasReadSeq {
			hasReadSeq = mm.Seq
		}
	}
	for _, mm := range msgs {
		// 未读条件：seq 超过已读上限、未撤回、且不是自己发送的消息
		if mm != nil && mm.Seq > hasReadSeq && mm.Status != model.MsgStatusRevoked && mm.SendID != owner {
			unread++
		}
	}
	return unread, nil
}

// —— 消息 ——

func (m *DefaultLocalDataMgr) ListMsgs(conversationID string, beforeSeq int64, limit int32) (*model.MsgPage, error) {
	if conversationID == "" {
		return nil, fmt.Errorf("local data: list msgs: conversationID is empty")
	}
	owner, err := m.owner()
	if err != nil {
		return nil, err
	}
	return m.stor.ListMsgsPage(owner, conversationID, beforeSeq, normalizeMsgLimit(limit), true)
}

func (m *DefaultLocalDataMgr) ListMsgsAfter(conversationID string, afterSeq int64, limit int32) (*model.MsgPage, error) {
	if conversationID == "" {
		return nil, fmt.Errorf("local data: list msgs after: conversationID is empty")
	}
	owner, err := m.owner()
	if err != nil {
		return nil, err
	}
	return m.stor.ListMsgsPage(owner, conversationID, afterSeq, normalizeMsgLimit(limit), false)
}

func (m *DefaultLocalDataMgr) GetMsg(conversationID string, seq int64) (*model.MsgData, error) {
	if conversationID == "" || seq <= 0 {
		return nil, fmt.Errorf("local data: get msg: invalid conversationID or seq")
	}
	owner, err := m.owner()
	if err != nil {
		return nil, err
	}
	return m.stor.GetMsg(owner, conversationID, seq)
}

func (m *DefaultLocalDataMgr) GetMsgByClientID(conversationID, clientMsgID string) (*model.MsgData, error) {
	if conversationID == "" || clientMsgID == "" {
		return nil, fmt.Errorf("local data: get msg by clientMsgID: invalid params")
	}
	owner, err := m.owner()
	if err != nil {
		return nil, err
	}
	return m.stor.GetMsgByClientID(owner, conversationID, clientMsgID)
}

// —— 好友 / 黑名单 ——

func (m *DefaultLocalDataMgr) ListFriends(offset, limit int32) (*model.FriendPage, error) {
	owner, err := m.owner()
	if err != nil {
		return nil, err
	}
	offset, limit = normalizeLocalPage(offset, limit)
	return m.stor.ListFriendsPage(owner, offset, limit)
}

func (m *DefaultLocalDataMgr) GetFriend(userID string) (*model.FriendInfo, error) {
	if userID == "" {
		return nil, fmt.Errorf("local data: get friend: userID is empty")
	}
	owner, err := m.owner()
	if err != nil {
		return nil, err
	}
	return m.stor.GetFriend(owner, userID)
}

func (m *DefaultLocalDataMgr) ListBlacks(offset, limit int32) (*model.BlackPage, error) {
	owner, err := m.owner()
	if err != nil {
		return nil, err
	}
	offset, limit = normalizeLocalPage(offset, limit)
	return m.stor.ListBlacksPage(owner, offset, limit)
}

func (m *DefaultLocalDataMgr) IsBlocked(userID string) (bool, error) {
	if userID == "" {
		return false, fmt.Errorf("local data: is blocked: userID is empty")
	}
	owner, err := m.owner()
	if err != nil {
		return false, err
	}
	b, err := m.stor.GetBlack(owner, userID)
	if err != nil {
		return false, err
	}
	return b != nil, nil
}

// —— 群 / 群成员 ——

func (m *DefaultLocalDataMgr) ListGroups(offset, limit int32) (*model.GroupPage, error) {
	owner, err := m.owner()
	if err != nil {
		return nil, err
	}
	offset, limit = normalizeLocalPage(offset, limit)
	return m.stor.ListGroupsPage(owner, offset, limit)
}

func (m *DefaultLocalDataMgr) GetGroup(groupID string) (*model.GroupInfo, error) {
	if groupID == "" {
		return nil, fmt.Errorf("local data: get group: groupID is empty")
	}
	owner, err := m.owner()
	if err != nil {
		return nil, err
	}
	return m.stor.GetGroup(owner, groupID)
}

func (m *DefaultLocalDataMgr) ListGroupMembers(groupID string, offset, limit int32) (*model.GroupMemberPage, error) {
	if groupID == "" {
		return nil, fmt.Errorf("local data: list group members: groupID is empty")
	}
	owner, err := m.owner()
	if err != nil {
		return nil, err
	}
	offset, limit = normalizeLocalPage(offset, limit)
	return m.stor.ListGroupMembersPage(owner, groupID, offset, limit)
}

func (m *DefaultLocalDataMgr) GetGroupMember(groupID, userID string) (*model.GroupMemberFullInfo, error) {
	if groupID == "" || userID == "" {
		return nil, fmt.Errorf("local data: get group member: invalid params")
	}
	owner, err := m.owner()
	if err != nil {
		return nil, err
	}
	return m.stor.GetGroupMember(owner, groupID, userID)
}

// —— 搜索 / 统计 ——

func (m *DefaultLocalDataMgr) SearchConversations(keyword string, limit int32) (*model.ConversationPage, error) {
	if strings.TrimSpace(keyword) == "" {
		return nil, fmt.Errorf("local data: search conversations: keyword is empty")
	}
	owner, err := m.owner()
	if err != nil {
		return nil, err
	}
	_, limit = normalizeLocalPage(0, limit)
	return m.stor.SearchConversations(owner, keyword, limit)
}

func (m *DefaultLocalDataMgr) SearchMsgs(conversationID, keyword string, limit int32) (*model.MsgPage, error) {
	if conversationID == "" {
		return nil, fmt.Errorf("local data: search msgs: conversationID is empty")
	}
	if strings.TrimSpace(keyword) == "" {
		return nil, fmt.Errorf("local data: search msgs: keyword is empty")
	}
	owner, err := m.owner()
	if err != nil {
		return nil, err
	}
	return m.stor.SearchMsgs(owner, conversationID, keyword, normalizeMsgLimit(limit))
}

func (m *DefaultLocalDataMgr) SearchFriends(keyword string, limit int32) (*model.FriendPage, error) {
	if strings.TrimSpace(keyword) == "" {
		return nil, fmt.Errorf("local data: search friends: keyword is empty")
	}
	owner, err := m.owner()
	if err != nil {
		return nil, err
	}
	_, limit = normalizeLocalPage(0, limit)
	return m.stor.SearchFriends(owner, keyword, limit)
}

func (m *DefaultLocalDataMgr) SearchGroups(keyword string, limit int32) (*model.GroupPage, error) {
	if strings.TrimSpace(keyword) == "" {
		return nil, fmt.Errorf("local data: search groups: keyword is empty")
	}
	owner, err := m.owner()
	if err != nil {
		return nil, err
	}
	_, limit = normalizeLocalPage(0, limit)
	return m.stor.SearchGroups(owner, keyword, limit)
}

// Stats 汇总当前 owner 各实体缓存条数（消息/群成员为全量口径）
func (m *DefaultLocalDataMgr) Stats() (*model.LocalDataStats, error) {
	owner, err := m.owner()
	if err != nil {
		return nil, err
	}
	var st model.LocalDataStats
	if st.Conversations, err = m.stor.CountConversations(owner); err != nil {
		return nil, fmt.Errorf("local data: stats: count conversations: %w", err)
	}
	if st.Messages, err = m.stor.CountMsgs(owner, ""); err != nil {
		return nil, fmt.Errorf("local data: stats: count messages: %w", err)
	}
	if st.Friends, err = m.stor.CountFriends(owner); err != nil {
		return nil, fmt.Errorf("local data: stats: count friends: %w", err)
	}
	if st.Blacks, err = m.stor.CountBlacks(owner); err != nil {
		return nil, fmt.Errorf("local data: stats: count blacks: %w", err)
	}
	if st.Groups, err = m.stor.CountGroups(owner); err != nil {
		return nil, fmt.Errorf("local data: stats: count groups: %w", err)
	}
	if st.GroupMembers, err = m.stor.CountGroupMembers(owner, ""); err != nil {
		return nil, fmt.Errorf("local data: stats: count group members: %w", err)
	}
	return &st, nil
}

// —— 本地维护 ——

func (m *DefaultLocalDataMgr) PurgeMsgs(conversationID string, upToSeq int64) (int64, error) {
	if conversationID == "" || upToSeq <= 0 {
		return 0, fmt.Errorf("local data: purge msgs: invalid conversationID or upToSeq")
	}
	owner, err := m.owner()
	if err != nil {
		return 0, err
	}
	// floor 先落盘再删消息：删除中途失败也不会在下次同步把已放弃的历史拉回
	floor := upToSeq
	if old, err := m.stor.GetMsgFloor(owner, conversationID); err != nil {
		return 0, fmt.Errorf("local data: purge msgs: get floor: %w", err)
	} else if old > floor {
		floor = old
	}
	if err := m.stor.SetMsgFloor(owner, conversationID, floor); err != nil {
		return 0, fmt.Errorf("local data: purge msgs: set floor: %w", err)
	}
	n, err := m.stor.PurgeMsgs(owner, conversationID, upToSeq)
	if err != nil {
		return 0, fmt.Errorf("local data: purge msgs: %w", err)
	}
	return n, nil
}

// ClearLocalData 清空当前 owner 本地缓存：先按会话写 floor（取同步游标与本地最新 seq 的
// 较大者），再删除消息与实体表；seqs/versions 游标与 SDK meta 保留（不登出、不回流）。
func (m *DefaultLocalDataMgr) ClearLocalData() error {
	owner, err := m.owner()
	if err != nil {
		return err
	}
	maxSeqs, _, err := m.stor.GetMaxSeqs(owner)
	if err != nil {
		return fmt.Errorf("local data: clear: get max seqs: %w", err)
	}
	convIDs, err := m.stor.ListConversationIDs(owner)
	if err != nil {
		return fmt.Errorf("local data: clear: list conversation ids: %w", err)
	}

	floors := make(map[string]int64, len(maxSeqs)+len(convIDs))
	for convID, seq := range maxSeqs {
		if seq > 0 {
			floors[convID] = seq
		}
	}
	for _, convID := range convIDs {
		if _, ok := floors[convID]; !ok {
			floors[convID] = 0
		}
	}
	// 显式历史请求/推送可能已写入超过同步游标的消息，floor 取本地最新 seq 兜底
	for convID, floor := range floors {
		latest, err := m.stor.GetLatestMsg(owner, convID)
		if err != nil {
			return fmt.Errorf("local data: clear: get latest msg %s: %w", convID, err)
		}
		if latest != nil && latest.Seq > floor {
			floors[convID] = latest.Seq
		}
	}

	var errs []error
	for convID, floor := range floors {
		if floor <= 0 {
			continue
		}
		if err := m.stor.SetMsgFloor(owner, convID, floor); err != nil {
			errs = append(errs, fmt.Errorf("local data: clear: set floor %s: %w", convID, err))
			continue
		}
		if _, err := m.stor.PurgeMsgs(owner, convID, floor); err != nil {
			errs = append(errs, fmt.Errorf("local data: clear: purge msgs %s: %w", convID, err))
		}
	}

	if err := m.stor.DeleteConversations(owner, convIDs); err != nil {
		errs = append(errs, fmt.Errorf("local data: clear: delete conversations: %w", err))
	}
	if ids, err := m.stor.ListFriendIDs(owner); err != nil {
		errs = append(errs, fmt.Errorf("local data: clear: list friend ids: %w", err))
	} else if err := m.stor.DeleteFriends(owner, ids); err != nil {
		errs = append(errs, fmt.Errorf("local data: clear: delete friends: %w", err))
	}
	if ids, err := m.stor.ListBlackIDs(owner); err != nil {
		errs = append(errs, fmt.Errorf("local data: clear: list black ids: %w", err))
	} else if err := m.stor.DeleteBlacks(owner, ids); err != nil {
		errs = append(errs, fmt.Errorf("local data: clear: delete blacks: %w", err))
	}
	groupIDs, err := m.stor.ListGroupIDs(owner)
	if err != nil {
		errs = append(errs, fmt.Errorf("local data: clear: list group ids: %w", err))
	} else {
		if err := m.stor.DeleteGroups(owner, groupIDs); err != nil {
			errs = append(errs, fmt.Errorf("local data: clear: delete groups: %w", err))
		}
		for _, groupID := range groupIDs {
			ids, err := m.stor.ListGroupMemberIDs(owner, groupID)
			if err != nil {
				errs = append(errs, fmt.Errorf("local data: clear: list group members %s: %w", groupID, err))
				continue
			}
			if err := m.stor.DeleteGroupMembers(owner, groupID, ids); err != nil {
				errs = append(errs, fmt.Errorf("local data: clear: delete group members %s: %w", groupID, err))
			}
		}
	}

	return errors.Join(errs...)
}

// Package storage 本地持久化接口 + 实现。
//
// 两种实现：
//   - MemoryStorage：内存版本（单元测试 + 早期开发用）
//   - SQLiteStorage：gorm + glebarez/sqlite 纯 Go 驱动，无 CGO，全平台可用（Phase 4.5）
//
// wasm 场景（Phase 5）接口不变，底层换 IndexedDB。
//
// 设计原则：
//   - 元数据：最大/最小 seq（SeqSyncEngine 游标）、版本游标（EntitySyncEngine）
//   - 业务缓存：消息 / 会话 / 好友 / 黑名单 / 群 / 群成员，全部按 OwnerUserID 隔离（多账号共存）
//   - 消息缓存可选；PullMsgBySeq 总是从服务端拉，Storage 不做消息缓存的硬依赖
package storage

import (
	"context"
	"sort"
	"strconv"
	"sync"

	"github.com/PaperMan11/goim-sdk-core/pkg/model"
)

// 实体游标 key（GetVersion / SetVersion 的 entity 参数）
const (
	EntityFriends         = "friends"
	EntityBlacks          = "blacks"
	EntityJoinGroups      = "joinGroups"
	EntityConversations   = "conversations"
	EntityGroupMembersFmt = "groupMembers:%s" // 群维度游标：fmt.Sprintf(EntityGroupMembersFmt, groupID)
)

// Storage 本地持久化接口
//
// 所有方法都以 ownerUserID 隔离数据（多账号共存，Logout 只清当前 owner）。
type Storage interface {
	// === 元数据 ===

	// GetMaxSeqs 获取某用户所有会话的最大/最小 seq（MsgMgr.NewestSeq 结果持久化）
	GetMaxSeqs(ownerUserID string) (maxSeqs map[string]int64, minSeqs map[string]int64, err error)
	// SetMaxSeqs 写入最大/最小 seq
	SetMaxSeqs(ownerUserID string, maxSeqs, minSeqs map[string]int64) error

	// GetVersion 获取增量同步游标（entity 见 Entity* 常量；不存在时返回 ("", 0, nil)）
	GetVersion(ownerUserID, entity string) (versionID string, version uint64, err error)
	// SetVersion 写入同步游标
	SetVersion(ownerUserID, entity, versionID string, version uint64) error
	// ListVersions 某用户全部游标（entity → 游标），用于同步引擎的补偿检查
	ListVersions(ownerUserID string) (map[string]VersionInfo, error)

	// === 消息缓存（SeqSyncEngine gap 拉取入库 + WS 推送实时入库） ===

	// UpsertMsgs 按 (owner, seq) upsert；seq <= 0 的消息跳过
	UpsertMsgs(ownerUserID string, msgs []*model.MsgData) error
	// ListMsgsByConversation 按会话取消息缓存（seq 升序）
	ListMsgsByConversation(ownerUserID, conversationID string) ([]*model.MsgData, error)
	// GetLatestMsg 会话缓存中 seq 最大的消息（无缓存返回 nil；会话摘要兜底用）
	GetLatestMsg(ownerUserID, conversationID string) (*model.MsgData, error)
	// DeleteMsgs 按 seq 删除消息缓存（2102 删除通知实时应用）
	DeleteMsgs(ownerUserID, conversationID string, seqs []int64) error
	// MarkMsgsRead 按 seq 标记已读（2200 已读回执实时应用）
	MarkMsgsRead(ownerUserID, conversationID string, seqs []int64) error
	// MarkMsgsReadUpTo 标记 seq <= hasReadSeq 的消息已读（2200 整会话已读）
	MarkMsgsReadUpTo(ownerUserID, conversationID string, hasReadSeq int64) error
	// MarkMsgsRevoked 按 seq 标记撤回（2101 撤回通知实时应用）
	MarkMsgsRevoked(ownerUserID, conversationID string, seqs []int64) error

	// === 会话 ===

	// UpsertConversations 按 conversationID upsert
	UpsertConversations(ownerUserID string, convs []*model.Conversation) error
	// DeleteConversations 硬删
	DeleteConversations(ownerUserID string, conversationIDs []string) error
	// ListConversations 会话列表（LatestMsgSendTime 倒序）
	ListConversations(ownerUserID string) ([]*model.Conversation, error)
	// GetConversation 单会话读取（不存在返回 nil, nil；避免全量列表再筛选）
	GetConversation(ownerUserID, conversationID string) (*model.Conversation, error)
	// ListConversationIDs 仅 ID 列表（IDHash 校验用）
	ListConversationIDs(ownerUserID string) ([]string, error)

	// === 好友 ===

	UpsertFriends(ownerUserID string, friends []*model.FriendInfo) error
	DeleteFriends(ownerUserID string, friendUserIDs []string) error
	ListFriends(ownerUserID string) ([]*model.FriendInfo, error)
	ListFriendIDs(ownerUserID string) ([]string, error)

	// === 黑名单 ===

	UpsertBlacks(ownerUserID string, blacks []*model.BlackInfo) error
	DeleteBlacks(ownerUserID string, blackUserIDs []string) error
	ListBlacks(ownerUserID string) ([]*model.BlackInfo, error)
	ListBlackIDs(ownerUserID string) ([]string, error)

	// === 加入的群 ===

	UpsertGroups(ownerUserID string, groups []*model.GroupInfo) error
	DeleteGroups(ownerUserID string, groupIDs []string) error
	ListGroups(ownerUserID string) ([]*model.GroupInfo, error)
	ListGroupIDs(ownerUserID string) ([]string, error)

	// === 群成员（群维度） ===

	UpsertGroupMembers(ownerUserID, groupID string, members []*model.GroupMemberFullInfo) error
	DeleteGroupMembers(ownerUserID, groupID string, userIDs []string) error
	ListGroupMembers(ownerUserID, groupID string) ([]*model.GroupMemberFullInfo, error)
	ListGroupMemberIDs(ownerUserID, groupID string) ([]string, error)

	// === 本地分页 / 单条查询（Local Data API，见 docs/LOCAL_DATA_API.md）===

	// ListConversationsPage 会话分页（置顶优先 + LatestMsgSendTime 倒序）
	ListConversationsPage(ownerUserID string, offset, limit int32) (*model.ConversationPage, error)
	// GetConversations 批量直读（返回命中的会话，顺序与入参一致；不存在的跳过）
	GetConversations(ownerUserID string, conversationIDs []string) ([]*model.Conversation, error)
	// ListMsgsPage 消息分页：desc=true 按 seq 倒序（seq<=0 取最新；否则取 <=seq），
	// desc=false 按 seq 升序（seq<=0 从头；否则取 >seq）
	ListMsgsPage(ownerUserID, conversationID string, seq int64, limit int32, desc bool) (*model.MsgPage, error)
	// GetMsg 按 seq 直读单条消息（不存在返回 nil, nil）
	GetMsg(ownerUserID, conversationID string, seq int64) (*model.MsgData, error)
	// GetMsgByClientID 按客户端消息 ID 直读单条消息（不存在返回 nil, nil）
	GetMsgByClientID(ownerUserID, conversationID, clientMsgID string) (*model.MsgData, error)
	// ListFriendsPage 好友分页（置顶优先 + userID 升序）
	ListFriendsPage(ownerUserID string, offset, limit int32) (*model.FriendPage, error)
	// GetFriend 单好友直读（不存在返回 nil, nil）
	GetFriend(ownerUserID, friendUserID string) (*model.FriendInfo, error)
	// ListBlacksPage 黑名单分页（blackUserID 升序）
	ListBlacksPage(ownerUserID string, offset, limit int32) (*model.BlackPage, error)
	// GetBlack 单条黑名单直读（不存在返回 nil, nil）
	GetBlack(ownerUserID, blackUserID string) (*model.BlackInfo, error)
	// ListGroupsPage 群分页（群名升序）
	ListGroupsPage(ownerUserID string, offset, limit int32) (*model.GroupPage, error)
	// GetGroup 单群直读（不存在返回 nil, nil）
	GetGroup(ownerUserID, groupID string) (*model.GroupInfo, error)
	// ListGroupMembersPage 群成员分页（角色高优先 + JoinTime 升序）
	ListGroupMembersPage(ownerUserID, groupID string, offset, limit int32) (*model.GroupMemberPage, error)
	// GetGroupMember 单群成员直读（不存在返回 nil, nil）
	GetGroupMember(ownerUserID, groupID, userID string) (*model.GroupMemberFullInfo, error)

	// === 生命周期 ===

	// Clear 清理指定 owner 的全部数据（Logout 时调用）
	Clear(ctx context.Context, ownerUserID string) error

	// === SDK 级元数据（与 owner 无关，如设备 ID）===

	// GetMeta 读取键值，不存在返回 ("", nil)
	GetMeta(key string) (string, error)
	// SetMeta 写入键值
	SetMeta(key, value string) error
}

// VersionInfo 实体游标信息
type VersionInfo struct {
	VersionID string
	Version   uint64
}

// MemoryStorage 内存实现（默认，单元测试和早期开发用）
type MemoryStorage struct {
	mu sync.RWMutex

	// owner → (conversationID → max/min)
	seqs map[string]struct {
		max map[string]int64
		min map[string]int64
	}

	// owner → entity → (versionID, version)
	versions map[string]map[string]struct {
		id      string
		version uint64
	}

	msgs         map[string]map[string]*model.MsgData                        // owner → (conversationID|seq → msg)，key: convID + "#" + seq
	convs        map[string]map[string]*model.Conversation                   // owner → conversationID → conv
	friends      map[string]map[string]*model.FriendInfo                     // owner → friendUserID → info
	blacks       map[string]map[string]*model.BlackInfo                      // owner → blackUserID → info
	groups       map[string]map[string]*model.GroupInfo                      // owner → groupID → info
	groupMembers map[string]map[string]map[string]*model.GroupMemberFullInfo // owner → groupID → userID → member
	meta         map[string]string                                           // SDK 级元数据（设备 ID 等）
}

// NewMemoryStorage 构造内存存储
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		seqs: make(map[string]struct{ max, min map[string]int64 }),
		versions: make(map[string]map[string]struct {
			id      string
			version uint64
		}),
		msgs:         make(map[string]map[string]*model.MsgData),
		convs:        make(map[string]map[string]*model.Conversation),
		friends:      make(map[string]map[string]*model.FriendInfo),
		blacks:       make(map[string]map[string]*model.BlackInfo),
		groups:       make(map[string]map[string]*model.GroupInfo),
		groupMembers: make(map[string]map[string]map[string]*model.GroupMemberFullInfo),
		meta:         make(map[string]string),
	}
}

// === 元数据 ===

func (s *MemoryStorage) GetMaxSeqs(owner string) (map[string]int64, map[string]int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seqs, ok := s.seqs[owner]
	if !ok {
		return nil, nil, nil
	}
	return seqs.max, seqs.min, nil
}

func (s *MemoryStorage) SetMaxSeqs(owner string, maxSeqs, minSeqs map[string]int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seqs[owner] = struct {
		max map[string]int64
		min map[string]int64
	}{max: maxSeqs, min: minSeqs}
	return nil
}

func (s *MemoryStorage) GetVersion(owner, entity string) (string, uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.versions[owner][entity]
	if !ok {
		return "", 0, nil
	}
	return v.id, v.version, nil
}

func (s *MemoryStorage) SetVersion(owner, entity, versionID string, version uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.versions[owner] == nil {
		s.versions[owner] = make(map[string]struct {
			id      string
			version uint64
		})
	}
	s.versions[owner][entity] = struct {
		id      string
		version uint64
	}{id: versionID, version: version}
	return nil
}

func (s *MemoryStorage) ListVersions(owner string) (map[string]VersionInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]VersionInfo, len(s.versions[owner]))
	for entity, v := range s.versions[owner] {
		out[entity] = VersionInfo{VersionID: v.id, Version: v.version}
	}
	return out, nil
}

// === 消息 ===

func msgKey(m *model.MsgData) string {
	return m.ConversationID + "#" + strconv.FormatInt(m.Seq, 10)
}

func (s *MemoryStorage) UpsertMsgs(owner string, msgs []*model.MsgData) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.msgs[owner] == nil {
		s.msgs[owner] = make(map[string]*model.MsgData)
	}
	for _, m := range msgs {
		if m == nil || m.Seq <= 0 {
			continue
		}
		s.msgs[owner][msgKey(m)] = m
	}
	return nil
}

func (s *MemoryStorage) ListMsgsByConversation(owner, conversationID string) ([]*model.MsgData, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*model.MsgData
	for key, m := range s.msgs[owner] {
		if len(key) > len(conversationID)+1 && key[:len(conversationID)+1] == conversationID+"#" {
			out = append(out, m)
		}
	}
	// seq 升序
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j].Seq < out[i].Seq {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out, nil
}

// updateMsgs 按会话 + seq 集合原地修改消息（通知实时应用）
func (s *MemoryStorage) updateMsgs(owner, conversationID string, seqs []int64, mutate func(*model.MsgData)) {
	set := make(map[int64]struct{}, len(seqs))
	for _, seq := range seqs {
		set[seq] = struct{}{}
	}
	for _, m := range s.msgs[owner] {
		if m.ConversationID == conversationID {
			if _, ok := set[m.Seq]; ok {
				mutate(m)
			}
		}
	}
}

func (s *MemoryStorage) GetLatestMsg(owner, conversationID string) (*model.MsgData, error) {
	if conversationID == "" {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var latest *model.MsgData
	for _, m := range s.msgs[owner] {
		if m == nil || m.ConversationID != conversationID {
			continue
		}
		if latest == nil || m.Seq > latest.Seq {
			latest = m
		}
	}
	return latest, nil
}

func (s *MemoryStorage) DeleteMsgs(owner, conversationID string, seqs []int64) error {
	if conversationID == "" || len(seqs) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	set := make(map[int64]struct{}, len(seqs))
	for _, seq := range seqs {
		set[seq] = struct{}{}
	}
	for key, m := range s.msgs[owner] {
		if m.ConversationID == conversationID {
			if _, ok := set[m.Seq]; ok {
				delete(s.msgs[owner], key)
			}
		}
	}
	return nil
}

func (s *MemoryStorage) MarkMsgsRead(owner, conversationID string, seqs []int64) error {
	if conversationID == "" || len(seqs) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updateMsgs(owner, conversationID, seqs, func(m *model.MsgData) { m.IsRead = true })
	return nil
}

func (s *MemoryStorage) MarkMsgsReadUpTo(owner, conversationID string, hasReadSeq int64) error {
	if conversationID == "" || hasReadSeq <= 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.msgs[owner] {
		if m.ConversationID == conversationID && m.Seq <= hasReadSeq {
			m.IsRead = true
		}
	}
	return nil
}

func (s *MemoryStorage) MarkMsgsRevoked(owner, conversationID string, seqs []int64) error {
	if conversationID == "" || len(seqs) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updateMsgs(owner, conversationID, seqs, func(m *model.MsgData) { m.Status = model.MsgStatusRevoked })
	return nil
}

// === 会话 ===

// mergeConversationLocalFields 服务端会话 DTO 不含 SDK 本地扩展字段（摘要/草稿/未读），
// 同步 upsert 时保留已有本地值，避免增量同步把消息推送维护的摘要清空。
func mergeConversationLocalFields(existing, incoming *model.Conversation) {
	if existing == nil || incoming == nil {
		return
	}
	if incoming.MsgInfo == nil && incoming.LatestMsgSendTime == 0 {
		incoming.MsgInfo = existing.MsgInfo
		incoming.LatestMsgSendTime = existing.LatestMsgSendTime
	}
	if incoming.DraftText == "" {
		incoming.DraftText = existing.DraftText
	}
	if incoming.UnreadCount == 0 {
		incoming.UnreadCount = existing.UnreadCount
	}
}

func (s *MemoryStorage) UpsertConversations(owner string, convs []*model.Conversation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.convs[owner] == nil {
		s.convs[owner] = make(map[string]*model.Conversation)
	}
	for _, c := range convs {
		if c == nil || c.ConversationID == "" {
			continue
		}
		mergeConversationLocalFields(s.convs[owner][c.ConversationID], c)
		s.convs[owner][c.ConversationID] = c
	}
	return nil
}

func (s *MemoryStorage) DeleteConversations(owner string, conversationIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range conversationIDs {
		delete(s.convs[owner], id)
	}
	return nil
}

func (s *MemoryStorage) ListConversations(owner string) ([]*model.Conversation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*model.Conversation
	for _, c := range s.convs[owner] {
		out = append(out, c)
	}
	// LatestMsgSendTime 倒序
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j].LatestMsgSendTime > out[i].LatestMsgSendTime {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out, nil
}

func (s *MemoryStorage) GetConversation(owner, conversationID string) (*model.Conversation, error) {
	if conversationID == "" {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.convs[owner][conversationID], nil
}

func (s *MemoryStorage) ListConversationIDs(owner string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []string
	for id := range s.convs[owner] {
		out = append(out, id)
	}
	return out, nil
}

// === 好友 ===

func (s *MemoryStorage) UpsertFriends(owner string, friends []*model.FriendInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.friends[owner] == nil {
		s.friends[owner] = make(map[string]*model.FriendInfo)
	}
	for _, f := range friends {
		if f == nil || f.FriendUserID() == "" {
			continue
		}
		s.friends[owner][f.FriendUserID()] = f
	}
	return nil
}

func (s *MemoryStorage) DeleteFriends(owner string, friendUserIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range friendUserIDs {
		delete(s.friends[owner], id)
	}
	return nil
}

func (s *MemoryStorage) ListFriends(owner string) ([]*model.FriendInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*model.FriendInfo
	for _, f := range s.friends[owner] {
		out = append(out, f)
	}
	return out, nil
}

func (s *MemoryStorage) ListFriendIDs(owner string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []string
	for id := range s.friends[owner] {
		out = append(out, id)
	}
	return out, nil
}

// === 黑名单 ===

func (s *MemoryStorage) UpsertBlacks(owner string, blacks []*model.BlackInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.blacks[owner] == nil {
		s.blacks[owner] = make(map[string]*model.BlackInfo)
	}
	for _, b := range blacks {
		if b == nil || b.BlackUserID() == "" {
			continue
		}
		s.blacks[owner][b.BlackUserID()] = b
	}
	return nil
}

func (s *MemoryStorage) DeleteBlacks(owner string, blackUserIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range blackUserIDs {
		delete(s.blacks[owner], id)
	}
	return nil
}

func (s *MemoryStorage) ListBlacks(owner string) ([]*model.BlackInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*model.BlackInfo
	for _, b := range s.blacks[owner] {
		out = append(out, b)
	}
	return out, nil
}

func (s *MemoryStorage) ListBlackIDs(owner string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []string
	for id := range s.blacks[owner] {
		out = append(out, id)
	}
	return out, nil
}

// === 群 ===

func (s *MemoryStorage) UpsertGroups(owner string, groups []*model.GroupInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.groups[owner] == nil {
		s.groups[owner] = make(map[string]*model.GroupInfo)
	}
	for _, g := range groups {
		if g == nil || g.GroupID == "" {
			continue
		}
		s.groups[owner][g.GroupID] = g
	}
	return nil
}

func (s *MemoryStorage) DeleteGroups(owner string, groupIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range groupIDs {
		delete(s.groups[owner], id)
		// 群删除时成员缓存一并清理
		delete(s.groupMembers[owner], id)
	}
	return nil
}

func (s *MemoryStorage) ListGroups(owner string) ([]*model.GroupInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*model.GroupInfo
	for _, g := range s.groups[owner] {
		out = append(out, g)
	}
	return out, nil
}

func (s *MemoryStorage) ListGroupIDs(owner string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []string
	for id := range s.groups[owner] {
		out = append(out, id)
	}
	return out, nil
}

// === 群成员 ===

func (s *MemoryStorage) UpsertGroupMembers(owner, groupID string, members []*model.GroupMemberFullInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.groupMembers[owner] == nil {
		s.groupMembers[owner] = make(map[string]map[string]*model.GroupMemberFullInfo)
	}
	if s.groupMembers[owner][groupID] == nil {
		s.groupMembers[owner][groupID] = make(map[string]*model.GroupMemberFullInfo)
	}
	for _, m := range members {
		if m == nil || m.UserID == "" {
			continue
		}
		s.groupMembers[owner][groupID][m.UserID] = m
	}
	return nil
}

func (s *MemoryStorage) DeleteGroupMembers(owner, groupID string, userIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range userIDs {
		delete(s.groupMembers[owner][groupID], id)
	}
	return nil
}

func (s *MemoryStorage) ListGroupMembers(owner, groupID string) ([]*model.GroupMemberFullInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*model.GroupMemberFullInfo
	for _, m := range s.groupMembers[owner][groupID] {
		out = append(out, m)
	}
	return out, nil
}

func (s *MemoryStorage) ListGroupMemberIDs(owner, groupID string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []string
	for id := range s.groupMembers[owner][groupID] {
		out = append(out, id)
	}
	return out, nil
}

// === 本地分页 / 单条查询（Local Data API）===

// pageSlice 通用分页切片：返回 [start,end) 与是否还有下一页
func pageSlice(total int, offset, limit int32) (start, end int, hasMore bool) {
	if offset < 0 {
		offset = 0
	}
	start = int(offset)
	if start >= total {
		return total, total, false
	}
	end = total
	if limit > 0 && start+int(limit) < total {
		end = start + int(limit)
	}
	return start, end, end < total
}

func (s *MemoryStorage) ListConversationsPage(owner string, offset, limit int32) (*model.ConversationPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	all := make([]*model.Conversation, 0, len(s.convs[owner]))
	for _, c := range s.convs[owner] {
		if c != nil {
			all = append(all, c)
		}
	}
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].IsPinned != all[j].IsPinned {
			return all[i].IsPinned
		}
		return all[i].LatestMsgSendTime > all[j].LatestMsgSendTime
	})
	start, end, hasMore := pageSlice(len(all), offset, limit)
	return &model.ConversationPage{Items: all[start:end], Total: int64(len(all)), HasMore: hasMore}, nil
}

func (s *MemoryStorage) GetConversations(owner string, conversationIDs []string) ([]*model.Conversation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*model.Conversation, 0, len(conversationIDs))
	for _, id := range conversationIDs {
		if c := s.convs[owner][id]; c != nil {
			out = append(out, c)
		}
	}
	return out, nil
}

func (s *MemoryStorage) ListMsgsPage(owner, conversationID string, seq int64, limit int32, desc bool) (*model.MsgPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	all := make([]*model.MsgData, 0, len(s.msgs[owner]))
	for _, m := range s.msgs[owner] {
		if m == nil || m.ConversationID != conversationID {
			continue
		}
		if desc {
			if seq > 0 && m.Seq > seq {
				continue
			}
		} else if seq > 0 && m.Seq <= seq {
			continue
		}
		all = append(all, m)
	}
	if desc {
		sort.SliceStable(all, func(i, j int) bool { return all[i].Seq > all[j].Seq })
	} else {
		sort.SliceStable(all, func(i, j int) bool { return all[i].Seq < all[j].Seq })
	}
	start, end, hasMore := pageSlice(len(all), 0, limit)
	items := all[start:end]
	page := &model.MsgPage{Items: items, HasMore: hasMore}
	for _, m := range items {
		if page.OldestSeq == 0 || m.Seq < page.OldestSeq {
			page.OldestSeq = m.Seq
		}
		if m.Seq > page.NewestSeq {
			page.NewestSeq = m.Seq
		}
	}
	return page, nil
}

func (s *MemoryStorage) GetMsg(owner, conversationID string, seq int64) (*model.MsgData, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.msgs[owner][conversationID+"#"+strconv.FormatInt(seq, 10)], nil
}

func (s *MemoryStorage) GetMsgByClientID(owner, conversationID, clientMsgID string) (*model.MsgData, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, m := range s.msgs[owner] {
		if m != nil && m.ConversationID == conversationID && m.ClientMsgID == clientMsgID {
			return m, nil
		}
	}
	return nil, nil
}

func (s *MemoryStorage) ListFriendsPage(owner string, offset, limit int32) (*model.FriendPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	all := make([]*model.FriendInfo, 0, len(s.friends[owner]))
	for _, f := range s.friends[owner] {
		if f != nil {
			all = append(all, f)
		}
	}
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].IsPinned != all[j].IsPinned {
			return all[i].IsPinned
		}
		return all[i].FriendUserID() < all[j].FriendUserID()
	})
	start, end, hasMore := pageSlice(len(all), offset, limit)
	return &model.FriendPage{Items: all[start:end], Total: int64(len(all)), HasMore: hasMore}, nil
}

func (s *MemoryStorage) GetFriend(owner, friendUserID string) (*model.FriendInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.friends[owner][friendUserID], nil
}

func (s *MemoryStorage) ListBlacksPage(owner string, offset, limit int32) (*model.BlackPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	all := make([]*model.BlackInfo, 0, len(s.blacks[owner]))
	for _, b := range s.blacks[owner] {
		if b != nil {
			all = append(all, b)
		}
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].BlackUserID() < all[j].BlackUserID() })
	start, end, hasMore := pageSlice(len(all), offset, limit)
	return &model.BlackPage{Items: all[start:end], Total: int64(len(all)), HasMore: hasMore}, nil
}

func (s *MemoryStorage) GetBlack(owner, blackUserID string) (*model.BlackInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.blacks[owner][blackUserID], nil
}

func (s *MemoryStorage) ListGroupsPage(owner string, offset, limit int32) (*model.GroupPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	all := make([]*model.GroupInfo, 0, len(s.groups[owner]))
	for _, g := range s.groups[owner] {
		if g != nil {
			all = append(all, g)
		}
	}
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].GroupName != all[j].GroupName {
			return all[i].GroupName < all[j].GroupName
		}
		return all[i].GroupID < all[j].GroupID
	})
	start, end, hasMore := pageSlice(len(all), offset, limit)
	return &model.GroupPage{Items: all[start:end], Total: int64(len(all)), HasMore: hasMore}, nil
}

func (s *MemoryStorage) GetGroup(owner, groupID string) (*model.GroupInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.groups[owner][groupID], nil
}

func (s *MemoryStorage) ListGroupMembersPage(owner, groupID string, offset, limit int32) (*model.GroupMemberPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	all := make([]*model.GroupMemberFullInfo, 0, len(s.groupMembers[owner][groupID]))
	for _, m := range s.groupMembers[owner][groupID] {
		if m != nil {
			all = append(all, m)
		}
	}
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].RoleLevel != all[j].RoleLevel {
			return all[i].RoleLevel > all[j].RoleLevel
		}
		if all[i].JoinTime != all[j].JoinTime {
			return all[i].JoinTime < all[j].JoinTime
		}
		return all[i].UserID < all[j].UserID
	})
	start, end, hasMore := pageSlice(len(all), offset, limit)
	return &model.GroupMemberPage{Items: all[start:end], Total: int64(len(all)), HasMore: hasMore}, nil
}

func (s *MemoryStorage) GetGroupMember(owner, groupID, userID string) (*model.GroupMemberFullInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.groupMembers[owner][groupID][userID], nil
}

// === 生命周期 ===

func (s *MemoryStorage) Clear(_ context.Context, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.seqs, owner)
	delete(s.versions, owner)
	delete(s.msgs, owner)
	delete(s.convs, owner)
	delete(s.friends, owner)
	delete(s.blacks, owner)
	delete(s.groups, owner)
	delete(s.groupMembers, owner)
	return nil
}

// === SDK 级元数据 ===

func (s *MemoryStorage) GetMeta(key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.meta[key], nil
}

func (s *MemoryStorage) SetMeta(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.meta[key] = value
	return nil
}

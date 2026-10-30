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

	// === 消息缓存（SeqSyncEngine gap 拉取入库） ===

	// UpsertMsgs 按 (owner, seq) upsert；seq <= 0 的消息跳过
	UpsertMsgs(ownerUserID string, msgs []*model.MsgData) error
	// ListMsgsByConversation 按会话取消息缓存（seq 升序）
	ListMsgsByConversation(ownerUserID, conversationID string) ([]*model.MsgData, error)

	// === 会话 ===

	// UpsertConversations 按 conversationID upsert
	UpsertConversations(ownerUserID string, convs []*model.Conversation) error
	// DeleteConversations 硬删
	DeleteConversations(ownerUserID string, conversationIDs []string) error
	// ListConversations 会话列表（LatestMsgSendTime 倒序）
	ListConversations(ownerUserID string) ([]*model.Conversation, error)
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

// === 会话 ===

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
		if f == nil || f.FriendUserID == "" {
			continue
		}
		s.friends[owner][f.FriendUserID] = f
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
		if b == nil || b.BlackUserID == "" {
			continue
		}
		s.blacks[owner][b.BlackUserID] = b
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

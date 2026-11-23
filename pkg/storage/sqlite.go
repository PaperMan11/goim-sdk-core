//go:build !(js && wasm)

package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	gormlogger "gorm.io/gorm/logger"

	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
)

// SQLiteStorage SQLite 实现（gorm + glebarez/sqlite 纯 Go 驱动，无 CGO）
//
// path 传 ":memory:" 走内存库（单测用）；否则为文件路径（自动创建父目录）。
type SQLiteStorage struct {
	db     *gorm.DB
	logger logger.Logger
}

// NewSQLiteStorage 打开（或创建）SQLite 库并自动迁移 8 张表
func NewSQLiteStorage(path string, l logger.Logger) (*SQLiteStorage, error) {
	if path == "" {
		path = ":memory:"
	}
	if path != ":memory:" {
		if dir := filepath.Dir(path); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, fmt.Errorf("storage: create dir: %w", err)
			}
		}
	}

	cfg := &gorm.Config{}
	if l == nil {
		cfg.Logger = gormlogger.Discard
	} else {
		cfg.Logger = gormlogger.Discard // SDK 日志走自身 logger，gorm SQL 日志关闭
	}
	db, err := gorm.Open(sqlite.Open(path), cfg)
	if err != nil {
		return nil, fmt.Errorf("storage: open sqlite %s: %w", path, err)
	}
	// 连接池：单连接避免 SQLite 写锁竞争
	if sqlDB, err := db.DB(); err == nil {
		sqlDB.SetMaxOpenConns(1)
	}
	if err := db.AutoMigrate(allModels...); err != nil {
		return nil, fmt.Errorf("storage: migrate: %w", err)
	}
	return &SQLiteStorage{db: db, logger: l}, nil
}

// Close 关闭底层连接
func (s *SQLiteStorage) Close() error {
	if sqlDB, err := s.db.DB(); err == nil {
		return sqlDB.Close()
	}
	return nil
}

// === 元数据 ===

func (s *SQLiteStorage) GetMaxSeqs(owner string) (map[string]int64, map[string]int64, error) {
	var row LocalSeq
	err := s.db.Where("owner_user_id = ?", owner).First(&row).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("storage: get max seqs: %w", err)
	}
	maxSeqs, minSeqs := map[string]int64{}, map[string]int64{}
	if row.MaxSeqsJSON != "" {
		if err := jsonUnmarshal(row.MaxSeqsJSON, &maxSeqs); err != nil {
			return nil, nil, fmt.Errorf("storage: max seqs json: %w", err)
		}
	}
	if row.MinSeqsJSON != "" {
		if err := jsonUnmarshal(row.MinSeqsJSON, &minSeqs); err != nil {
			return nil, nil, fmt.Errorf("storage: min seqs json: %w", err)
		}
	}
	return maxSeqs, minSeqs, nil
}

func (s *SQLiteStorage) SetMaxSeqs(owner string, maxSeqs, minSeqs map[string]int64) error {
	row := LocalSeq{OwnerUserID: owner}
	if len(maxSeqs) > 0 {
		b, err := jsonMarshal(maxSeqs)
		if err != nil {
			return fmt.Errorf("storage: marshal max seqs: %w", err)
		}
		row.MaxSeqsJSON = string(b)
	}
	if len(minSeqs) > 0 {
		b, err := jsonMarshal(minSeqs)
		if err != nil {
			return fmt.Errorf("storage: marshal min seqs: %w", err)
		}
		row.MinSeqsJSON = string(b)
	}
	if err := s.db.Save(&row).Error; err != nil {
		return fmt.Errorf("storage: set max seqs: %w", err)
	}
	return nil
}

func (s *SQLiteStorage) GetVersion(owner, entity string) (string, uint64, error) {
	var row Version
	err := s.db.Where("owner_user_id = ? AND entity = ?", owner, entity).First(&row).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return "", 0, nil
		}
		return "", 0, fmt.Errorf("storage: get version: %w", err)
	}
	return row.VersionID, row.Version, nil
}

func (s *SQLiteStorage) SetVersion(owner, entity, versionID string, version uint64) error {
	row := Version{OwnerUserID: owner, Entity: entity, VersionID: versionID, Version: version}
	if err := s.db.Save(&row).Error; err != nil {
		return fmt.Errorf("storage: set version: %w", err)
	}
	return nil
}

func (s *SQLiteStorage) ListVersions(owner string) (map[string]VersionInfo, error) {
	var rows []Version
	if err := s.db.Where("owner_user_id = ?", owner).Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("storage: list versions: %w", err)
	}
	out := make(map[string]VersionInfo, len(rows))
	for i := range rows {
		out[rows[i].Entity] = VersionInfo{VersionID: rows[i].VersionID, Version: rows[i].Version}
	}
	return out, nil
}

// === 消息 ===

func msgDataToRow(owner string, m *model.MsgData) (*MessageRow, error) {
	b, err := jsonMarshal(m)
	if err != nil {
		return nil, err
	}
	return &MessageRow{
		OwnerUserID:    owner,
		ClientMsgID:    m.ClientMsgID,
		ServerMsgID:    m.ServerMsgID,
		Seq:            m.Seq,
		ConversationID: m.ConversationID,
		SenderID:       m.SendID,
		RecvID:         m.RecvID,
		ContentType:    m.ContentType,
		Status:         m.Status,
		SendTime:       m.SendTime,
		Content:        m.Content,
		DataJSON:       string(b),
	}, nil
}

func (s *SQLiteStorage) UpsertMsgs(owner string, msgs []*model.MsgData) error {
	rows := make([]*MessageRow, 0, len(msgs))
	for _, m := range msgs {
		if m == nil || m.Seq <= 0 {
			continue
		}
		row, err := msgDataToRow(owner, m)
		if err != nil {
			return fmt.Errorf("storage: marshal msg %s: %w", m.ClientMsgID, err)
		}
		rows = append(rows, row)
	}
	err := upsertBatch(s.db, rows, []clause.Column{
		{Name: "owner_user_id"}, {Name: "client_msg_id"},
	})
	if err != nil {
		return fmt.Errorf("storage: upsert msgs: %w", err)
	}
	return nil
}

func (s *SQLiteStorage) ListMsgsByConversation(owner, conversationID string) ([]*model.MsgData, error) {
	var rows []MessageRow
	if err := s.db.Where("owner_user_id = ? AND conversation_id = ?", owner, conversationID).
		Order("seq ASC").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("storage: list msgs: %w", err)
	}
	out := make([]*model.MsgData, 0, len(rows))
	for i := range rows {
		m := &model.MsgData{}
		if err := jsonUnmarshal(rows[i].DataJSON, m); err != nil {
			return nil, fmt.Errorf("storage: unmarshal msg %s: %w", rows[i].ClientMsgID, err)
		}
		out = append(out, m)
	}
	return out, nil
}

func (s *SQLiteStorage) DeleteMsgs(owner, conversationID string, seqs []int64) error {
	if conversationID == "" || len(seqs) == 0 {
		return nil
	}
	if err := s.db.Where("owner_user_id = ? AND conversation_id = ? AND seq IN ?", owner, conversationID, seqs).
		Delete(&MessageRow{}).Error; err != nil {
		return fmt.Errorf("storage: delete msgs: %w", err)
	}
	return nil
}

// mutateMsgRows 按查询条件读-改-写消息缓存：mutate 修改 model 后同步 DataJSON 与冗余列
// （通知实时应用，2101 撤回 / 2200 已读）
func (s *SQLiteStorage) mutateMsgRows(query func(*gorm.DB) *gorm.DB, mutate func(*model.MsgData)) error {
	var rows []MessageRow
	if err := query(s.db).Find(&rows).Error; err != nil {
		return fmt.Errorf("storage: query msgs: %w", err)
	}
	for i := range rows {
		m := &model.MsgData{}
		if err := jsonUnmarshal(rows[i].DataJSON, m); err != nil {
			return fmt.Errorf("storage: unmarshal msg %s: %w", rows[i].ClientMsgID, err)
		}
		mutate(m)
		b, err := jsonMarshal(m)
		if err != nil {
			return fmt.Errorf("storage: marshal msg %s: %w", rows[i].ClientMsgID, err)
		}
		rows[i].DataJSON = string(b)
		rows[i].Status = m.Status
		rows[i].Content = m.Content
		if err := s.db.Save(&rows[i]).Error; err != nil {
			return fmt.Errorf("storage: update msg %s: %w", rows[i].ClientMsgID, err)
		}
	}
	return nil
}

func (s *SQLiteStorage) MarkMsgsRead(owner, conversationID string, seqs []int64) error {
	if conversationID == "" || len(seqs) == 0 {
		return nil
	}
	return s.mutateMsgRows(
		func(db *gorm.DB) *gorm.DB {
			return db.Where("owner_user_id = ? AND conversation_id = ? AND seq IN ?", owner, conversationID, seqs)
		},
		func(m *model.MsgData) { m.IsRead = true },
	)
}

func (s *SQLiteStorage) MarkMsgsReadUpTo(owner, conversationID string, hasReadSeq int64) error {
	if conversationID == "" || hasReadSeq <= 0 {
		return nil
	}
	return s.mutateMsgRows(
		func(db *gorm.DB) *gorm.DB {
			return db.Where("owner_user_id = ? AND conversation_id = ? AND seq <= ?", owner, conversationID, hasReadSeq)
		},
		func(m *model.MsgData) { m.IsRead = true },
	)
}

func (s *SQLiteStorage) MarkMsgsRevoked(owner, conversationID string, seqs []int64) error {
	if conversationID == "" || len(seqs) == 0 {
		return nil
	}
	return s.mutateMsgRows(
		func(db *gorm.DB) *gorm.DB {
			return db.Where("owner_user_id = ? AND conversation_id = ? AND seq IN ?", owner, conversationID, seqs)
		},
		func(m *model.MsgData) { m.Status = model.MsgStatusRevoked },
	)
}

// === 会话 ===

func (s *SQLiteStorage) GetLatestMsg(owner, conversationID string) (*model.MsgData, error) {
	if conversationID == "" {
		return nil, nil
	}
	var row MessageRow
	err := s.db.Where("owner_user_id = ? AND conversation_id = ?", owner, conversationID).
		Order("seq DESC").First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("storage: get latest msg of %s: %w", conversationID, err)
	}
	m := &model.MsgData{}
	if err := jsonUnmarshal(row.DataJSON, m); err != nil {
		return nil, fmt.Errorf("storage: unmarshal latest msg %s: %w", conversationID, err)
	}
	return m, nil
}

func convToRow(owner string, c *model.Conversation) (*ConversationRow, error) {
	b, err := jsonMarshal(c)
	if err != nil {
		return nil, err
	}
	return &ConversationRow{
		OwnerUserID:       owner,
		ConversationID:    c.ConversationID,
		ConversationType:  c.ConversationType,
		RecvMsgOpt:        c.RecvMsgOpt,
		UnreadCount:       c.UnreadCount,
		IsPinned:          c.IsPinned,
		LatestMsgSendTime: c.LatestMsgSendTime,
		DraftText:         c.DraftText,
		Ex:                c.Ex,
		DataJSON:          string(b),
		MsgInfo:           &MsgInfoWrapper{V: c.MsgInfo},
	}, nil
}

func (s *SQLiteStorage) UpsertConversations(owner string, convs []*model.Conversation) error {
	ids := make([]string, 0, len(convs))
	for _, c := range convs {
		if c != nil && c.ConversationID != "" {
			ids = append(ids, c.ConversationID)
		}
	}
	// 读取已有会话，保留 SDK 本地扩展字段（服务端 DTO 不携带摘要）；
	// 仅当入参存在缺少摘要的会话（同步路径）时才需要读旧值
	needMerge := false
	for _, c := range convs {
		if c != nil && c.ConversationID != "" && c.MsgInfo == nil && c.LatestMsgSendTime == 0 {
			needMerge = true
			break
		}
	}
	existing := make(map[string]*model.Conversation, len(ids))
	if needMerge && len(ids) > 0 {
		var rows []ConversationRow
		if err := s.db.Where("owner_user_id = ? AND conversation_id IN ?", owner, ids).
			Find(&rows).Error; err != nil {
			return fmt.Errorf("storage: load conversations for merge: %w", err)
		}
		for i := range rows {
			c := &model.Conversation{}
			if err := jsonUnmarshal(rows[i].DataJSON, c); err != nil {
				continue
			}
			existing[rows[i].ConversationID] = c
		}
	}

	rows := make([]*ConversationRow, 0, len(convs))
	for _, c := range convs {
		if c == nil || c.ConversationID == "" {
			continue
		}
		mergeConversationLocalFields(existing[c.ConversationID], c)
		row, err := convToRow(owner, c)
		if err != nil {
			return fmt.Errorf("storage: marshal conv %s: %w", c.ConversationID, err)
		}
		rows = append(rows, row)
	}
	err := upsertBatch(s.db, rows, []clause.Column{
		{Name: "owner_user_id"}, {Name: "conversation_id"},
	})
	if err != nil {
		return fmt.Errorf("storage: upsert convs: %w", err)
	}
	return nil
}

func (s *SQLiteStorage) DeleteConversations(owner string, conversationIDs []string) error {
	if len(conversationIDs) == 0 {
		return nil
	}
	if err := s.db.Where("owner_user_id = ? AND conversation_id IN ?", owner, conversationIDs).
		Delete(&ConversationRow{}).Error; err != nil {
		return fmt.Errorf("storage: delete conversations: %w", err)
	}
	return nil
}

func (s *SQLiteStorage) GetConversation(owner, conversationID string) (*model.Conversation, error) {
	if conversationID == "" {
		return nil, nil
	}
	var row ConversationRow
	err := s.db.Where("owner_user_id = ? AND conversation_id = ?", owner, conversationID).
		First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("storage: get conversation %s: %w", conversationID, err)
	}
	c := &model.Conversation{}
	if err := jsonUnmarshal(row.DataJSON, c); err != nil {
		return nil, fmt.Errorf("storage: unmarshal conv %s: %w", conversationID, err)
	}
	return c, nil
}

func (s *SQLiteStorage) ListConversations(owner string) ([]*model.Conversation, error) {
	var rows []ConversationRow
	if err := s.db.Where("owner_user_id = ?", owner).
		Order("latest_msg_send_time DESC").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("storage: list conversations: %w", err)
	}
	out := make([]*model.Conversation, 0, len(rows))
	for i := range rows {
		c := &model.Conversation{}
		if err := jsonUnmarshal(rows[i].DataJSON, c); err != nil {
			return nil, fmt.Errorf("storage: unmarshal conv %s: %w", rows[i].ConversationID, err)
		}
		out = append(out, c)
	}
	return out, nil
}

func (s *SQLiteStorage) ListConversationIDs(owner string) ([]string, error) {
	var ids []string
	if err := s.db.Model(&ConversationRow{}).Where("owner_user_id = ?", owner).
		Pluck("conversation_id", &ids).Error; err != nil {
		return nil, fmt.Errorf("storage: list conversation ids: %w", err)
	}
	return ids, nil
}

// === 好友 ===

func (s *SQLiteStorage) UpsertFriends(owner string, friends []*model.FriendInfo) error {
	rows := make([]*FriendRow, 0, len(friends))
	for _, f := range friends {
		if f == nil || f.FriendUserID() == "" {
			continue
		}
		b, err := jsonMarshal(f)
		if err != nil {
			return fmt.Errorf("storage: marshal friend %s: %w", f.FriendUserID(), err)
		}
		rows = append(rows, &FriendRow{
			OwnerUserID:  owner,
			FriendUserID: f.FriendUserID(),
			Remark:       f.Remark,
			IsPinned:     f.IsPinned,
			AddSource:    f.AddSource,
			DataJSON:     string(b),
		})
	}
	err := upsertBatch(s.db, rows, []clause.Column{
		{Name: "owner_user_id"}, {Name: "friend_user_id"},
	})
	if err != nil {
		return fmt.Errorf("storage: upsert friends: %w", err)
	}
	return nil
}

func (s *SQLiteStorage) DeleteFriends(owner string, friendUserIDs []string) error {
	if len(friendUserIDs) == 0 {
		return nil
	}
	if err := s.db.Where("owner_user_id = ? AND friend_user_id IN ?", owner, friendUserIDs).
		Delete(&FriendRow{}).Error; err != nil {
		return fmt.Errorf("storage: delete friends: %w", err)
	}
	return nil
}

func (s *SQLiteStorage) ListFriends(owner string) ([]*model.FriendInfo, error) {
	var rows []FriendRow
	if err := s.db.Where("owner_user_id = ?", owner).Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("storage: list friends: %w", err)
	}
	dest := make([]*model.FriendInfo, 0, len(rows))
	for i := range rows {
		f := &model.FriendInfo{}
		if err := jsonUnmarshal(rows[i].DataJSON, f); err != nil {
			return nil, fmt.Errorf("storage: unmarshal friend %s: %w", rows[i].FriendUserID, err)
		}
		dest = append(dest, f)
	}
	return dest, nil
}

func (s *SQLiteStorage) ListFriendIDs(owner string) ([]string, error) {
	var ids []string
	if err := s.db.Model(&FriendRow{}).Where("owner_user_id = ?", owner).
		Pluck("friend_user_id", &ids).Error; err != nil {
		return nil, fmt.Errorf("storage: list friend ids: %w", err)
	}
	return ids, nil
}

// === 黑名单 ===

func (s *SQLiteStorage) UpsertBlacks(owner string, blacks []*model.BlackInfo) error {
	rows := make([]*BlackRow, 0, len(blacks))
	for _, b := range blacks {
		if b == nil || b.BlackUserID() == "" {
			continue
		}
		data, err := jsonMarshal(b)
		if err != nil {
			return fmt.Errorf("storage: marshal black %s: %w", b.BlackUserID(), err)
		}
		rows = append(rows, &BlackRow{OwnerUserID: owner, BlackUserID: b.BlackUserID(), DataJSON: string(data)})
	}
	err := upsertBatch(s.db, rows, []clause.Column{
		{Name: "owner_user_id"}, {Name: "black_user_id"},
	})
	if err != nil {
		return fmt.Errorf("storage: upsert blacks: %w", err)
	}
	return nil
}

func (s *SQLiteStorage) DeleteBlacks(owner string, blackUserIDs []string) error {
	if len(blackUserIDs) == 0 {
		return nil
	}
	if err := s.db.Where("owner_user_id = ? AND black_user_id IN ?", owner, blackUserIDs).
		Delete(&BlackRow{}).Error; err != nil {
		return fmt.Errorf("storage: delete blacks: %w", err)
	}
	return nil
}

func (s *SQLiteStorage) ListBlacks(owner string) ([]*model.BlackInfo, error) {
	var rows []BlackRow
	if err := s.db.Where("owner_user_id = ?", owner).Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("storage: list blacks: %w", err)
	}
	out := make([]*model.BlackInfo, 0, len(rows))
	for i := range rows {
		b := &model.BlackInfo{}
		if err := jsonUnmarshal(rows[i].DataJSON, b); err != nil {
			return nil, fmt.Errorf("storage: unmarshal black %s: %w", rows[i].BlackUserID, err)
		}
		out = append(out, b)
	}
	return out, nil
}

func (s *SQLiteStorage) ListBlackIDs(owner string) ([]string, error) {
	var ids []string
	if err := s.db.Model(&BlackRow{}).Where("owner_user_id = ?", owner).
		Pluck("black_user_id", &ids).Error; err != nil {
		return nil, fmt.Errorf("storage: list black ids: %w", err)
	}
	return ids, nil
}

// === 群 ===

func (s *SQLiteStorage) UpsertGroups(owner string, groups []*model.GroupInfo) error {
	rows := make([]*GroupRow, 0, len(groups))
	for _, g := range groups {
		if g == nil || g.GroupID == "" {
			continue
		}
		b, err := jsonMarshal(g)
		if err != nil {
			return fmt.Errorf("storage: marshal group %s: %w", g.GroupID, err)
		}
		rows = append(rows, &GroupRow{
			OwnerUserID: owner,
			GroupID:     g.GroupID,
			GroupName:   g.GroupName,
			MemberCount: int32(g.MemberCount),
			Status:      g.Status,
			DataJSON:    string(b),
		})
	}
	err := upsertBatch(s.db, rows, []clause.Column{
		{Name: "owner_user_id"}, {Name: "group_id"},
	})
	if err != nil {
		return fmt.Errorf("storage: upsert groups: %w", err)
	}
	return nil
}

func (s *SQLiteStorage) DeleteGroups(owner string, groupIDs []string) error {
	if len(groupIDs) == 0 {
		return nil
	}
	// 群删除时成员缓存一并清理
	return s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("owner_user_id = ? AND group_id IN ?", owner, groupIDs).
			Delete(&GroupRow{}).Error; err != nil {
			return fmt.Errorf("storage: delete groups: %w", err)
		}
		if err := tx.Where("owner_user_id = ? AND group_id IN ?", owner, groupIDs).
			Delete(&GroupMemberRow{}).Error; err != nil {
			return fmt.Errorf("storage: delete group members of groups: %w", err)
		}
		return nil
	})
}

func (s *SQLiteStorage) ListGroups(owner string) ([]*model.GroupInfo, error) {
	var rows []GroupRow
	if err := s.db.Where("owner_user_id = ?", owner).Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("storage: list groups: %w", err)
	}
	out := make([]*model.GroupInfo, 0, len(rows))
	for i := range rows {
		g := &model.GroupInfo{}
		if err := jsonUnmarshal(rows[i].DataJSON, g); err != nil {
			return nil, fmt.Errorf("storage: unmarshal group %s: %w", rows[i].GroupID, err)
		}
		out = append(out, g)
	}
	return out, nil
}

func (s *SQLiteStorage) ListGroupIDs(owner string) ([]string, error) {
	var ids []string
	if err := s.db.Model(&GroupRow{}).Where("owner_user_id = ?", owner).
		Pluck("group_id", &ids).Error; err != nil {
		return nil, fmt.Errorf("storage: list group ids: %w", err)
	}
	return ids, nil
}

// === 群成员 ===

func (s *SQLiteStorage) UpsertGroupMembers(owner, groupID string, members []*model.GroupMemberFullInfo) error {
	rows := make([]*GroupMemberRow, 0, len(members))
	for _, m := range members {
		if m == nil || m.UserID == "" {
			continue
		}
		b, err := jsonMarshal(m)
		if err != nil {
			return fmt.Errorf("storage: marshal member %s: %w", m.UserID, err)
		}
		rows = append(rows, &GroupMemberRow{
			OwnerUserID: owner,
			GroupID:     groupID,
			UserID:      m.UserID,
			RoleLevel:   m.RoleLevel,
			JoinTime:    m.JoinTime,
			Nickname:    m.Nickname,
			DataJSON:    string(b),
		})
	}
	err := upsertBatch(s.db, rows, []clause.Column{
		{Name: "owner_user_id"}, {Name: "group_id"}, {Name: "user_id"},
	})
	if err != nil {
		return fmt.Errorf("storage: upsert members: %w", err)
	}
	return nil
}

func (s *SQLiteStorage) DeleteGroupMembers(owner, groupID string, userIDs []string) error {
	if len(userIDs) == 0 {
		return nil
	}
	if err := s.db.Where("owner_user_id = ? AND group_id = ? AND user_id IN ?", owner, groupID, userIDs).
		Delete(&GroupMemberRow{}).Error; err != nil {
		return fmt.Errorf("storage: delete group members: %w", err)
	}
	return nil
}

func (s *SQLiteStorage) listGroupMembers(owner, groupID string, dest *[]*model.GroupMemberFullInfo) error {
	var rows []GroupMemberRow
	if err := s.db.Where("owner_user_id = ? AND group_id = ?", owner, groupID).Find(&rows).Error; err != nil {
		return fmt.Errorf("storage: list group members: %w", err)
	}
	*dest = make([]*model.GroupMemberFullInfo, 0, len(rows))
	for i := range rows {
		m := &model.GroupMemberFullInfo{}
		if err := jsonUnmarshal(rows[i].DataJSON, m); err != nil {
			return fmt.Errorf("storage: unmarshal member %s: %w", rows[i].UserID, err)
		}
		*dest = append(*dest, m)
	}
	return nil
}

func (s *SQLiteStorage) ListGroupMembers(owner, groupID string) ([]*model.GroupMemberFullInfo, error) {
	var out []*model.GroupMemberFullInfo
	if err := s.listGroupMembers(owner, groupID, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *SQLiteStorage) ListGroupMemberIDs(owner, groupID string) ([]string, error) {
	var ids []string
	if err := s.db.Model(&GroupMemberRow{}).Where("owner_user_id = ? AND group_id = ?", owner, groupID).
		Pluck("user_id", &ids).Error; err != nil {
		return nil, fmt.Errorf("storage: list group member ids: %w", err)
	}
	return ids, nil
}

// === 本地分页 / 单条查询（Local Data API）===

func (s *SQLiteStorage) ListConversationsPage(owner string, offset, limit int32) (*model.ConversationPage, error) {
	if offset < 0 {
		offset = 0
	}
	var total int64
	if err := s.db.Model(&ConversationRow{}).Where("owner_user_id = ?", owner).Count(&total).Error; err != nil {
		return nil, fmt.Errorf("storage: count conversations: %w", err)
	}
	db := s.db.Where("owner_user_id = ?", owner).
		Order("is_pinned DESC").Order("latest_msg_send_time DESC").
		Offset(int(offset))
	if limit > 0 {
		db = db.Limit(int(limit))
	}
	var rows []ConversationRow
	if err := db.Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("storage: list conversations page: %w", err)
	}
	items := make([]*model.Conversation, 0, len(rows))
	for i := range rows {
		c := &model.Conversation{}
		if err := jsonUnmarshal(rows[i].DataJSON, c); err != nil {
			return nil, fmt.Errorf("storage: unmarshal conv %s: %w", rows[i].ConversationID, err)
		}
		items = append(items, c)
	}
	hasMore := int64(int(offset)+len(items)) < total
	return &model.ConversationPage{Items: items, Total: total, HasMore: hasMore}, nil
}

func (s *SQLiteStorage) GetConversations(owner string, conversationIDs []string) ([]*model.Conversation, error) {
	if len(conversationIDs) == 0 {
		return nil, nil
	}
	var rows []ConversationRow
	if err := s.db.Where("owner_user_id = ? AND conversation_id IN ?", owner, conversationIDs).
		Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("storage: get conversations: %w", err)
	}
	byID := make(map[string]*model.Conversation, len(rows))
	for i := range rows {
		c := &model.Conversation{}
		if err := jsonUnmarshal(rows[i].DataJSON, c); err != nil {
			return nil, fmt.Errorf("storage: unmarshal conv %s: %w", rows[i].ConversationID, err)
		}
		byID[rows[i].ConversationID] = c
	}
	out := make([]*model.Conversation, 0, len(byID))
	for _, id := range conversationIDs {
		if c := byID[id]; c != nil {
			out = append(out, c)
		}
	}
	return out, nil
}

func (s *SQLiteStorage) ListMsgsPage(owner, conversationID string, seq int64, limit int32, desc bool) (*model.MsgPage, error) {
	if limit <= 0 {
		limit = 50
	}
	db := s.db.Model(&MessageRow{}).Where("owner_user_id = ? AND conversation_id = ?", owner, conversationID)
	if desc {
		if seq > 0 {
			db = db.Where("seq <= ?", seq)
		}
		db = db.Order("seq DESC")
	} else {
		if seq > 0 {
			db = db.Where("seq > ?", seq)
		}
		db = db.Order("seq ASC")
	}
	var rows []MessageRow
	if err := db.Limit(int(limit) + 1).Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("storage: list msgs page: %w", err)
	}
	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}
	page := &model.MsgPage{Items: make([]*model.MsgData, 0, len(rows)), HasMore: hasMore}
	for i := range rows {
		m := &model.MsgData{}
		if err := jsonUnmarshal(rows[i].DataJSON, m); err != nil {
			return nil, fmt.Errorf("storage: unmarshal msg %s: %w", rows[i].ClientMsgID, err)
		}
		if page.OldestSeq == 0 || m.Seq < page.OldestSeq {
			page.OldestSeq = m.Seq
		}
		if m.Seq > page.NewestSeq {
			page.NewestSeq = m.Seq
		}
		page.Items = append(page.Items, m)
	}
	return page, nil
}

func (s *SQLiteStorage) GetMsg(owner, conversationID string, seq int64) (*model.MsgData, error) {
	var row MessageRow
	err := s.db.Where("owner_user_id = ? AND conversation_id = ? AND seq = ?", owner, conversationID, seq).
		First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("storage: get msg seq=%d: %w", seq, err)
	}
	m := &model.MsgData{}
	if err := jsonUnmarshal(row.DataJSON, m); err != nil {
		return nil, fmt.Errorf("storage: unmarshal msg %s: %w", row.ClientMsgID, err)
	}
	return m, nil
}

func (s *SQLiteStorage) GetMsgByClientID(owner, conversationID, clientMsgID string) (*model.MsgData, error) {
	var row MessageRow
	err := s.db.Where("owner_user_id = ? AND client_msg_id = ? AND conversation_id = ?",
		owner, clientMsgID, conversationID).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("storage: get msg by clientMsgID %s: %w", clientMsgID, err)
	}
	m := &model.MsgData{}
	if err := jsonUnmarshal(row.DataJSON, m); err != nil {
		return nil, fmt.Errorf("storage: unmarshal msg %s: %w", clientMsgID, err)
	}
	return m, nil
}

func (s *SQLiteStorage) ListFriendsPage(owner string, offset, limit int32) (*model.FriendPage, error) {
	if offset < 0 {
		offset = 0
	}
	var total int64
	if err := s.db.Model(&FriendRow{}).Where("owner_user_id = ?", owner).Count(&total).Error; err != nil {
		return nil, fmt.Errorf("storage: count friends: %w", err)
	}
	db := s.db.Where("owner_user_id = ?", owner).
		Order("is_pinned DESC").Order("friend_user_id ASC").
		Offset(int(offset))
	if limit > 0 {
		db = db.Limit(int(limit))
	}
	var rows []FriendRow
	if err := db.Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("storage: list friends page: %w", err)
	}
	items := make([]*model.FriendInfo, 0, len(rows))
	for i := range rows {
		f := &model.FriendInfo{}
		if err := jsonUnmarshal(rows[i].DataJSON, f); err != nil {
			return nil, fmt.Errorf("storage: unmarshal friend %s: %w", rows[i].FriendUserID, err)
		}
		items = append(items, f)
	}
	hasMore := int64(int(offset)+len(items)) < total
	return &model.FriendPage{Items: items, Total: total, HasMore: hasMore}, nil
}

func (s *SQLiteStorage) GetFriend(owner, friendUserID string) (*model.FriendInfo, error) {
	var row FriendRow
	err := s.db.Where("owner_user_id = ? AND friend_user_id = ?", owner, friendUserID).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("storage: get friend %s: %w", friendUserID, err)
	}
	f := &model.FriendInfo{}
	if err := jsonUnmarshal(row.DataJSON, f); err != nil {
		return nil, fmt.Errorf("storage: unmarshal friend %s: %w", friendUserID, err)
	}
	return f, nil
}

func (s *SQLiteStorage) ListBlacksPage(owner string, offset, limit int32) (*model.BlackPage, error) {
	if offset < 0 {
		offset = 0
	}
	var total int64
	if err := s.db.Model(&BlackRow{}).Where("owner_user_id = ?", owner).Count(&total).Error; err != nil {
		return nil, fmt.Errorf("storage: count blacks: %w", err)
	}
	db := s.db.Where("owner_user_id = ?", owner).Order("black_user_id ASC").Offset(int(offset))
	if limit > 0 {
		db = db.Limit(int(limit))
	}
	var rows []BlackRow
	if err := db.Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("storage: list blacks page: %w", err)
	}
	items := make([]*model.BlackInfo, 0, len(rows))
	for i := range rows {
		b := &model.BlackInfo{}
		if err := jsonUnmarshal(rows[i].DataJSON, b); err != nil {
			return nil, fmt.Errorf("storage: unmarshal black %s: %w", rows[i].BlackUserID, err)
		}
		items = append(items, b)
	}
	hasMore := int64(int(offset)+len(items)) < total
	return &model.BlackPage{Items: items, Total: total, HasMore: hasMore}, nil
}

func (s *SQLiteStorage) GetBlack(owner, blackUserID string) (*model.BlackInfo, error) {
	var row BlackRow
	err := s.db.Where("owner_user_id = ? AND black_user_id = ?", owner, blackUserID).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("storage: get black %s: %w", blackUserID, err)
	}
	b := &model.BlackInfo{}
	if err := jsonUnmarshal(row.DataJSON, b); err != nil {
		return nil, fmt.Errorf("storage: unmarshal black %s: %w", blackUserID, err)
	}
	return b, nil
}

func (s *SQLiteStorage) ListGroupsPage(owner string, offset, limit int32) (*model.GroupPage, error) {
	if offset < 0 {
		offset = 0
	}
	var total int64
	if err := s.db.Model(&GroupRow{}).Where("owner_user_id = ?", owner).Count(&total).Error; err != nil {
		return nil, fmt.Errorf("storage: count groups: %w", err)
	}
	db := s.db.Where("owner_user_id = ?", owner).
		Order("group_name ASC").Order("group_id ASC").
		Offset(int(offset))
	if limit > 0 {
		db = db.Limit(int(limit))
	}
	var rows []GroupRow
	if err := db.Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("storage: list groups page: %w", err)
	}
	items := make([]*model.GroupInfo, 0, len(rows))
	for i := range rows {
		g := &model.GroupInfo{}
		if err := jsonUnmarshal(rows[i].DataJSON, g); err != nil {
			return nil, fmt.Errorf("storage: unmarshal group %s: %w", rows[i].GroupID, err)
		}
		items = append(items, g)
	}
	hasMore := int64(int(offset)+len(items)) < total
	return &model.GroupPage{Items: items, Total: total, HasMore: hasMore}, nil
}

func (s *SQLiteStorage) GetGroup(owner, groupID string) (*model.GroupInfo, error) {
	var row GroupRow
	err := s.db.Where("owner_user_id = ? AND group_id = ?", owner, groupID).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("storage: get group %s: %w", groupID, err)
	}
	g := &model.GroupInfo{}
	if err := jsonUnmarshal(row.DataJSON, g); err != nil {
		return nil, fmt.Errorf("storage: unmarshal group %s: %w", groupID, err)
	}
	return g, nil
}

func (s *SQLiteStorage) ListGroupMembersPage(owner, groupID string, offset, limit int32) (*model.GroupMemberPage, error) {
	if offset < 0 {
		offset = 0
	}
	var total int64
	if err := s.db.Model(&GroupMemberRow{}).
		Where("owner_user_id = ? AND group_id = ?", owner, groupID).Count(&total).Error; err != nil {
		return nil, fmt.Errorf("storage: count group members: %w", err)
	}
	db := s.db.Where("owner_user_id = ? AND group_id = ?", owner, groupID).
		Order("role_level DESC").Order("join_time ASC").Order("user_id ASC").
		Offset(int(offset))
	if limit > 0 {
		db = db.Limit(int(limit))
	}
	var rows []GroupMemberRow
	if err := db.Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("storage: list group members page: %w", err)
	}
	items := make([]*model.GroupMemberFullInfo, 0, len(rows))
	for i := range rows {
		m := &model.GroupMemberFullInfo{}
		if err := jsonUnmarshal(rows[i].DataJSON, m); err != nil {
			return nil, fmt.Errorf("storage: unmarshal group member %s: %w", rows[i].UserID, err)
		}
		items = append(items, m)
	}
	hasMore := int64(int(offset)+len(items)) < total
	return &model.GroupMemberPage{Items: items, Total: total, HasMore: hasMore}, nil
}

func (s *SQLiteStorage) GetGroupMember(owner, groupID, userID string) (*model.GroupMemberFullInfo, error) {
	var row GroupMemberRow
	err := s.db.Where("owner_user_id = ? AND group_id = ? AND user_id = ?", owner, groupID, userID).
		First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("storage: get group member %s: %w", userID, err)
	}
	m := &model.GroupMemberFullInfo{}
	if err := jsonUnmarshal(row.DataJSON, m); err != nil {
		return nil, fmt.Errorf("storage: unmarshal group member %s: %w", userID, err)
	}
	return m, nil
}

// === 本地搜索 / 计数（Local Data API P2）===

// searchLikePattern 生成 LOWER(data_json) LIKE 的匹配串（转义 % _ \，避免用户关键字被当通配符）
func searchLikePattern(keyword string) string {
	kw := strings.ToLower(keyword)
	kw = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(kw)
	return "%" + kw + "%"
}

// searchKeywordCond 搜索 WHERE 片段：owner 隔离 + JSON 文本大小写不敏感匹配
const searchKeywordCond = "LOWER(data_json) LIKE ? ESCAPE '\\'"

func (s *SQLiteStorage) SearchConversations(owner, keyword string, limit int32) (*model.ConversationPage, error) {
	if limit <= 0 {
		limit = entitySearchDefaultLimit
	}
	if keyword == "" {
		return &model.ConversationPage{Items: []*model.Conversation{}, Total: 0}, nil
	}
	where := "owner_user_id = ? AND " + searchKeywordCond
	var total int64
	if err := s.db.Model(&ConversationRow{}).Where(where, owner, searchLikePattern(keyword)).Count(&total).Error; err != nil {
		return nil, fmt.Errorf("storage: search conversations count: %w", err)
	}
	var rows []ConversationRow
	if err := s.db.Where(where, owner, searchLikePattern(keyword)).
		Order("is_pinned DESC").Order("latest_msg_send_time DESC").
		Limit(int(limit)).Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("storage: search conversations: %w", err)
	}
	items := make([]*model.Conversation, 0, len(rows))
	for i := range rows {
		c := &model.Conversation{}
		if err := jsonUnmarshal(rows[i].DataJSON, c); err != nil {
			return nil, fmt.Errorf("storage: unmarshal conv %s: %w", rows[i].ConversationID, err)
		}
		items = append(items, c)
	}
	return &model.ConversationPage{Items: items, Total: total, HasMore: int64(len(items)) < total}, nil
}

func (s *SQLiteStorage) SearchMsgs(owner, conversationID, keyword string, limit int32) (*model.MsgPage, error) {
	if limit <= 0 {
		limit = msgSearchDefaultLimit
	}
	if keyword == "" {
		return &model.MsgPage{Items: []*model.MsgData{}}, nil
	}
	var rows []MessageRow
	if err := s.db.Where("owner_user_id = ? AND conversation_id = ? AND "+searchKeywordCond,
		owner, conversationID, searchLikePattern(keyword)).
		Order("seq DESC").Limit(int(limit) + 1).Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("storage: search msgs: %w", err)
	}
	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}
	items := make([]*model.MsgData, 0, len(rows))
	for i := range rows {
		m := &model.MsgData{}
		if err := jsonUnmarshal(rows[i].DataJSON, m); err != nil {
			return nil, fmt.Errorf("storage: unmarshal msg %s: %w", rows[i].ClientMsgID, err)
		}
		items = append(items, m)
	}
	return buildMsgPage(items, hasMore), nil
}

func (s *SQLiteStorage) SearchFriends(owner, keyword string, limit int32) (*model.FriendPage, error) {
	if limit <= 0 {
		limit = entitySearchDefaultLimit
	}
	if keyword == "" {
		return &model.FriendPage{Items: []*model.FriendInfo{}, Total: 0}, nil
	}
	where := "owner_user_id = ? AND " + searchKeywordCond
	var total int64
	if err := s.db.Model(&FriendRow{}).Where(where, owner, searchLikePattern(keyword)).Count(&total).Error; err != nil {
		return nil, fmt.Errorf("storage: search friends count: %w", err)
	}
	var rows []FriendRow
	if err := s.db.Where(where, owner, searchLikePattern(keyword)).
		Order("is_pinned DESC").Order("friend_user_id ASC").
		Limit(int(limit)).Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("storage: search friends: %w", err)
	}
	items := make([]*model.FriendInfo, 0, len(rows))
	for i := range rows {
		f := &model.FriendInfo{}
		if err := jsonUnmarshal(rows[i].DataJSON, f); err != nil {
			return nil, fmt.Errorf("storage: unmarshal friend %s: %w", rows[i].FriendUserID, err)
		}
		items = append(items, f)
	}
	return &model.FriendPage{Items: items, Total: total, HasMore: int64(len(items)) < total}, nil
}

func (s *SQLiteStorage) SearchGroups(owner, keyword string, limit int32) (*model.GroupPage, error) {
	if limit <= 0 {
		limit = entitySearchDefaultLimit
	}
	if keyword == "" {
		return &model.GroupPage{Items: []*model.GroupInfo{}, Total: 0}, nil
	}
	where := "owner_user_id = ? AND " + searchKeywordCond
	var total int64
	if err := s.db.Model(&GroupRow{}).Where(where, owner, searchLikePattern(keyword)).Count(&total).Error; err != nil {
		return nil, fmt.Errorf("storage: search groups count: %w", err)
	}
	var rows []GroupRow
	if err := s.db.Where(where, owner, searchLikePattern(keyword)).
		Order("group_name ASC").Order("group_id ASC").
		Limit(int(limit)).Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("storage: search groups: %w", err)
	}
	items := make([]*model.GroupInfo, 0, len(rows))
	for i := range rows {
		g := &model.GroupInfo{}
		if err := jsonUnmarshal(rows[i].DataJSON, g); err != nil {
			return nil, fmt.Errorf("storage: unmarshal group %s: %w", rows[i].GroupID, err)
		}
		items = append(items, g)
	}
	return &model.GroupPage{Items: items, Total: total, HasMore: int64(len(items)) < total}, nil
}

func (s *SQLiteStorage) CountConversations(owner string) (int64, error) {
	var n int64
	if err := s.db.Model(&ConversationRow{}).Where("owner_user_id = ?", owner).Count(&n).Error; err != nil {
		return 0, fmt.Errorf("storage: count conversations: %w", err)
	}
	return n, nil
}

func (s *SQLiteStorage) CountMsgs(owner, conversationID string) (int64, error) {
	q := s.db.Model(&MessageRow{}).Where("owner_user_id = ?", owner)
	if conversationID != "" {
		q = q.Where("conversation_id = ?", conversationID)
	}
	var n int64
	if err := q.Count(&n).Error; err != nil {
		return 0, fmt.Errorf("storage: count msgs: %w", err)
	}
	return n, nil
}

func (s *SQLiteStorage) CountFriends(owner string) (int64, error) {
	var n int64
	if err := s.db.Model(&FriendRow{}).Where("owner_user_id = ?", owner).Count(&n).Error; err != nil {
		return 0, fmt.Errorf("storage: count friends: %w", err)
	}
	return n, nil
}

func (s *SQLiteStorage) CountBlacks(owner string) (int64, error) {
	var n int64
	if err := s.db.Model(&BlackRow{}).Where("owner_user_id = ?", owner).Count(&n).Error; err != nil {
		return 0, fmt.Errorf("storage: count blacks: %w", err)
	}
	return n, nil
}

func (s *SQLiteStorage) CountGroups(owner string) (int64, error) {
	var n int64
	if err := s.db.Model(&GroupRow{}).Where("owner_user_id = ?", owner).Count(&n).Error; err != nil {
		return 0, fmt.Errorf("storage: count groups: %w", err)
	}
	return n, nil
}

func (s *SQLiteStorage) CountGroupMembers(owner, groupID string) (int64, error) {
	q := s.db.Model(&GroupMemberRow{}).Where("owner_user_id = ?", owner)
	if groupID != "" {
		q = q.Where("group_id = ?", groupID)
	}
	var n int64
	if err := q.Count(&n).Error; err != nil {
		return 0, fmt.Errorf("storage: count group members: %w", err)
	}
	return n, nil
}

// === 生命周期 ===

func (s *SQLiteStorage) Clear(_ context.Context, owner string) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		for _, m := range allModels {
			// sdk_meta 为 SDK 级数据（设备 ID 等），无 owner 列且需跨 Logout 保留
			if _, isMeta := m.(*SDKMetaRow); isMeta {
				continue
			}
			if err := tx.Where("owner_user_id = ?", owner).Delete(m).Error; err != nil {
				return fmt.Errorf("storage: clear %T: %w", m, err)
			}
		}
		return nil
	})
}

// === SDK 级元数据 ===

func (s *SQLiteStorage) GetMeta(key string) (string, error) {
	var row SDKMetaRow
	err := s.db.Where("`key` = ?", key).First(&row).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return "", nil
		}
		return "", fmt.Errorf("storage: get meta %s: %w", key, err)
	}
	return row.Value, nil
}

func (s *SQLiteStorage) SetMeta(key, value string) error {
	row := SDKMetaRow{Key: key, Value: value}
	if err := s.db.Save(&row).Error; err != nil {
		return fmt.Errorf("storage: set meta %s: %w", key, err)
	}
	return nil
}

// === JSON helpers ===

// upsertBatchSize 单批行数：兼顾 SQL 语句长度与 SQLite 参数上限
const upsertBatchSize = 200

// upsertBatch 通用批量 upsert：INSERT ... ON CONFLICT(target) DO UPDATE 整行覆盖
//
// conflictCols 必须为表的主键列（SQLite 的 DO UPDATE 必须显式指定冲突目标）。
// 事务包裹保持 all-or-nothing（超过单批行数时 CreateInBatches 会拆多条语句）。
// 包级泛型 helper，不跨 gomobile 边界。
func upsertBatch[T any](db *gorm.DB, rows []T, conflictCols []clause.Column) error {
	if len(rows) == 0 {
		return nil
	}
	return db.Transaction(func(tx *gorm.DB) error {
		return tx.Clauses(clause.OnConflict{Columns: conflictCols, UpdateAll: true}).
			CreateInBatches(rows, upsertBatchSize).Error
	})
}

func jsonMarshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func jsonUnmarshal(data string, v interface{}) error {
	return json.Unmarshal([]byte(data), v)
}

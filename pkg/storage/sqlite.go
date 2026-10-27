package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	gormlogger "gorm.io/gorm/logger"

	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/convert"
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
		Seq:            convert.ToInt64(m.Seqs),
		ConversationID: m.ConversationID,
		SenderID:       m.SendID,
		RecvID:         m.RecvID,
		ContentType:    m.MsgType,
		Status:         m.Status,
		SendTime:       m.CreateTime,
		Content:        m.Content,
		DataJSON:       string(b),
	}, nil
}

func (s *SQLiteStorage) UpsertMsgs(owner string, msgs []*model.MsgData) error {
	rows := make([]*MessageRow, 0, len(msgs))
	for _, m := range msgs {
		if m == nil || convert.ToInt64(m.Seqs) <= 0 {
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

// === 会话 ===

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
	rows := make([]*ConversationRow, 0, len(convs))
	for _, c := range convs {
		if c == nil || c.ConversationID == "" {
			continue
		}
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
		if f == nil || f.FriendUserID == "" {
			continue
		}
		b, err := jsonMarshal(f)
		if err != nil {
			return fmt.Errorf("storage: marshal friend %s: %w", f.FriendUserID, err)
		}
		rows = append(rows, &FriendRow{
			OwnerUserID:  owner,
			FriendUserID: f.FriendUserID,
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
		if b == nil || b.BlackUserID == "" {
			continue
		}
		data, err := jsonMarshal(b)
		if err != nil {
			return fmt.Errorf("storage: marshal black %s: %w", b.BlackUserID, err)
		}
		rows = append(rows, &BlackRow{OwnerUserID: owner, BlackUserID: b.BlackUserID, DataJSON: string(data)})
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
			MemberCount: g.MemberCount,
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

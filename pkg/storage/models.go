package storage

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"

	"github.com/PaperMan11/goim-sdk-core/pkg/model"
)

// === gorm 表模型（8 张表，全部按 owner_user_id 隔离） ===

// LocalSeq 单值行：owner → maxSeqs/minSeqs JSON
type LocalSeq struct {
	OwnerUserID string `gorm:"primaryKey;column:owner_user_id;size:128"`
	MaxSeqsJSON string `gorm:"column:max_seqs_json;type:text"`
	MinSeqsJSON string `gorm:"column:min_seqs_json;type:text"`
}

func (LocalSeq) TableName() string { return "local_seqs" }

// Version 实体同步游标
type Version struct {
	OwnerUserID string `gorm:"primaryKey;column:owner_user_id;size:128"`
	Entity      string `gorm:"primaryKey;column:entity;size:128"`
	VersionID   string `gorm:"column:version_id;size:128"`
	Version     uint64 `gorm:"column:version"`
}

func (Version) TableName() string { return "versions" }

// MessageRow 消息缓存（(owner, client_msg_id) 主键；(owner, conversation_id, seq) 唯一索引）
type MessageRow struct {
	OwnerUserID    string `gorm:"primaryKey;column:owner_user_id;size:128"`
	ClientMsgID    string `gorm:"primaryKey;column:client_msg_id;size:128"`
	ServerMsgID    string `gorm:"column:server_msg_id;size:128"`
	Seq            int64  `gorm:"column:seq;index:idx_owner_conv_seq,unique"`
	ConversationID string `gorm:"column:conversation_id;size:128;index:idx_owner_conv_seq,unique"`
	SenderID       string `gorm:"column:sender_id;size:128"`
	RecvID         string `gorm:"column:recv_id;size:128"`
	ContentType    int32  `gorm:"column:content_type"`
	Status         int32  `gorm:"column:status"`
	SendTime       int64  `gorm:"column:send_time"`
	Content        string `gorm:"column:content;type:text"`
	DataJSON       string `gorm:"column:data_json;type:text"` // 整个 MsgData JSON（恢复原对象用）
}

func (MessageRow) TableName() string { return "messages" }

// ConversationRow 会话缓存
type ConversationRow struct {
	OwnerUserID       string          `gorm:"primaryKey;column:owner_user_id;size:128"`
	ConversationID    string          `gorm:"primaryKey;column:conversation_id;size:128"`
	ConversationType  int32           `gorm:"column:conversation_type"`
	RecvMsgOpt        int32           `gorm:"column:recv_msg_opt"`
	UnreadCount       int32           `gorm:"column:unread_count"`
	IsPinned          bool            `gorm:"column:is_pinned"`
	LatestMsgSendTime int64           `gorm:"column:latest_msg_send_time;index:idx_owner_latest_msg,sort:desc"`
	DraftText         string          `gorm:"column:draft_text;type:text"`
	Ex                string          `gorm:"column:ex;type:text"`
	DataJSON          string          `gorm:"column:data_json;type:text"` // 整个 Conversation JSON
	MsgInfo           *MsgInfoWrapper `gorm:"column:msg_info;type:text"`  // 摘要消息（nullable JSON）
}

func (ConversationRow) TableName() string { return "conversations" }

// MsgInfoWrapper 让 *model.MsgInfo 实现 gorm 的 Serializer 接口
type MsgInfoWrapper struct{ V *model.MsgInfo }

// Value 实现 driver.Valuer
func (w MsgInfoWrapper) Value() (driver.Value, error) {
	if w.V == nil {
		return nil, nil
	}
	b, err := json.Marshal(w.V)
	return string(b), err
}

// Scan 实现 sql.Scanner
func (w *MsgInfoWrapper) Scan(src interface{}) error {
	if src == nil {
		w.V = nil
		return nil
	}
	var b []byte
	switch v := src.(type) {
	case string:
		b = []byte(v)
	case []byte:
		b = v
	default:
		return fmt.Errorf("msg_info: unsupported scan type %T", src)
	}
	if len(b) == 0 {
		w.V = nil
		return nil
	}
	w.V = &model.MsgInfo{}
	return json.Unmarshal(b, w.V)
}

// FriendRow 好友缓存
type FriendRow struct {
	OwnerUserID  string `gorm:"primaryKey;column:owner_user_id;size:128"`
	FriendUserID string `gorm:"primaryKey;column:friend_user_id;size:128"`
	Remark       string `gorm:"column:remark;type:text"`
	IsPinned     bool   `gorm:"column:is_pinned"`
	AddSource    int32  `gorm:"column:add_source"`
	DataJSON     string `gorm:"column:data_json;type:text"`
}

func (FriendRow) TableName() string { return "friends" }

// BlackRow 黑名单缓存
type BlackRow struct {
	OwnerUserID string `gorm:"primaryKey;column:owner_user_id;size:128"`
	BlackUserID string `gorm:"primaryKey;column:black_user_id;size:128"`
	DataJSON    string `gorm:"column:data_json;type:text"`
}

func (BlackRow) TableName() string { return "blacks" }

// GroupRow 加入群缓存
type GroupRow struct {
	OwnerUserID string `gorm:"primaryKey;column:owner_user_id;size:128"`
	GroupID     string `gorm:"primaryKey;column:group_id;size:128"`
	GroupName   string `gorm:"column:group_name;type:text"`
	MemberCount int32  `gorm:"column:member_count"`
	Status      int32  `gorm:"column:status"`
	DataJSON    string `gorm:"column:data_json;type:text"`
}

func (GroupRow) TableName() string { return "groups" }

// GroupMemberRow 群成员缓存
type GroupMemberRow struct {
	OwnerUserID string `gorm:"primaryKey;column:owner_user_id;size:128"`
	GroupID     string `gorm:"primaryKey;column:group_id;size:128;index:idx_owner_group"`
	UserID      string `gorm:"primaryKey;column:user_id;size:128"`
	RoleLevel   int32  `gorm:"column:role_level"`
	JoinTime    int64  `gorm:"column:join_time"`
	Nickname    string `gorm:"column:nickname;type:text"`
	DataJSON    string `gorm:"column:data_json;type:text"`
}

func (GroupMemberRow) TableName() string { return "group_members" }

// allModels AutoMigrate 用
var allModels = []interface{}{
	&LocalSeq{}, &Version{}, &MessageRow{}, &ConversationRow{},
	&FriendRow{}, &BlackRow{}, &GroupRow{}, &GroupMemberRow{},
}

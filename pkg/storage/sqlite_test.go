package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
)

// === SQLite :memory: 覆盖 Storage 接口全部方法 ===

func newSQLiteForTest(t *testing.T) *SQLiteStorage {
	t.Helper()
	s, err := NewSQLiteStorage(":memory:", &logger.NopLogger{})
	if err != nil {
		t.Fatalf("new sqlite storage: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestSQLiteStorage_Seqs(t *testing.T) {
	s := newSQLiteForTest(t)

	// 空读取
	max, min, err := s.GetMaxSeqs("u1")
	if err != nil || max != nil || min != nil {
		t.Fatalf("empty get: max=%v min=%v err=%v", max, min, err)
	}

	err = s.SetMaxSeqs("u1", map[string]int64{"si_a": 100}, map[string]int64{"si_a": 90})
	if err != nil {
		t.Fatal(err)
	}
	max, min, err = s.GetMaxSeqs("u1")
	if err != nil || max["si_a"] != 100 || min["si_a"] != 90 {
		t.Fatalf("got max=%v min=%v err=%v", max, min, err)
	}
}

func TestSQLiteStorage_Versions(t *testing.T) {
	s := newSQLiteForTest(t)

	vID, v, err := s.GetVersion("u1", EntityFriends)
	if err != nil || vID != "" || v != 0 {
		t.Fatalf("empty: %q %d %v", vID, v, err)
	}

	if err := s.SetVersion("u1", EntityFriends, "vid-1", 5); err != nil {
		t.Fatal(err)
	}
	if err := s.SetVersion("u1", fmt.Sprintf(EntityGroupMembersFmt, "g1"), "vid-g1", 3); err != nil {
		t.Fatal(err)
	}
	// owner 隔离
	if err := s.SetVersion("u2", EntityFriends, "vid-other", 9); err != nil {
		t.Fatal(err)
	}

	vID, v, err = s.GetVersion("u1", EntityFriends)
	if err != nil || vID != "vid-1" || v != 5 {
		t.Fatalf("u1 friends: %q %d %v", vID, v, err)
	}
	vID, v, _ = s.GetVersion("u1", fmt.Sprintf(EntityGroupMembersFmt, "g1"))
	if vID != "vid-g1" || v != 3 {
		t.Fatalf("u1 members: %q %d", vID, v)
	}
	vID, v, _ = s.GetVersion("u2", EntityFriends)
	if vID != "vid-other" || v != 9 {
		t.Fatalf("u2 friends: %q %d", vID, v)
	}

	// ListVersions：u1 共 2 条游标，u2 共 1 条
	u1v, err := s.ListVersions("u1")
	if err != nil {
		t.Fatal(err)
	}
	if len(u1v) != 2 || u1v[EntityFriends].VersionID != "vid-1" ||
		u1v[fmt.Sprintf(EntityGroupMembersFmt, "g1")].Version != 3 {
		t.Fatalf("u1 versions: %+v", u1v)
	}
	u2v, _ := s.ListVersions("u2")
	if len(u2v) != 1 || u2v[EntityFriends].Version != 9 {
		t.Fatalf("u2 versions: %+v", u2v)
	}
}

func TestSQLiteStorage_Messages(t *testing.T) {
	s := newSQLiteForTest(t)

	msgs := []*model.MsgData{
		{ClientMsgID: "c1", Seq: 1, ConversationID: "si_a", SendID: "u1", RecvID: "u2", Content: "first", ContentType: 101, CreateTime: 100},
		{ClientMsgID: "c2", Seq: 2, ConversationID: "si_a", SendID: "u2", RecvID: "u1", Content: "second", ContentType: 101, CreateTime: 200},
		{ClientMsgID: "c3", Seq: 3, ConversationID: "si_b", SendID: "u1", RecvID: "u2", Content: "other conv", ContentType: 101, CreateTime: 300},
	}
	if err := s.UpsertMsgs("u1", msgs); err != nil {
		t.Fatal(err)
	}
	// 重复 seq upsert（ClientMsgID 相同）→ 不报错不重复
	if err := s.UpsertMsgs("u1", []*model.MsgData{{ClientMsgID: "c1", Seq: 1, ConversationID: "si_a", Content: "first-edited", ContentType: 101}}); err != nil {
		t.Fatal(err)
	}

	got, err := s.ListMsgsByConversation("u1", "si_a")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].Content != "first-edited" || got[1].Content != "second" {
		t.Fatalf("order/content wrong: %s, %s", got[0].Content, got[1].Content)
	}
	// owner 隔离
	got2, _ := s.ListMsgsByConversation("u2", "si_a")
	if len(got2) != 0 {
		t.Fatalf("u2 should see nothing, got %d", len(got2))
	}
}

func TestSQLiteStorage_Conversations(t *testing.T) {
	s := newSQLiteForTest(t)

	convs := []*model.Conversation{
		{ConversationID: "si_a", ConversationType: 1, LatestMsgSendTime: 100, UnreadCount: 1, MsgInfo: &model.MsgInfo{ClientMsgID: "m1", Content: "hi"}},
		{ConversationID: "si_b", ConversationType: 2, LatestMsgSendTime: 300},
		{ConversationID: "si_c", ConversationType: 1, LatestMsgSendTime: 200},
	}
	if err := s.UpsertConversations("u1", convs); err != nil {
		t.Fatal(err)
	}

	got, err := s.ListConversations("u1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].ConversationID != "si_b" || got[2].ConversationID != "si_a" {
		t.Fatalf("sort wrong: %+v", got)
	}
	if got[2].MsgInfo == nil || got[2].MsgInfo.Content != "hi" {
		t.Fatalf("msg_info lost: %+v", got[2].MsgInfo)
	}

	ids, _ := s.ListConversationIDs("u1")
	if len(ids) != 3 {
		t.Fatalf("ids = %v", ids)
	}

	if err := s.DeleteConversations("u1", []string{"si_a", "si_b"}); err != nil {
		t.Fatal(err)
	}
	got, _ = s.ListConversations("u1")
	if len(got) != 1 || got[0].ConversationID != "si_c" {
		t.Fatalf("after delete: %+v", got)
	}
}

func TestSQLiteStorage_FriendsBlacks(t *testing.T) {
	s := newSQLiteForTest(t)

	err := s.UpsertFriends("u1", []*model.FriendInfo{
		{OwnerUserID: "u1", FriendUserID: "f1", Remark: "one"},
		{OwnerUserID: "u1", FriendUserID: "f2", Remark: "two", IsPinned: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	// update f1
	if err := s.UpsertFriends("u1", []*model.FriendInfo{{OwnerUserID: "u1", FriendUserID: "f1", Remark: "one-x"}}); err != nil {
		t.Fatal(err)
	}

	fs, _ := s.ListFriends("u1")
	if len(fs) != 2 {
		t.Fatalf("friends = %d", len(fs))
	}
	ids, _ := s.ListFriendIDs("u1")
	if len(ids) != 2 {
		t.Fatalf("ids = %v", ids)
	}

	if err := s.UpsertBlacks("u1", []*model.BlackInfo{{OwnerUserID: "u1", BlackUserID: "b1"}}); err != nil {
		t.Fatal(err)
	}
	bs, _ := s.ListBlacks("u1")
	if len(bs) != 1 || bs[0].BlackUserID != "b1" {
		t.Fatalf("blacks = %+v", bs)
	}

	if err := s.DeleteFriends("u1", []string{"f2"}); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteBlacks("u1", []string{"b1"}); err != nil {
		t.Fatal(err)
	}
	fs, _ = s.ListFriends("u1")
	bs, _ = s.ListBlacks("u1")
	if len(fs) != 1 || fs[0].Remark != "one-x" {
		t.Fatalf("after delete friends: %+v", fs)
	}
	if len(bs) != 0 {
		t.Fatalf("after delete blacks: %+v", bs)
	}
}

func TestSQLiteStorage_GroupsMembers(t *testing.T) {
	s := newSQLiteForTest(t)

	if err := s.UpsertGroups("u1", []*model.GroupInfo{
		{GroupID: "g1", GroupName: "one", MemberCount: 2},
		{GroupID: "g2", GroupName: "two"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertGroupMembers("u1", "g1", []*model.GroupMemberFullInfo{
		{GroupID: "g1", UserID: "m1", Nickname: "member-1", RoleLevel: 1, JoinTime: 10},
		{GroupID: "g1", UserID: "m2", Nickname: "member-2", RoleLevel: 3},
	}); err != nil {
		t.Fatal(err)
	}

	ms, _ := s.ListGroupMembers("u1", "g1")
	if len(ms) != 2 {
		t.Fatalf("members = %d", len(ms))
	}
	mids, _ := s.ListGroupMemberIDs("u1", "g1")
	if len(mids) != 2 {
		t.Fatalf("member ids = %v", mids)
	}
	// g2 无成员
	ms, _ = s.ListGroupMembers("u1", "g2")
	if len(ms) != 0 {
		t.Fatalf("g2 members = %d", len(ms))
	}

	// 删群时成员级联清理
	if err := s.DeleteGroups("u1", []string{"g1"}); err != nil {
		t.Fatal(err)
	}
	ms, _ = s.ListGroupMembers("u1", "g1")
	if len(ms) != 0 {
		t.Fatalf("g1 members after group delete = %d", len(ms))
	}
}

func TestSQLiteStorage_ClearOwnerIsolation(t *testing.T) {
	s := newSQLiteForTest(t)

	// 两个 owner 各写数据
	_ = s.SetVersion("u1", EntityFriends, "v1", 1)
	_ = s.SetVersion("u2", EntityFriends, "v2", 2)
	_ = s.UpsertFriends("u1", []*model.FriendInfo{{OwnerUserID: "u1", FriendUserID: "f1"}})
	_ = s.UpsertFriends("u2", []*model.FriendInfo{{OwnerUserID: "u2", FriendUserID: "f2"}})
	_ = s.SetMaxSeqs("u1", map[string]int64{"c": 10}, nil)
	_ = s.SetMaxSeqs("u2", map[string]int64{"c": 20}, nil)

	if err := s.Clear(context.Background(), "u1"); err != nil {
		t.Fatal(err)
	}

	// u1 全空
	max, _, _ := s.GetMaxSeqs("u1")
	fids, _ := s.ListFriendIDs("u1")
	vID, v, _ := s.GetVersion("u1", EntityFriends)
	if max != nil || len(fids) != 0 || vID != "" || v != 0 {
		t.Fatalf("u1 not cleared: max=%v fids=%v ver=%q,%d", max, fids, vID, v)
	}
	// u2 不受影响
	fids, _ = s.ListFriendIDs("u2")
	if len(fids) != 1 || fids[0] != "f2" {
		t.Fatalf("u2 polluted: %v", fids)
	}
	max, _, _ = s.GetMaxSeqs("u2")
	if max["c"] != 20 {
		t.Fatalf("u2 seqs polluted: %v", max)
	}
}

func TestNewSQLiteStorage_FilePath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data", "sdk.db") // 不存在的父目录，验证自动创建
	s, err := NewSQLiteStorage(path, &logger.NopLogger{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer s.Close()

	if err := s.SetVersion("u1", EntityFriends, "v", 1); err != nil {
		t.Fatal(err)
	}
	// 重新打开验证持久化
	s2, err := NewSQLiteStorage(path, &logger.NopLogger{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	vID, v, err := s2.GetVersion("u1", EntityFriends)
	if err != nil || vID != "v" || v != 1 {
		t.Fatalf("after reopen: %q %d %v", vID, v, err)
	}
}

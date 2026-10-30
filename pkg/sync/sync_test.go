package sync

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/storage"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/convert"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/eventbus"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/hash"
)

const testOwner = "u1"

// === mock：MsgSource ===

type mockMsgSource struct {
	newestMax map[string]int64
	newestMin map[string]int64
	newestErr error

	// 每次调用记录的请求范围
	pulls []*model.SeqRange

	// results 非空时按 convID 返回固定结果（优先于自动生成）
	results map[string]*model.PullMsgsResult
	// script 按 PullMsgBySeq 调用次序（1-based）覆盖返回结果（优先于 results）
	script map[int]*model.PullMsgsResult
	// failConvs 按 convID 注入拉取失败：先放行 n 次成功调用，再失败一次，之后恢复
	failConvs map[string]int
	// genFull 为 true 时自动为请求范围 [begin,end] 逐 seq 生成消息
	genFull bool

	callCnt int
}

func (m *mockMsgSource) NewestSeq(ctx context.Context) (map[string]int64, map[string]int64, error) {
	return m.newestMax, m.newestMin, m.newestErr
}

func (m *mockMsgSource) PullMsgBySeq(ctx context.Context, userID string, seqRanges []*model.SeqRange, order int32) (map[string]*model.PullMsgsResult, error) {
	m.pulls = append(m.pulls, seqRanges...)
	m.callCnt++
	if m.callCnt > 50 {
		// 安全阀：游标防御失效时快速失败，避免测试死循环
		return nil, errors.New("mock: too many pulls, cursor guard broken")
	}
	out := make(map[string]*model.PullMsgsResult)
	for _, r := range seqRanges {
		if n, ok := m.failConvs[r.ConversationID]; ok {
			if n > 0 {
				m.failConvs[r.ConversationID] = n - 1
			} else {
				delete(m.failConvs, r.ConversationID)
				return nil, fmt.Errorf("mock: pull %s failed", r.ConversationID)
			}
		}
		if pr, ok := m.script[m.callCnt]; ok {
			out[r.ConversationID] = pr
			continue
		}
		if pr, ok := m.results[r.ConversationID]; ok {
			out[r.ConversationID] = pr
			continue
		}
		if !m.genFull {
			out[r.ConversationID] = &model.PullMsgsResult{}
			continue
		}
		begin := convert.ToInt64(r.Begin)
		end := convert.ToInt64(r.End)
		pr := &model.PullMsgsResult{}
		for i := begin; i <= end; i++ {
			pr.Msgs = append(pr.Msgs, &model.MsgData{
				ConversationID: r.ConversationID,
				Seq:            i,
			})
		}
		out[r.ConversationID] = pr
	}
	return out, nil
}

// === mock：RelSource ===

type mockRelSource struct {
	incFriends  *model.IncrementalFriends
	incBlacks   *model.IncrementalBlacks
	fullFriends *model.FullFriendIDs
	friendList  []*model.FriendInfo
	blackList   []*model.BlackInfo
	// friendListFails 剩余失败次数：>0 时 GetFriendList 返回错误并递减，耗尽后恢复成功
	friendListFails int

	incFriendsCalled  int
	incFriendsVersion []string // 记录 versionID
	incFriendsVerNum  []uint64
	fullFriendsCalled bool
	fullFriendsIDHash []uint64
	listPages         []*model.RequestPagination
}

func (m *mockRelSource) GetIncrementalFriends(ctx context.Context, userID, versionID string, version uint64) (*model.IncrementalFriends, error) {
	m.incFriendsCalled++
	m.incFriendsVersion = append(m.incFriendsVersion, versionID)
	m.incFriendsVerNum = append(m.incFriendsVerNum, version)
	return m.incFriends, nil
}

func (m *mockRelSource) GetIncrementalBlacks(ctx context.Context, userID, versionID string, version uint64) (*model.IncrementalBlacks, error) {
	return m.incBlacks, nil
}

func (m *mockRelSource) GetFullFriendUserIDs(ctx context.Context, userID string, idHash uint64) (*model.FullFriendIDs, error) {
	m.fullFriendsCalled = true
	m.fullFriendsIDHash = append(m.fullFriendsIDHash, idHash)
	return m.fullFriends, nil
}

func (m *mockRelSource) GetFriendList(ctx context.Context, userID string, pagination *model.RequestPagination) (int32, []*model.FriendInfo, error) {
	m.listPages = append(m.listPages, pagination)
	if m.friendListFails > 0 {
		m.friendListFails--
		return 0, nil, errors.New("mock: friend list failed")
	}
	return int32(len(m.friendList)), pageSlice(m.friendList, int(pagination.PageNumber), int(pagination.ShowNumber)), nil
}

func (m *mockRelSource) GetBlackList(ctx context.Context, userID string, pagination *model.RequestPagination) (int32, []*model.BlackInfo, error) {
	m.listPages = append(m.listPages, pagination)
	return int32(len(m.blackList)), pageSlice(m.blackList, int(pagination.PageNumber), int(pagination.ShowNumber)), nil
}

// === mock：GroupSource ===

type mockGroupSource struct {
	incGroups *model.IncrementalGroups
	// incMembers 按 groupID 返回成员增量
	incMembers map[string]*model.IncrementalMembers
	// memberFailTimes 按 groupID 注入成员增量拉取失败（剩余失败次数，耗尽后恢复成功）
	memberFailTimes map[string]int
	fullJoin        *model.FullGroupIDs
	fullMembers     map[string]*model.FullMemberIDs
	joinedList      []*model.GroupInfo
	memberList      map[string][]*model.GroupMemberFullInfo
	memberIncArg    []string // 记录调用 SyncGroupMembers 的 groupID
	fullJoinIDHash  []uint64
}

func (m *mockGroupSource) GetIncrementalJoinGroups(ctx context.Context, userID, versionID string, version uint64) (*model.IncrementalGroups, error) {
	return m.incGroups, nil
}

func (m *mockGroupSource) GetIncrementalGroupMembers(ctx context.Context, groupID, versionID string, version uint64) (*model.IncrementalMembers, error) {
	m.memberIncArg = append(m.memberIncArg, groupID)
	if n, ok := m.memberFailTimes[groupID]; ok && n > 0 {
		m.memberFailTimes[groupID] = n - 1
		return nil, errors.New("mock: member inc failed")
	}
	if inc, ok := m.incMembers[groupID]; ok {
		return inc, nil
	}
	return &model.IncrementalMembers{VersionID: "mv0", Version: 1}, nil
}

func (m *mockGroupSource) GetFullGroupMemberUserIDs(ctx context.Context, groupID string, idHash uint64) (*model.FullMemberIDs, error) {
	if fm, ok := m.fullMembers[groupID]; ok {
		return fm, nil
	}
	return &model.FullMemberIDs{VersionID: "fmv0", Version: 1, Equal: true}, nil
}

func (m *mockGroupSource) GetFullJoinGroupIDs(ctx context.Context, userID string, idHash uint64) (*model.FullGroupIDs, error) {
	m.fullJoinIDHash = append(m.fullJoinIDHash, idHash)
	return m.fullJoin, nil
}

func (m *mockGroupSource) GetJoinedGroupList(ctx context.Context, userID string, pagination *model.RequestPagination) (int32, []*model.GroupInfo, error) {
	return int32(len(m.joinedList)), pageSlice(m.joinedList, int(pagination.PageNumber), int(pagination.ShowNumber)), nil
}

func (m *mockGroupSource) GetGroupMemberList(ctx context.Context, pagination *model.RequestPagination, groupID string, filter int32, keyword string) (uint32, []*model.GroupMemberFullInfo, error) {
	return uint32(len(m.memberList[groupID])), pageSlice(m.memberList[groupID], int(pagination.PageNumber), int(pagination.ShowNumber)), nil
}

// === mock：ConvSource ===

type mockConvSource struct {
	incConvs *model.IncrementalConversations
	fullConv *model.FullConversationIDs
	allConvs []*model.Conversation
}

func (m *mockConvSource) GetIncrementalConversations(ctx context.Context, userID string, versionID string, version uint64) (*model.IncrementalConversations, error) {
	return m.incConvs, nil
}

func (m *mockConvSource) GetFullConversationIDs(ctx context.Context, userID string, idHash uint64) (*model.FullConversationIDs, error) {
	return m.fullConv, nil
}

func (m *mockConvSource) GetAllConversations(ctx context.Context, ownerUserID string) ([]*model.Conversation, error) {
	return m.allConvs, nil
}

// === helpers ===

func pageSlice[T any](items []T, pageNumber, showNumber int) []T {
	start := (pageNumber - 1) * showNumber
	if start >= len(items) {
		return nil
	}
	end := start + showNumber
	if end > len(items) {
		end = len(items)
	}
	return items[start:end]
}

// drainEvents 非阻塞读取 channel 中已有事件数量
func drainEvents(ch <-chan interface{}, max int) int {
	count := 0
	for i := 0; i < max; i++ {
		select {
		case <-ch:
			count++
		default:
			return count
		}
	}
	return count
}

func newTestBus(t *testing.T, types ...events.EventType) (*eventbus.EventBus, []<-chan interface{}) {
	t.Helper()
	bus := eventbus.New(eventbus.WithLogger(&logger.NopLogger{}))
	var chans []<-chan interface{}
	for _, et := range types {
		ch, _ := bus.Subscribe(et)
		chans = append(chans, ch)
	}
	return bus, chans
}

// TestEntitySync_FriendsFullFetchFailureKeepsOldData 全量回退拉取失败：本地旧数据与游标原封不动
//
// fetch-then-swap 保证：全量数据收集完成才清表换新。拉取中途失败（网络断）时
// 本地好友列表不会出现空窗（感知丢失），下次同步基于完整旧数据重走全量流程。
func TestEntitySync_FriendsFullFetchFailureKeepsOldData(t *testing.T) {
	rel := &mockRelSource{
		incFriends: &model.IncrementalFriends{VersionID: "fv2", Version: 5, Full: true},
		fullFriends: &model.FullFriendIDs{VersionID: "fv2", Version: 5, Equal: false,
			UserIDs: []string{"f_new"}},
		friendList:      []*model.FriendInfo{{FriendUserID: "f_new"}},
		friendListFails: 1, // 全量拉取首 页即失败
	}
	stor := storage.NewMemoryStorage()
	bus, chans := newTestBus(t, events.EventFriendListSynced)
	engine := NewEntitySyncEngine(rel, &mockGroupSource{}, &mockConvSource{}, stor, testOwner, bus, &logger.NopLogger{})

	// 预置旧数据 + 旧游标
	if err := stor.UpsertFriends(testOwner, []*model.FriendInfo{{FriendUserID: "f_old"}}); err != nil {
		t.Fatal(err)
	}
	if err := stor.SetVersion(testOwner, storage.EntityFriends, "fv1", 1); err != nil {
		t.Fatal(err)
	}

	// 第一次：full → not-equal → 拉取失败
	if err := engine.SyncFriends(context.Background()); err == nil {
		t.Fatal("SyncFriends should fail on full fetch error")
	}
	// fetch-then-swap：旧数据与游标不受影响（先清表就会在这里出现空列表）
	fs, _ := stor.ListFriends(testOwner)
	if len(fs) != 1 || fs[0].FriendUserID != "f_old" {
		t.Fatalf("friends after failed full resync = %+v, want [f_old]", fs)
	}
	vID, ver, _ := stor.GetVersion(testOwner, storage.EntityFriends)
	if vID != "fv1" || ver != 1 {
		t.Errorf("version = %s/%d, want fv1/1（拉取失败不应动游标）", vID, ver)
	}
	if got := drainEvents(chans[0], 5); got != 0 {
		t.Errorf("EventFriendListSynced on failure = %d, want 0", got)
	}

	// 第二次：拉取恢复 → 收集完成后换表
	rel.friendListFails = 0
	if err := engine.SyncFriends(context.Background()); err != nil {
		t.Fatalf("SyncFriends #2: %v", err)
	}
	fs, _ = stor.ListFriends(testOwner)
	if len(fs) != 1 || fs[0].FriendUserID != "f_new" {
		t.Fatalf("friends after recovery = %+v, want [f_new]", fs)
	}
	vID, ver, _ = stor.GetVersion(testOwner, storage.EntityFriends)
	if vID != "fv2" || ver != 5 {
		t.Errorf("version = %s/%d, want fv2/5", vID, ver)
	}
	if got := drainEvents(chans[0], 5); got != 1 {
		t.Errorf("EventFriendListSynced after recovery = %d, want 1", got)
	}
}

// === SeqSyncEngine 测试 ===

// TestSeqSync_GapPull 两会话缺口分批拉取：游标推进、消息入库、事件发布
func TestSeqSync_GapPull(t *testing.T) {
	msg := &mockMsgSource{
		newestMax: map[string]int64{"conv1": 250, "conv2": 50},
		newestMin: map[string]int64{"conv1": 1, "conv2": 1},
		genFull:   true,
	}
	stor := storage.NewMemoryStorage()
	bus, chans := newTestBus(t, events.EventSeqSynced)

	engine := NewSeqSyncEngine(msg, stor, testOwner, bus, &logger.NopLogger{})
	if err := engine.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	// conv1: [1,100] [101,200] [201,250]；conv2: [1,50]
	if len(msg.pulls) != 4 {
		t.Fatalf("pull calls = %d, want 4", len(msg.pulls))
	}
	conv1Ranges := 0
	conv2Ranges := 0
	for _, r := range msg.pulls {
		switch r.ConversationID {
		case "conv1":
			conv1Ranges++
		case "conv2":
			conv2Ranges++
		}
		if r.Order != model.PullOrderAsc {
			t.Errorf("order = %d, want %d", r.Order, model.PullOrderAsc)
		}
	}
	if conv1Ranges != 3 || conv2Ranges != 1 {
		t.Errorf("ranges conv1=%d conv2=%d, want 3/1", conv1Ranges, conv2Ranges)
	}

	// 消息入库
	msgs, err := stor.ListMsgsByConversation(testOwner, "conv1")
	if err != nil {
		t.Fatalf("ListMsgsByConversation: %v", err)
	}
	if len(msgs) != 250 {
		t.Fatalf("conv1 msgs = %d, want 250", len(msgs))
	}
	if msgs[0].Seq != 1 || msgs[len(msgs)-1].Seq != 250 {
		t.Errorf("first/last seq = %d/%d, want 1/250", msgs[0].Seq, msgs[len(msgs)-1].Seq)
	}

	// 游标前移
	maxSeqs, minSeqs, err := stor.GetMaxSeqs(testOwner)
	if err != nil {
		t.Fatalf("GetMaxSeqs: %v", err)
	}
	if maxSeqs["conv1"] != 250 || maxSeqs["conv2"] != 50 {
		t.Errorf("maxSeqs = %v, want conv1:250 conv2:50", maxSeqs)
	}
	if minSeqs["conv1"] != 1 || minSeqs["conv2"] != 1 {
		t.Errorf("minSeqs = %v, want conv1:1 conv2:1", minSeqs)
	}

	// 事件
	if got := drainEvents(chans[0], 5); got != 1 {
		t.Errorf("EventSeqSynced count = %d, want 1", got)
	}
}

// TestSeqSync_UpToDate 本地游标已追平：不发拉取请求，但事件照常发布
func TestSeqSync_UpToDate(t *testing.T) {
	msg := &mockMsgSource{
		newestMax: map[string]int64{"conv1": 250},
		newestMin: map[string]int64{"conv1": 1},
	}
	stor := storage.NewMemoryStorage()
	if err := stor.SetMaxSeqs(testOwner, map[string]int64{"conv1": 250}, map[string]int64{"conv1": 1}); err != nil {
		t.Fatalf("SetMaxSeqs: %v", err)
	}
	bus, chans := newTestBus(t, events.EventSeqSynced)

	engine := NewSeqSyncEngine(msg, stor, testOwner, bus, &logger.NopLogger{})
	if err := engine.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if len(msg.pulls) != 0 {
		t.Errorf("pull calls = %d, want 0", len(msg.pulls))
	}
	if got := drainEvents(chans[0], 5); got != 1 {
		t.Errorf("EventSeqSynced count = %d, want 1", got)
	}
}

// TestSeqSync_EmptyRange 服务端范围内无消息（seq 空洞）：不出错，游标不前进
func TestSeqSync_EmptyRange(t *testing.T) {
	msg := &mockMsgSource{
		newestMax: map[string]int64{"conv1": 10},
		newestMin: map[string]int64{"conv1": 1},
		results:   map[string]*model.PullMsgsResult{"conv1": {}},
	}
	stor := storage.NewMemoryStorage()
	bus, _ := newTestBus(t, events.EventSeqSynced)

	engine := NewSeqSyncEngine(msg, stor, testOwner, bus, &logger.NopLogger{})
	if err := engine.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	msgs, err := stor.ListMsgsByConversation(testOwner, "conv1")
	if err != nil {
		t.Fatalf("ListMsgsByConversation: %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("msgs = %d, want 0", len(msgs))
	}
	maxSeqs, _, err := stor.GetMaxSeqs(testOwner)
	if err != nil {
		t.Fatalf("GetMaxSeqs: %v", err)
	}
	if maxSeqs["conv1"] != 0 {
		t.Errorf("maxSeq conv1 = %d, want 0（无消息时游标不前进，下次重拉）", maxSeqs["conv1"])
	}
}

// TestSeqSync_NonAdvancingCursorGuard 游标停滞防御：后续批次返回滞后 seq 时跳批不死循环
func TestSeqSync_NonAdvancingCursorGuard(t *testing.T) {
	msg := &mockMsgSource{
		newestMax: map[string]int64{"conv1": 250},
		newestMin: map[string]int64{"conv1": 1},
		genFull:   true,
		// 第 2 批 [101,200] 返回滞后 seq=100（游标停滞），其余批次正常
		script: map[int]*model.PullMsgsResult{
			2: {Msgs: []*model.MsgData{{ConversationID: "conv1", Seq: 100}}},
		},
	}
	stor := storage.NewMemoryStorage()
	bus, chans := newTestBus(t, events.EventSeqSynced)

	engine := NewSeqSyncEngine(msg, stor, testOwner, bus, &logger.NopLogger{})
	if err := engine.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v（mock 50 次安全阀触发 = 防御未生效死循环）", err)
	}

	// 第 1 批 [1,100] 正常 → 第 2 批停滞跳到 end=200 → 第 3 批 [201,250] 正常
	if msg.callCnt != 3 {
		t.Errorf("pull calls = %d, want 3", msg.callCnt)
	}
	if last := msg.pulls[len(msg.pulls)-1]; last.Begin != "201" || last.End != "250" {
		t.Errorf("last range = [%s,%s], want [201,250]", last.Begin, last.End)
	}
	maxSeqs, _, err := stor.GetMaxSeqs(testOwner)
	if err != nil {
		t.Fatalf("GetMaxSeqs: %v", err)
	}
	if maxSeqs["conv1"] != 250 {
		t.Errorf("maxSeq conv1 = %d, want 250", maxSeqs["conv1"])
	}
	select {
	case <-chans[0]:
	default:
		t.Error("EventSeqSynced not published")
	}
}

// TestSeqSync_ConversationFailureIsolation 会话隔离：单会话失败不中断其余会话，
// 已成功会话游标持久化，重试只拉失败会话
func TestSeqSync_ConversationFailureIsolation(t *testing.T) {
	msg := &mockMsgSource{
		newestMax: map[string]int64{"convA": 200, "convB": 150, "convC": 50},
		newestMin: map[string]int64{"convA": 1, "convB": 1, "convC": 1},
		genFull:   true,
		// convB 首批拉取失败（放行 0 次），之后恢复
		failConvs: map[string]int{"convB": 0},
	}
	stor := storage.NewMemoryStorage()
	bus, chans := newTestBus(t, events.EventSeqSynced)
	engine := NewSeqSyncEngine(msg, stor, testOwner, bus, &logger.NopLogger{})

	// 第一次：convB 失败，convA/convC 正常完成
	err := engine.Sync(context.Background())
	if err == nil || !strings.Contains(err.Error(), "convB") {
		t.Fatalf("Sync should fail with convB error, got %v", err)
	}

	// 已成功会话游标已持久化（重试不重拉）；失败会话无游标
	maxSeqs, _, err := stor.GetMaxSeqs(testOwner)
	if err != nil {
		t.Fatalf("GetMaxSeqs: %v", err)
	}
	if maxSeqs["convA"] != 200 || maxSeqs["convC"] != 50 {
		t.Errorf("cursors convA=%d convC=%d, want 200/50（应已落盘）", maxSeqs["convA"], maxSeqs["convC"])
	}
	if maxSeqs["convB"] != 0 {
		t.Errorf("convB cursor = %d, want 0", maxSeqs["convB"])
	}
	// 失败会话恰好拉了 1 批（首批失败即停）
	for _, r := range msg.pulls {
		if r.ConversationID == "convB" {
			if r.Begin != "1" || r.End != "100" {
				t.Errorf("convB range = [%s,%s], want [1,100]", r.Begin, r.End)
			}
		}
	}
	// 失败时无就绪事件
	if got := drainEvents(chans[0], 5); got != 0 {
		t.Errorf("EventSeqSynced on failure = %d, want 0", got)
	}

	// 第二次：convB 恢复，只拉 convB（convA/convC 无 gap 不发请求）
	if err := engine.Sync(context.Background()); err != nil {
		t.Fatalf("Sync #2: %v", err)
	}
	convACalls, convBCalls, convCCalls := 0, 0, 0
	for _, r := range msg.pulls {
		switch r.ConversationID {
		case "convA":
			convACalls++
		case "convB":
			convBCalls++
		case "convC":
			convCCalls++
		}
	}
	if convACalls != 2 || convCCalls != 1 {
		t.Errorf("convA=%d convC=%d pulls, want 2/1（成功会话不应重拉）", convACalls, convCCalls)
	}
	if convBCalls != 3 {
		t.Errorf("convB pulls = %d, want 3（首败 1 + 重试 2）", convBCalls)
	}
	maxSeqs, _, _ = stor.GetMaxSeqs(testOwner)
	if maxSeqs["convB"] != 150 {
		t.Errorf("convB cursor = %d, want 150", maxSeqs["convB"])
	}
	if got := drainEvents(chans[0], 5); got != 1 {
		t.Errorf("EventSeqSynced after retry = %d, want 1", got)
	}
}

// TestSeqSync_ResumeAfterBatchFailure 批内恢复：会话中途失败，游标停在已收批次，
// 重试从下一批续拉（不重复已完成批次）
func TestSeqSync_ResumeAfterBatchFailure(t *testing.T) {
	msg := &mockMsgSource{
		newestMax: map[string]int64{"convX": 250},
		newestMin: map[string]int64{"convX": 1},
		genFull:   true,
		// 第 1 批放行，第 2 批失败，之后恢复
		failConvs: map[string]int{"convX": 1},
	}
	stor := storage.NewMemoryStorage()
	bus, chans := newTestBus(t, events.EventSeqSynced)
	engine := NewSeqSyncEngine(msg, stor, testOwner, bus, &logger.NopLogger{})

	// 第一次：第 2 批失败（第 1 批已入库、游标已落盘）
	if err := engine.Sync(context.Background()); err == nil {
		t.Fatal("Sync #1 should fail")
	}
	maxSeqs, _, err := stor.GetMaxSeqs(testOwner)
	if err != nil {
		t.Fatalf("GetMaxSeqs: %v", err)
	}
	if maxSeqs["convX"] != 100 {
		t.Fatalf("convX cursor after fail = %d, want 100（第 1 批进度应已落盘）", maxSeqs["convX"])
	}
	pullsAfterFail := len(msg.pulls)

	// 第二次：从第 2 批续拉
	msg.failConvs = nil
	if err := engine.Sync(context.Background()); err != nil {
		t.Fatalf("Sync #2: %v", err)
	}
	if len(msg.pulls) <= pullsAfterFail {
		t.Fatal("expected retry pulls")
	}
	first := msg.pulls[pullsAfterFail]
	if first.Begin != "101" || first.End != "200" {
		t.Errorf("retry first range = [%s,%s], want [101,200]（应从第 2 批续拉）", first.Begin, first.End)
	}
	maxSeqs, _, _ = stor.GetMaxSeqs(testOwner)
	if maxSeqs["convX"] != 250 {
		t.Errorf("convX cursor = %d, want 250", maxSeqs["convX"])
	}
	msgs, _ := stor.ListMsgsByConversation(testOwner, "convX")
	if len(msgs) != 250 {
		t.Errorf("convX msgs = %d, want 250", len(msgs))
	}
	if got := drainEvents(chans[0], 5); got != 1 {
		t.Errorf("EventSeqSynced = %d, want 1", got)
	}
}

// === EntitySyncEngine 测试 ===

// TestEntitySync_FriendsIncremental 增量应用：insert/update/delete + 游标写回 + 事件
func TestEntitySync_FriendsIncremental(t *testing.T) {
	rel := &mockRelSource{
		incFriends: &model.IncrementalFriends{
			VersionID: "v1", Version: 2,
			Insert: []model.FriendInfo{{FriendUserID: "f1"}, {FriendUserID: "f2"}},
			Update: []model.FriendInfo{{FriendUserID: "f3", Nickname: "newName"}},
			Delete: []string{"f9"},
		},
	}
	stor := storage.NewMemoryStorage()
	if err := stor.SetVersion(testOwner, storage.EntityFriends, "v0", 1); err != nil {
		t.Fatalf("seed version: %v", err)
	}
	if err := stor.UpsertFriends(testOwner, []*model.FriendInfo{
		{FriendUserID: "f3", Nickname: "oldName"},
		{FriendUserID: "f9"},
	}); err != nil {
		t.Fatalf("seed friends: %v", err)
	}
	bus, chans := newTestBus(t, events.EventFriendListSynced)

	engine := NewEntitySyncEngine(rel, &mockGroupSource{}, &mockConvSource{}, stor, testOwner, bus, &logger.NopLogger{})
	if err := engine.SyncFriends(context.Background()); err != nil {
		t.Fatalf("SyncFriends: %v", err)
	}

	// 上次游标作为请求参数
	if rel.incFriendsCalled != 1 || rel.incFriendsVersion[0] != "v0" || rel.incFriendsVerNum[0] != 1 {
		t.Errorf("inc args = %s/%d, want v0/1", rel.incFriendsVersion[0], rel.incFriendsVerNum[0])
	}

	// 存储：f1/f2 新增，f3 更新，f9 删除
	fs, err := stor.ListFriends(testOwner)
	if err != nil {
		t.Fatalf("ListFriends: %v", err)
	}
	byID := map[string]*model.FriendInfo{}
	for _, f := range fs {
		byID[f.FriendUserID] = f
	}
	if len(fs) != 3 {
		t.Fatalf("friends = %d, want 3", len(fs))
	}
	if byID["f1"] == nil || byID["f2"] == nil || byID["f9"] != nil {
		t.Errorf("insert/delete not applied: %v", byID)
	}
	if byID["f3"] == nil || byID["f3"].Nickname != "newName" {
		t.Errorf("update not applied: %+v", byID["f3"])
	}

	// 游标
	vID, ver, err := stor.GetVersion(testOwner, storage.EntityFriends)
	if err != nil || vID != "v1" || ver != 2 {
		t.Errorf("version = %s/%d, want v1/2 (err=%v)", vID, ver, err)
	}

	if got := drainEvents(chans[0], 5); got != 1 {
		t.Errorf("EventFriendListSynced count = %d, want 1", got)
	}
}

// TestEntitySync_FriendsFullEqual full=true + IDHash 一致：仅对齐游标，不清表
func TestEntitySync_FriendsFullEqual(t *testing.T) {
	rel := &mockRelSource{
		incFriends: &model.IncrementalFriends{VersionID: "v1", Version: 2, Full: true},
		fullFriends: &model.FullFriendIDs{
			VersionID: "fv", Version: 9, Equal: true, UserIDs: []string{"f1", "f2"},
		},
	}
	stor := storage.NewMemoryStorage()
	if err := stor.SetVersion(testOwner, storage.EntityFriends, "v0", 1); err != nil {
		t.Fatalf("seed version: %v", err)
	}
	if err := stor.UpsertFriends(testOwner, []*model.FriendInfo{
		{FriendUserID: "f1"}, {FriendUserID: "f2"},
	}); err != nil {
		t.Fatalf("seed friends: %v", err)
	}
	bus, chans := newTestBus(t, events.EventFriendListSynced)

	engine := NewEntitySyncEngine(rel, &mockGroupSource{}, &mockConvSource{}, stor, testOwner, bus, &logger.NopLogger{})
	if err := engine.SyncFriends(context.Background()); err != nil {
		t.Fatalf("SyncFriends: %v", err)
	}

	// IDHash 参数正确（order-invariant）
	wantHash := hash.HashStringSet([]string{"f1", "f2"})
	if !rel.fullFriendsCalled || rel.fullFriendsIDHash[0] != wantHash {
		t.Errorf("full idHash = %d, want %d", rel.fullFriendsIDHash[0], wantHash)
	}

	// 本地数据未动
	fs, _ := stor.ListFriends(testOwner)
	if len(fs) != 2 {
		t.Errorf("friends = %d, want 2", len(fs))
	}
	// 游标对齐 full 响应
	vID, ver, _ := stor.GetVersion(testOwner, storage.EntityFriends)
	if vID != "fv" || ver != 9 {
		t.Errorf("version = %s/%d, want fv/9", vID, ver)
	}
	if got := drainEvents(chans[0], 5); got != 1 {
		t.Errorf("EventFriendListSynced count = %d, want 1", got)
	}
}

// TestEntitySync_FriendsFullNotEqual full=true + IDHash 不一致：清表 + 分页全量拉取
func TestEntitySync_FriendsFullNotEqual(t *testing.T) {
	rel := &mockRelSource{
		incFriends: &model.IncrementalFriends{VersionID: "v1", Version: 2, Full: true},
		fullFriends: &model.FullFriendIDs{
			VersionID: "fv", Version: 9, Equal: false, UserIDs: []string{"f3", "f4", "f5"},
		},
		friendList: []*model.FriendInfo{
			{FriendUserID: "f3"}, {FriendUserID: "f4"}, {FriendUserID: "f5"},
		},
	}
	stor := storage.NewMemoryStorage()
	if err := stor.SetVersion(testOwner, storage.EntityFriends, "v0", 1); err != nil {
		t.Fatalf("seed version: %v", err)
	}
	if err := stor.UpsertFriends(testOwner, []*model.FriendInfo{
		{FriendUserID: "f1"}, {FriendUserID: "f2"},
	}); err != nil {
		t.Fatalf("seed friends: %v", err)
	}
	bus, chans := newTestBus(t, events.EventFriendListSynced)

	engine := NewEntitySyncEngine(rel, &mockGroupSource{}, &mockConvSource{}, stor, testOwner, bus, &logger.NopLogger{})
	if err := engine.SyncFriends(context.Background()); err != nil {
		t.Fatalf("SyncFriends: %v", err)
	}

	// 旧数据清除，新数据全量入库
	fs, _ := stor.ListFriends(testOwner)
	if len(fs) != 3 {
		t.Fatalf("friends = %d, want 3", len(fs))
	}
	for _, id := range []string{"f3", "f4", "f5"} {
		found := false
		for _, f := range fs {
			if f.FriendUserID == id {
				found = true
			}
		}
		if !found {
			t.Errorf("friend %s missing", id)
		}
	}
	for _, f := range fs {
		if f.FriendUserID == "f1" || f.FriendUserID == "f2" {
			t.Errorf("stale friend %s not cleared", f.FriendUserID)
		}
	}
	// 游标
	vID, ver, _ := stor.GetVersion(testOwner, storage.EntityFriends)
	if vID != "fv" || ver != 9 {
		t.Errorf("version = %s/%d, want fv/9", vID, ver)
	}
	if got := drainEvents(chans[0], 5); got != 1 {
		t.Errorf("EventFriendListSynced count = %d, want 1", got)
	}
}

// TestEntitySync_BlacksFull 黑名单 full=true：直接清表重拉（无 IDHash 校验接口）
func TestEntitySync_BlacksFull(t *testing.T) {
	rel := &mockRelSource{
		incBlacks: &model.IncrementalBlacks{VersionID: "bv", Version: 3, Full: true},
		blackList: []*model.BlackInfo{{BlackUserID: "b2"}},
	}
	stor := storage.NewMemoryStorage()
	if err := stor.UpsertBlacks(testOwner, []*model.BlackInfo{{BlackUserID: "b1"}}); err != nil {
		t.Fatalf("seed blacks: %v", err)
	}
	bus, chans := newTestBus(t, events.EventBlackListSynced)

	engine := NewEntitySyncEngine(rel, &mockGroupSource{}, &mockConvSource{}, stor, testOwner, bus, &logger.NopLogger{})
	if err := engine.SyncBlacks(context.Background()); err != nil {
		t.Fatalf("SyncBlacks: %v", err)
	}

	bs, _ := stor.ListBlacks(testOwner)
	if len(bs) != 1 || bs[0].BlackUserID != "b2" {
		t.Fatalf("blacks = %v, want [b2]", bs)
	}
	vID, ver, _ := stor.GetVersion(testOwner, storage.EntityBlacks)
	if vID != "bv" || ver != 3 {
		t.Errorf("version = %s/%d, want bv/3", vID, ver)
	}
	if got := drainEvents(chans[0], 5); got != 1 {
		t.Errorf("EventBlackListSynced count = %d, want 1", got)
	}
}

// TestEntitySync_GroupsIncrementalWithMembers 群增量 insert → 成员增量联动 + 双事件
func TestEntitySync_GroupsIncrementalWithMembers(t *testing.T) {
	group := &mockGroupSource{
		incGroups: &model.IncrementalGroups{
			VersionID: "gv1", Version: 2,
			Insert: []model.GroupInfo{{GroupID: "g1", GroupName: "test"}},
		},
		incMembers: map[string]*model.IncrementalMembers{
			"g1": {
				VersionID: "mv1", Version: 2, GroupID: "g1",
				Insert: []model.GroupMemberFullInfo{{GroupID: "g1", UserID: "m1"}, {GroupID: "g1", UserID: "m2"}},
			},
		},
	}
	stor := storage.NewMemoryStorage()
	bus, chans := newTestBus(t, events.EventGroupListSynced, events.EventGroupMemberSynced)

	engine := NewEntitySyncEngine(&mockRelSource{}, group, &mockConvSource{}, stor, testOwner, bus, &logger.NopLogger{})
	if err := engine.SyncGroups(context.Background()); err != nil {
		t.Fatalf("SyncGroups: %v", err)
	}

	// 群入库
	gs, _ := stor.ListGroups(testOwner)
	if len(gs) != 1 || gs[0].GroupID != "g1" {
		t.Fatalf("groups = %v, want [g1]", gs)
	}
	// 成员联动拉取
	if len(group.memberIncArg) != 1 || group.memberIncArg[0] != "g1" {
		t.Errorf("member sync groupIDs = %v, want [g1]", group.memberIncArg)
	}
	ms, _ := stor.ListGroupMembers(testOwner, "g1")
	if len(ms) != 2 {
		t.Fatalf("members = %d, want 2", len(ms))
	}
	// 双游标
	vID, ver, _ := stor.GetVersion(testOwner, storage.EntityJoinGroups)
	if vID != "gv1" || ver != 2 {
		t.Errorf("joinGroups version = %s/%d, want gv1/2", vID, ver)
	}
	vID, ver, _ = stor.GetVersion(testOwner, "groupMembers:g1")
	if vID != "mv1" || ver != 2 {
		t.Errorf("groupMembers:g1 version = %s/%d, want mv1/2", vID, ver)
	}
	// 双事件
	if got := drainEvents(chans[0], 5); got != 1 {
		t.Errorf("EventGroupListSynced count = %d, want 1", got)
	}
	if got := drainEvents(chans[1], 5); got != 1 {
		t.Errorf("EventGroupMemberSynced count = %d, want 1", got)
	}
}

// TestEntitySync_GroupMembersGapHeal 成员同步失败后，下次同步经游标补偿恢复
//
// 场景：群信息入库成功、群游标已推进，但 g2 成员同步失败 —— 该群不会再出现在
// 后续增量响应中。下次 SyncGroups 靠 reconcileGroupMembers 按游标缺失补同步。
func TestEntitySync_GroupMembersGapHeal(t *testing.T) {
	group := &mockGroupSource{
		incGroups: &model.IncrementalGroups{
			VersionID: "gv1", Version: 2,
			Insert: []model.GroupInfo{{GroupID: "g1"}, {GroupID: "g2"}},
		},
		incMembers: map[string]*model.IncrementalMembers{
			"g1": {VersionID: "mv1", Version: 1, GroupID: "g1",
				Insert: []model.GroupMemberFullInfo{{UserID: "m1"}}},
			"g2": {VersionID: "mv1", Version: 1, GroupID: "g2",
				Insert: []model.GroupMemberFullInfo{{UserID: "m2"}}},
		},
		// g2 首次成员拉取失败，之后恢复
		memberFailTimes: map[string]int{"g2": 1},
	}
	stor := storage.NewMemoryStorage()
	bus, _ := newTestBus(t)
	engine := NewEntitySyncEngine(&mockRelSource{}, group, &mockConvSource{}, stor, testOwner, bus, &logger.NopLogger{})

	// 第一次：群信息入库、g1 成员成功、g2 成员失败 → 返回错误
	if err := engine.SyncGroups(context.Background()); err == nil {
		t.Fatal("SyncGroups #1 should fail on g2 member sync")
	}
	if _, _, err := stor.GetVersion(testOwner, fmt.Sprintf(storage.EntityGroupMembersFmt, "g2")); err != nil {
		t.Fatalf("GetVersion: %v", err)
	} // 游标缺失即 ("",0,nil)，不校验值
	m2, _ := stor.ListGroupMembers(testOwner, "g2")
	if len(m2) != 0 {
		t.Fatalf("g2 members after failed sync = %d, want 0", len(m2))
	}

	// 第二次：增量返回空变更（群游标已推进），reconcile 补同步 g2
	group.incGroups = &model.IncrementalGroups{VersionID: "gv1", Version: 2}
	if err := engine.SyncGroups(context.Background()); err != nil {
		t.Fatalf("SyncGroups #2: %v", err)
	}

	// g2 成员补齐、游标落盘
	m2, _ = stor.ListGroupMembers(testOwner, "g2")
	if len(m2) != 1 || m2[0].UserID != "m2" {
		t.Fatalf("g2 members after heal = %+v, want [m2]", m2)
	}
	vID, ver, _ := stor.GetVersion(testOwner, fmt.Sprintf(storage.EntityGroupMembersFmt, "g2"))
	if vID != "mv1" || ver != 1 {
		t.Errorf("groupMembers:g2 version = %s/%d, want mv1/1", vID, ver)
	}
	// g2 恰好补同步一次（首次失败 + 补偿成功 = 2 次调用）
	g2Calls := 0
	for _, gid := range group.memberIncArg {
		if gid == "g2" {
			g2Calls++
		}
	}
	if g2Calls != 2 {
		t.Errorf("g2 member sync calls = %d, want 2", g2Calls)
	}
}

// TestEntitySync_GroupMemberUpdateRetry Update 群成员同步失败：群游标不推进，下次重试
//
// 区别于 Insert 群（游标缺失 → reconcile 兜底），Update 群的成员游标已存在（旧版本），
// reconcile 无法识别 —— 依赖 commit-last：成员失败时群游标不推进，
// 下次同步重拉同批群增量，afterSync 重试成员同步。
func TestEntitySync_GroupMemberUpdateRetry(t *testing.T) {
	group := &mockGroupSource{
		incGroups: &model.IncrementalGroups{
			VersionID: "gv2", Version: 3,
			Update: []model.GroupInfo{{GroupID: "g1", GroupName: "renamed"}},
		},
		incMembers: map[string]*model.IncrementalMembers{
			"g1": {VersionID: "mv2", Version: 2, GroupID: "g1",
				Insert: []model.GroupMemberFullInfo{{UserID: "m1"}}},
		},
		memberFailTimes: map[string]int{"g1": 1},
	}
	stor := storage.NewMemoryStorage()
	bus, _ := newTestBus(t)
	engine := NewEntitySyncEngine(&mockRelSource{}, group, &mockConvSource{}, stor, testOwner, bus, &logger.NopLogger{})

	// 第一次：群信息入库、成员同步失败 → 返回错误
	if err := engine.SyncGroups(context.Background()); err == nil {
		t.Fatal("SyncGroups #1 should fail on g1 member sync")
	}
	// commit-last：群游标未推进
	vID, ver, _ := stor.GetVersion(testOwner, storage.EntityJoinGroups)
	if vID != "" || ver != 0 {
		t.Fatalf("joinGroups version after fail = %s/%d, want empty（游标不应推进）", vID, ver)
	}

	// 第二次：同样的群增量（游标未推进，服务端会重发）→ 成员重试成功
	if err := engine.SyncGroups(context.Background()); err != nil {
		t.Fatalf("SyncGroups #2: %v", err)
	}
	m1, _ := stor.ListGroupMembers(testOwner, "g1")
	if len(m1) != 1 || m1[0].UserID != "m1" {
		t.Fatalf("g1 members = %+v, want [m1]", m1)
	}
	mvID, mv, _ := stor.GetVersion(testOwner, fmt.Sprintf(storage.EntityGroupMembersFmt, "g1"))
	if mvID != "mv2" || mv != 2 {
		t.Errorf("groupMembers:g1 = %s/%d, want mv2/2", mvID, mv)
	}
	vID, ver, _ = stor.GetVersion(testOwner, storage.EntityJoinGroups)
	if vID != "gv2" || ver != 3 {
		t.Errorf("joinGroups version = %s/%d, want gv2/3", vID, ver)
	}
	if len(group.memberIncArg) != 2 {
		t.Errorf("g1 member sync calls = %d, want 2（首败 + 重试）", len(group.memberIncArg))
	}
}

// TestEntitySync_ConversationsFullNotEqual 会话 full=true + 不一致：清表 + GetAllConversations 重拉
func TestEntitySync_ConversationsFullNotEqual(t *testing.T) {
	conv := &mockConvSource{
		incConvs: &model.IncrementalConversations{VersionID: "cv0", Version: 1, Full: true},
		fullConv: &model.FullConversationIDs{VersionID: "cv1", Version: 5, Equal: false},
		allConvs: []*model.Conversation{
			{ConversationID: "c2", LatestMsgSendTime: 200},
			{ConversationID: "c3", LatestMsgSendTime: 100},
		},
	}
	stor := storage.NewMemoryStorage()
	if err := stor.SetVersion(testOwner, storage.EntityConversations, "cv0", 1); err != nil {
		t.Fatalf("seed version: %v", err)
	}
	if err := stor.UpsertConversations(testOwner, []*model.Conversation{{ConversationID: "c1"}}); err != nil {
		t.Fatalf("seed convs: %v", err)
	}
	bus, chans := newTestBus(t, events.EventConversationSynced)

	engine := NewEntitySyncEngine(&mockRelSource{}, &mockGroupSource{}, conv, stor, testOwner, bus, &logger.NopLogger{})
	if err := engine.SyncConversations(context.Background()); err != nil {
		t.Fatalf("SyncConversations: %v", err)
	}

	cs, _ := stor.ListConversations(testOwner)
	if len(cs) != 2 {
		t.Fatalf("convs = %d, want 2", len(cs))
	}
	// 倒序排列
	if cs[0].ConversationID != "c2" {
		t.Errorf("first conv = %s, want c2", cs[0].ConversationID)
	}
	vID, ver, _ := stor.GetVersion(testOwner, storage.EntityConversations)
	if vID != "cv1" || ver != 5 {
		t.Errorf("version = %s/%d, want cv1/5", vID, ver)
	}
	if got := drainEvents(chans[0], 5); got != 1 {
		t.Errorf("EventConversationSynced count = %d, want 1", got)
	}
}

// TestEntitySync_SyncAll_Chain SyncAll 顺序执行五类同步：游标与事件齐全
func TestEntitySync_SyncAll_Chain(t *testing.T) {
	rel := &mockRelSource{
		incFriends: &model.IncrementalFriends{VersionID: "fv", Version: 1, Insert: []model.FriendInfo{{FriendUserID: "f1"}}},
		incBlacks:  &model.IncrementalBlacks{VersionID: "bv", Version: 1},
	}
	group := &mockGroupSource{
		incGroups: &model.IncrementalGroups{VersionID: "gv", Version: 1},
	}
	conv := &mockConvSource{
		incConvs: &model.IncrementalConversations{VersionID: "cv", Version: 1},
	}
	stor := storage.NewMemoryStorage()
	bus, chans := newTestBus(t,
		events.EventFriendListSynced,
		events.EventBlackListSynced,
		events.EventGroupListSynced,
		events.EventConversationSynced,
	)

	engine := NewEntitySyncEngine(rel, group, conv, stor, testOwner, bus, &logger.NopLogger{})
	if err := engine.SyncAll(context.Background()); err != nil {
		t.Fatalf("SyncAll: %v", err)
	}

	for i, want := range []int{1, 1, 1, 1} {
		if got := drainEvents(chans[i], 5); got != want {
			t.Errorf("event channel %d count = %d, want %d", i, got, want)
		}
	}
	// 全部游标落盘
	for _, entity := range []string{storage.EntityFriends, storage.EntityBlacks, storage.EntityJoinGroups, storage.EntityConversations} {
		vID, _, err := stor.GetVersion(testOwner, entity)
		if err != nil || vID == "" {
			t.Errorf("entity %s version empty (err=%v)", entity, err)
		}
	}
}

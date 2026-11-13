//go:build js && wasm

package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"syscall/js"

	"github.com/PaperMan11/goim-sdk-core/pkg/model"
)

// IdbStorage IndexedDB 实现（wasm 平台）
//
// 架构：调用 JS 侧 goimIdb.op(handle, operation, argsJSON)，返回 Promise。
// 用 awaitPromise 同步阻塞等结果（js.Value.then/catch + channel）。
//
// Store 设计与 SQLiteStorage gorm 表模型完全同构：
//   seqs            keyPath=["owner"]                       {owner, maxSeqsJSON, minSeqsJSON}
//   versions        keyPath=["owner","entity"]              {owner, entity, versionID, version}
//   messages        keyPath=["owner","conversationID","seq"]  {owner, conversationID, seq, clientMsgID, dataJSON}
//   conversations   keyPath=["owner","conversationID"]       {owner, conversationID, latestMsgSendTime, dataJSON}
//   friends         keyPath=["owner","friendUserID"]        {owner, friendUserID, dataJSON}
//   blacks          keyPath=["owner","blackUserID"]         {owner, blackUserID, dataJSON}
//   groups          keyPath=["owner","groupID"]             {owner, groupID, dataJSON}
//   groupMembers    keyPath=["owner","groupID","userID"]    {owner, groupID, userID, dataJSON}
//   meta            keyPath=["key"]                         {key, value}

type IdbStorage struct {
	goimIdb js.Value // JS 侧 goimIdb 全局对象
}

// awaitPromise 阻塞等待 JS Promise 解析
// （wasm 单线程事件循环，回调和 select 同一 goroutine，安全）
func awaitPromise(promise js.Value) (js.Value, error) {
	resultCh := make(chan js.Value, 1)
	errCh := make(chan string, 1)

	jsErrorCtor := js.Global().Get("Error")

	then := js.FuncOf(func(this js.Value, args []js.Value) any {
		resultCh <- args[0]
		return nil
	})
	catch := js.FuncOf(func(this js.Value, args []js.Value) any {
		v := args[0]
		if v.InstanceOf(jsErrorCtor) {
			errCh <- js.Error{Value: v}.Error()
		} else {
			errCh <- v.String()
		}
		return nil
	})
	defer then.Release()
	defer catch.Release()

	promise.Call("then", then)
	promise.Call("catch", catch)

	select {
	case v := <-resultCh:
		return v, nil
	case msg := <-errCh:
		return js.Undefined(), fmt.Errorf("js promise rejected: %s", msg)
	}
}

// stringifyJS 把 js.Value 序列化为 JSON 文本。
//
// js.Value 未实现 json.Marshaler（结构体字段全部不可导出），encoding/json 直接
// 序列化只会得到 "{}"，必须回到 JS 侧 JSON.stringify。
func stringifyJS(v js.Value) ([]byte, error) {
	encoded := js.Global().Get("JSON").Call("stringify", v)
	if encoded.Type() != js.TypeString {
		return nil, fmt.Errorf("JSON.stringify result type %s", encoded.Type().String())
	}
	return []byte(encoded.String()), nil
}

func NewIdbStorage() (*IdbStorage, error) {
	g := js.Global().Get("goimIdb")
	if g.IsUndefined() || g.IsNull() {
		return nil, fmt.Errorf("storage: goimIdb not found, load cmd/wasm/idb_wrapper.js first")
	}
	if _, err := awaitPromise(g.Call("open")); err != nil {
		return nil, fmt.Errorf("storage: goimIdb.open: %w", err)
	}
	return &IdbStorage{goimIdb: g}, nil
}

// op 统一 JS→IndexedDB 桥：Go args → JSON → goimIdb.op → await → 解析结果
func (s *IdbStorage) op(operation string, args any) (js.Value, error) {
	argsJSON := ""
	if args != nil {
		bs, err := json.Marshal(args)
		if err != nil {
			return js.Undefined(), fmt.Errorf("storage: marshal args: %w", err)
		}
		argsJSON = string(bs)
	}
	result, err := awaitPromise(s.goimIdb.Call("op", operation, argsJSON))
	if err != nil {
		return js.Undefined(), err
	}
	// goimIdb.op 返回 JSON 字符串 {"ok":bool,"data":...,"error":string}，
	// 先解析信封再取 data（字符串上做 Get("ok") 恒为 undefined）
	if result.Type() != js.TypeString {
		return js.Undefined(), fmt.Errorf("storage: %s: unexpected result type %s", operation, result.Type().String())
	}
	var env struct {
		OK    bool            `json:"ok"`
		Data  json.RawMessage `json:"data"`
		Error string          `json:"error"`
	}
	if err := json.Unmarshal([]byte(result.String()), &env); err != nil {
		return js.Undefined(), fmt.Errorf("storage: %s: decode result: %w", operation, err)
	}
	if !env.OK {
		return js.Undefined(), fmt.Errorf("storage: %s: %s", operation, env.Error)
	}
	if len(env.Data) == 0 || string(env.Data) == "null" {
		return js.Null(), nil
	}
	return js.Global().Get("JSON").Call("parse", string(env.Data)), nil
}

func (s *IdbStorage) opJSONList(operation string, args any) ([]json.RawMessage, error) {
	v, err := s.op(operation, args)
	if err != nil {
		return nil, err
	}
	if v.IsNull() || v.IsUndefined() || !v.Truthy() {
		return nil, nil
	}
	bs, err := stringifyJS(v)
	if err != nil {
		return nil, fmt.Errorf("storage: %s marshal result: %w", operation, err)
	}
	var out []json.RawMessage
	if err := json.Unmarshal(bs, &out); err != nil {
		return nil, fmt.Errorf("storage: %s unmarshal result: %w", operation, err)
	}
	return out, nil
}

func dataFromRow(row json.RawMessage, dest any) error {
	var wrapped struct {
		DataJSON string `json:"dataJSON"`
	}
	if err := json.Unmarshal(row, &wrapped); err != nil {
		return fmt.Errorf("storage: row wrap: %w", err)
	}
	return json.Unmarshal([]byte(wrapped.DataJSON), dest)
}

func idsFromRows(rows []json.RawMessage, idField string) ([]string, error) {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		var row map[string]any
		if err := json.Unmarshal(r, &row); err != nil {
			return nil, err
		}
		v, ok := row[idField]
		if ok && v != nil {
			out = append(out, fmt.Sprint(v))
		}
	}
	return out, nil
}

// === 元数据 ===

func (s *IdbStorage) GetMaxSeqs(owner string) (map[string]int64, map[string]int64, error) {
	v, err := s.op("getMaxSeqs", map[string]any{"owner": owner})
	if err != nil {
		return nil, nil, err
	}
	if v.IsNull() || v.IsUndefined() || !v.Truthy() {
		return nil, nil, nil
	}
	// 直接读字段（js.Value 无 MarshalJSON，不能 json.Marshal(v)）
	maxJSON := v.Get("maxSeqsJSON").String()
	minJSON := v.Get("minSeqsJSON").String()

	var maxSeqs, minSeqs map[string]int64
	if maxJSON != "" {
		if err := json.Unmarshal([]byte(maxJSON), &maxSeqs); err != nil {
			return nil, nil, fmt.Errorf("storage: getMaxSeqs maxSeqsJSON: %w", err)
		}
	}
	if minJSON != "" {
		if err := json.Unmarshal([]byte(minJSON), &minSeqs); err != nil {
			return nil, nil, fmt.Errorf("storage: getMaxSeqs minSeqsJSON: %w", err)
		}
	}
	return maxSeqs, minSeqs, nil
}

func (s *IdbStorage) SetMaxSeqs(owner string, maxSeqs, minSeqs map[string]int64) error {
	maxJSON := ""
	if len(maxSeqs) > 0 {
		bs, _ := json.Marshal(maxSeqs)
		maxJSON = string(bs)
	}
	minJSON := ""
	if len(minSeqs) > 0 {
		bs, _ := json.Marshal(minSeqs)
		minJSON = string(bs)
	}
	_, err := s.op("setMaxSeqs", map[string]any{
		"owner":       owner,
		"maxSeqsJSON": maxJSON,
		"minSeqsJSON": minJSON,
	})
	return err
}

func (s *IdbStorage) GetVersion(owner, entity string) (string, uint64, error) {
	v, err := s.op("getVersion", map[string]any{"owner": owner, "entity": entity})
	if err != nil {
		return "", 0, err
	}
	if v.IsNull() || v.IsUndefined() || !v.Truthy() {
		return "", 0, nil
	}
	return v.Get("versionID").String(), uint64(v.Get("version").Int()), nil
}

func (s *IdbStorage) SetVersion(owner, entity, versionID string, version uint64) error {
	_, err := s.op("setVersion", map[string]any{
		"owner":     owner,
		"entity":    entity,
		"versionID": versionID,
		"version":   version,
	})
	return err
}

func (s *IdbStorage) ListVersions(owner string) (map[string]VersionInfo, error) {
	rows, err := s.opJSONList("listVersions", map[string]any{"owner": owner})
	if err != nil {
		return nil, err
	}
	out := make(map[string]VersionInfo, len(rows))
	for _, r := range rows {
		var row struct {
			Entity    string `json:"entity"`
			VersionID string `json:"versionID"`
			Version   uint64 `json:"version"`
		}
		if err := json.Unmarshal(r, &row); err != nil {
			return nil, err
		}
		out[row.Entity] = VersionInfo{VersionID: row.VersionID, Version: row.Version}
	}
	return out, nil
}

// === 消息 ===

func (s *IdbStorage) UpsertMsgs(owner string, msgs []*model.MsgData) error {
	if len(msgs) == 0 {
		return nil
	}
	rows := make([]map[string]any, 0, len(msgs))
	for _, m := range msgs {
		if m == nil || m.Seq <= 0 {
			continue
		}
		dataJSON, _ := json.Marshal(m)
		rows = append(rows, map[string]any{
			"owner":          owner,
			"conversationID": m.ConversationID,
			"seq":            m.Seq,
			"clientMsgID":    m.ClientMsgID,
			"dataJSON":       string(dataJSON),
		})
	}
	if len(rows) == 0 {
		return nil
	}
	_, err := s.op("upsertMsgs", map[string]any{"owner": owner, "rows": rows})
	return err
}

func (s *IdbStorage) ListMsgsByConversation(owner, conversationID string) ([]*model.MsgData, error) {
	rows, err := s.opJSONList("listMsgsByConversation", map[string]any{
		"owner":          owner,
		"conversationID": conversationID,
	})
	if err != nil {
		return nil, err
	}
	out := make([]*model.MsgData, 0, len(rows))
	for _, r := range rows {
		m := &model.MsgData{}
		if err := dataFromRow(r, m); err != nil {
			return nil, fmt.Errorf("storage: listMsgsByConversation: %w", err)
		}
		out = append(out, m)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

func (s *IdbStorage) DeleteMsgs(owner, conversationID string, seqs []int64) error {
	if conversationID == "" || len(seqs) == 0 {
		return nil
	}
	_, err := s.op("deleteMsgs", map[string]any{
		"owner":          owner,
		"conversationID": conversationID,
		"seqs":           seqs,
	})
	return err
}

func (s *IdbStorage) MarkMsgsRead(owner, conversationID string, seqs []int64) error {
	if conversationID == "" || len(seqs) == 0 {
		return nil
	}
	_, err := s.op("markMsgsRead", map[string]any{
		"owner":          owner,
		"conversationID": conversationID,
		"seqs":           seqs,
	})
	return err
}

func (s *IdbStorage) MarkMsgsReadUpTo(owner, conversationID string, hasReadSeq int64) error {
	if conversationID == "" || hasReadSeq <= 0 {
		return nil
	}
	_, err := s.op("markMsgsReadUpTo", map[string]any{
		"owner":          owner,
		"conversationID": conversationID,
		"hasReadSeq":     hasReadSeq,
	})
	return err
}

func (s *IdbStorage) MarkMsgsRevoked(owner, conversationID string, seqs []int64) error {
	if conversationID == "" || len(seqs) == 0 {
		return nil
	}
	_, err := s.op("markMsgsRevoked", map[string]any{
		"owner":          owner,
		"conversationID": conversationID,
		"seqs":           seqs,
	})
	return err
}

// === 会话 ===

func (s *IdbStorage) UpsertConversations(owner string, convs []*model.Conversation) error {
	if len(convs) == 0 {
		return nil
	}
	rows := make([]map[string]any, 0, len(convs))
	for _, c := range convs {
		if c == nil || c.ConversationID == "" {
			continue
		}
		dataJSON, _ := json.Marshal(c)
		rows = append(rows, map[string]any{
			"owner":             owner,
			"conversationID":    c.ConversationID,
			"latestMsgSendTime": c.LatestMsgSendTime,
			"dataJSON":          string(dataJSON),
		})
	}
	if len(rows) == 0 {
		return nil
	}
	_, err := s.op("upsertConversations", map[string]any{"owner": owner, "rows": rows})
	return err
}

func (s *IdbStorage) DeleteConversations(owner string, conversationIDs []string) error {
	if len(conversationIDs) == 0 {
		return nil
	}
	_, err := s.op("deleteConversations", map[string]any{
		"owner":           owner,
		"conversationIDs": conversationIDs,
	})
	return err
}

func (s *IdbStorage) ListConversations(owner string) ([]*model.Conversation, error) {
	rows, err := s.opJSONList("listConversations", map[string]any{"owner": owner})
	if err != nil {
		return nil, err
	}
	out := make([]*model.Conversation, 0, len(rows))
	for _, r := range rows {
		c := &model.Conversation{}
		if err := dataFromRow(r, c); err != nil {
			return nil, fmt.Errorf("storage: listConversations: %w", err)
		}
		out = append(out, c)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].LatestMsgSendTime > out[j].LatestMsgSendTime })
	return out, nil
}

func (s *IdbStorage) ListConversationIDs(owner string) ([]string, error) {
	rows, err := s.opJSONList("listConversationIDs", map[string]any{"owner": owner})
	if err != nil {
		return nil, err
	}
	return idsFromRows(rows, "conversationID")
}

// === 好友 ===

func (s *IdbStorage) UpsertFriends(owner string, friends []*model.FriendInfo) error {
	if len(friends) == 0 {
		return nil
	}
	rows := make([]map[string]any, 0, len(friends))
	for _, f := range friends {
		if f == nil || f.FriendUserID() == "" {
			continue
		}
		dataJSON, _ := json.Marshal(f)
		rows = append(rows, map[string]any{
			"owner":        owner,
			"friendUserID": f.FriendUserID(),
			"dataJSON":     string(dataJSON),
		})
	}
	if len(rows) == 0 {
		return nil
	}
	_, err := s.op("upsertFriends", map[string]any{"owner": owner, "rows": rows})
	return err
}

func (s *IdbStorage) DeleteFriends(owner string, friendUserIDs []string) error {
	if len(friendUserIDs) == 0 {
		return nil
	}
	_, err := s.op("deleteFriends", map[string]any{"owner": owner, "friendUserIDs": friendUserIDs})
	return err
}

func (s *IdbStorage) ListFriends(owner string) ([]*model.FriendInfo, error) {
	rows, err := s.opJSONList("listFriends", map[string]any{"owner": owner})
	if err != nil {
		return nil, err
	}
	out := make([]*model.FriendInfo, 0, len(rows))
	for _, r := range rows {
		f := &model.FriendInfo{}
		if err := dataFromRow(r, f); err != nil {
			return nil, fmt.Errorf("storage: listFriends: %w", err)
		}
		out = append(out, f)
	}
	return out, nil
}

func (s *IdbStorage) ListFriendIDs(owner string) ([]string, error) {
	rows, err := s.opJSONList("listFriendIDs", map[string]any{"owner": owner})
	if err != nil {
		return nil, err
	}
	return idsFromRows(rows, "friendUserID")
}

// === 黑名单 ===

func (s *IdbStorage) UpsertBlacks(owner string, blacks []*model.BlackInfo) error {
	if len(blacks) == 0 {
		return nil
	}
	rows := make([]map[string]any, 0, len(blacks))
	for _, b := range blacks {
		if b == nil || b.BlackUserID() == "" {
			continue
		}
		dataJSON, _ := json.Marshal(b)
		rows = append(rows, map[string]any{
			"owner":       owner,
			"blackUserID": b.BlackUserID(),
			"dataJSON":    string(dataJSON),
		})
	}
	if len(rows) == 0 {
		return nil
	}
	_, err := s.op("upsertBlacks", map[string]any{"owner": owner, "rows": rows})
	return err
}

func (s *IdbStorage) DeleteBlacks(owner string, blackUserIDs []string) error {
	if len(blackUserIDs) == 0 {
		return nil
	}
	_, err := s.op("deleteBlacks", map[string]any{"owner": owner, "blackUserIDs": blackUserIDs})
	return err
}

func (s *IdbStorage) ListBlacks(owner string) ([]*model.BlackInfo, error) {
	rows, err := s.opJSONList("listBlacks", map[string]any{"owner": owner})
	if err != nil {
		return nil, err
	}
	out := make([]*model.BlackInfo, 0, len(rows))
	for _, r := range rows {
		b := &model.BlackInfo{}
		if err := dataFromRow(r, b); err != nil {
			return nil, fmt.Errorf("storage: listBlacks: %w", err)
		}
		out = append(out, b)
	}
	return out, nil
}

func (s *IdbStorage) ListBlackIDs(owner string) ([]string, error) {
	rows, err := s.opJSONList("listBlackIDs", map[string]any{"owner": owner})
	if err != nil {
		return nil, err
	}
	return idsFromRows(rows, "blackUserID")
}

// === 群 ===

func (s *IdbStorage) UpsertGroups(owner string, groups []*model.GroupInfo) error {
	if len(groups) == 0 {
		return nil
	}
	rows := make([]map[string]any, 0, len(groups))
	for _, g := range groups {
		if g == nil || g.GroupID == "" {
			continue
		}
		dataJSON, _ := json.Marshal(g)
		rows = append(rows, map[string]any{
			"owner":    owner,
			"groupID":  g.GroupID,
			"dataJSON": string(dataJSON),
		})
	}
	if len(rows) == 0 {
		return nil
	}
	_, err := s.op("upsertGroups", map[string]any{"owner": owner, "rows": rows})
	return err
}

func (s *IdbStorage) DeleteGroups(owner string, groupIDs []string) error {
	if len(groupIDs) == 0 {
		return nil
	}
	_, err := s.op("deleteGroups", map[string]any{"owner": owner, "groupIDs": groupIDs})
	return err
}

func (s *IdbStorage) ListGroups(owner string) ([]*model.GroupInfo, error) {
	rows, err := s.opJSONList("listGroups", map[string]any{"owner": owner})
	if err != nil {
		return nil, err
	}
	out := make([]*model.GroupInfo, 0, len(rows))
	for _, r := range rows {
		g := &model.GroupInfo{}
		if err := dataFromRow(r, g); err != nil {
			return nil, fmt.Errorf("storage: listGroups: %w", err)
		}
		out = append(out, g)
	}
	return out, nil
}

func (s *IdbStorage) ListGroupIDs(owner string) ([]string, error) {
	rows, err := s.opJSONList("listGroupIDs", map[string]any{"owner": owner})
	if err != nil {
		return nil, err
	}
	return idsFromRows(rows, "groupID")
}

// === 群成员 ===

func (s *IdbStorage) UpsertGroupMembers(owner, groupID string, members []*model.GroupMemberFullInfo) error {
	if len(members) == 0 {
		return nil
	}
	rows := make([]map[string]any, 0, len(members))
	for _, m := range members {
		if m == nil || m.UserID == "" {
			continue
		}
		dataJSON, _ := json.Marshal(m)
		rows = append(rows, map[string]any{
			"owner":    owner,
			"groupID":  groupID,
			"userID":   m.UserID,
			"dataJSON": string(dataJSON),
		})
	}
	if len(rows) == 0 {
		return nil
	}
	_, err := s.op("upsertGroupMembers", map[string]any{
		"owner":   owner,
		"groupID": groupID,
		"rows":    rows,
	})
	return err
}

func (s *IdbStorage) DeleteGroupMembers(owner, groupID string, userIDs []string) error {
	if len(userIDs) == 0 {
		return nil
	}
	_, err := s.op("deleteGroupMembers", map[string]any{
		"owner":   owner,
		"groupID": groupID,
		"userIDs": userIDs,
	})
	return err
}

func (s *IdbStorage) ListGroupMembers(owner, groupID string) ([]*model.GroupMemberFullInfo, error) {
	rows, err := s.opJSONList("listGroupMembers", map[string]any{
		"owner":   owner,
		"groupID": groupID,
	})
	if err != nil {
		return nil, err
	}
	out := make([]*model.GroupMemberFullInfo, 0, len(rows))
	for _, r := range rows {
		m := &model.GroupMemberFullInfo{}
		if err := dataFromRow(r, m); err != nil {
			return nil, fmt.Errorf("storage: listGroupMembers: %w", err)
		}
		out = append(out, m)
	}
	return out, nil
}

func (s *IdbStorage) ListGroupMemberIDs(owner, groupID string) ([]string, error) {
	rows, err := s.opJSONList("listGroupMemberIDs", map[string]any{
		"owner":   owner,
		"groupID": groupID,
	})
	if err != nil {
		return nil, err
	}
	return idsFromRows(rows, "userID")
}

// === 生命周期 ===

func (s *IdbStorage) Clear(_ context.Context, owner string) error {
	_, err := s.op("clearOwner", map[string]any{"owner": owner})
	return err
}

// === SDK 级元数据 ===

func (s *IdbStorage) GetMeta(key string) (string, error) {
	v, err := s.op("getMeta", map[string]any{"key": key})
	if err != nil {
		return "", err
	}
	if v.IsNull() || v.IsUndefined() || !v.Truthy() {
		return "", nil
	}
	return v.Get("value").String(), nil
}

func (s *IdbStorage) SetMeta(key, value string) error {
	_, err := s.op("setMeta", map[string]any{"key": key, "value": value})
	return err
}

var _ Storage = (*IdbStorage)(nil)

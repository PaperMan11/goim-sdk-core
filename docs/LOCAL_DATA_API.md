# 本地数据能力 API 设计（Local Data API）

> 状态：P1/P2/P3 已实现（`Client.Local()` 门面 + 会话/消息/好友/黑名单/群/群成员直读、分页、搜索、
> `Stats` 统计与本地清理；Storage 三实现：Memory/SQLite 完整，IndexedDB 消息分页/直读/计数/搜索/清理已下推，
> DB 升至 v2）。`PurgeMsgs` / floor 坐标复用 `versions`（`entity=msgFloor:<convID>`），
> `SeqSyncEngine` 的 gap 起点已按 `max(localMax, floor)` 收敛。

## 1. 背景与现状

SDK 的本地存储（SQLite / IndexedDB / Memory）已经维护了会话、消息、好友、黑名单、群、群成员
六类数据，但对外只暴露了几个零散方法：

| 已有 API | 说明 |
|---|---|
| `Local.GetConversation(convID)` | 单会话本地直读 |
| `Local.UnreadCount(convID)` / `UnreadTotal()` | 本地推导未读 |
| `Client.GetHistoryMsgs` / `GetNewerMsgs` | 服务端分页历史 + 写穿缓存（需网络，不归 Local） |

> P1 已收敛：`Client` 接口不再重复暴露前三个方法，统一从 `Local()` 访问。

缺的是一整层「列表 / 分页 / 直读 / 搜索 / 统计 / 本地清理」能力，宿主只能自己
`ListConversations` 全量再筛选，或者绕到 `Storage`（跨语言不可达）。

## 2. 设计边界

**放在 core、做成完整 API**：

- 所有读写本地库的操作（数据所有权、owner 隔离、一致性）
- 一致性敏感的派生值：未读数、会话摘要、消息 seq 边界

**不放进来**：

- 远程业务写操作（置顶/免打扰/草稿/清空服务端历史）仍走 `Conv` / `Msg` Manager，
  本地缓存由同步引擎收敛
- UI 状态（滚动位置、加载中、选中态）留在语言封装层
- 语言层不得直接打开 SQLite/IndexedDB

## 3. 接口形态

`Client` 接口已经较大，再塞 20+ 方法会失控。新增**独立门面**，与六个 Manager 平级：

```go
// pkg/client/client.go
Local() manager.LocalDataMgr   // 本地数据能力（只读缓存 + 本地维护）
```

- 实现放在 `pkg/manager/local.go`：依赖 `storage.Storage` + `ownerFn func() string`（由
  `DefaultClient` 注入 `currentOwner`），不依赖 HTTP/WS
- 反射注册统一加前缀 `Local.`（沿用现有 Manager 的 `Conv.` / `Msg.` 规则），
  跨语言调用形如 `goimSdk.call("Local.ListConversations", [0, 50])`
- P1 已收敛：`Client` 接口不再重复暴露 `GetConversation` / `UnreadCount` / `UnreadTotal`，
  统一从 `Local()` 访问；跨语言只注册 `Local.*`（`GetHistoryMsgs` / `GetNewerMsgs` 需网络，仍留在 `Client`）

## 4. API 设计

### 4.1 会话

| 方法 | 返回 | 说明 |
|---|---|---|
| `ListConversations(offset, limit int32)` | `*model.ConversationPage` | 本地列表，置顶优先 + `LatestMsgSendTime` 倒序，分页 |
| `GetConversation(convID)` | `*Conversation` | 单条，不存在 `nil` |
| `GetConversations(convIDs []string)` | `[]*Conversation` | 批量直读（减少跨语言往返） |
| `SearchConversations(keyword, limit int32)` | `*model.ConversationPage` | 匹配会话 ID / 对方昵称 / 群名 / 摘要文本 |
| `UnreadCount(convID)` / `UnreadTotal()` | `int64` | 沿用现有本地推导 |
| `GetConversationSeqs(convID)` | `*model.ConversationSeqs` | `LocalMinSeq / LocalMaxSeq / HasReadSeq / ServerMinSeq / ServerMaxSeq` |

### 4.2 消息

| 方法 | 返回 | 说明 |
|---|---|---|
| `ListMsgs(convID, beforeSeq int64, limit int32)` | `*model.MsgPage` | 本地降序分页；`beforeSeq<=0` 取最新 |
| `ListMsgsAfter(convID, afterSeq int64, limit int32)` | `*model.MsgPage` | 本地升序分页（补断档查看用） |
| `GetMsg(convID, seq int64)` | `*MsgData` | 按 seq 直读 |
| `GetMsgByClientID(convID, clientMsgID string)` | `*MsgData` | 按客户端消息 ID 直读（重发去重、回执匹配） |
| `SearchMsgs(convID, keyword string, limit int32)` | `*model.MsgPage` | 单会话内搜索（v1 用 LIKE，后续可换 FTS） |
| `CountMsgs(convID)` | `int64` | 缓存条数 |

### 4.3 好友 / 黑名单

| 方法 | 返回 | 说明 |
|---|---|---|
| `ListFriends(offset, limit int32)` | `*model.FriendPage` | 置顶优先 + 昵称排序，分页 |
| `GetFriend(userID)` | `*FriendInfo` | 单条，不存在 `nil` |
| `ListBlacks(offset, limit int32)` | `*model.BlackPage` | 黑名单分页 |
| `IsBlocked(userID)` | `bool` | 黑名单存在性判断 |
| `SearchFriends(keyword, limit int32)` | `*model.FriendPage` | 匹配 userID / 昵称 / 备注 |

### 4.4 群 / 群成员

| 方法 | 返回 | 说明 |
|---|---|---|
| `ListGroups(offset, limit int32)` | `*model.GroupPage` | 分页 |
| `GetGroup(groupID)` | `*GroupInfo` | 单条，不存在 `nil` |
| `SearchGroups(keyword, limit int32)` | `*model.GroupPage` | 匹配 groupID / 群名 |
| `ListGroupMembers(groupID string, offset, limit int32)` | `*model.GroupMemberPage` | 群内成员分页 |
| `GetGroupMember(groupID, userID string)` | `*GroupMemberFullInfo` | 单条 |

### 4.5 统计与本地维护

| 方法 | 返回 | 说明 |
|---|---|---|
| `Stats()` | `*model.LocalDataStats` | 各类实体条数（会话/消息/好友/黑名单/群/成员） |
| `PurgeMsgs(convID string, upToSeq int64)` | `int64` | 删除本地 `seq <= upToSeq` 的消息并记录 floor；返回删除条数 |
| `ClearLocalData()` | `error` | 清空当前 owner 全部本地数据（不登出） |

> 清空/删除服务端历史依旧走 `Msg.ClearMsgs` 远程接口；`PurgeMsgs` 只处理本机缓存。

## 5. 统一语义

### 5.1 分页

- 实体列表（会话/好友/群/成员）：`offset + limit`，`limit` 默认 50、上限 200
- 消息列表：seq 游标闭区间 `beforeSeq / afterSeq`，与 `GetHistoryMsgs` 一致（下一次传 `page.OldestSeq-1`）
- 分页结果统一带 `HasMore`；实体分页额外带 `Total`（SQLite `COUNT`、IndexedDB 逐条计数、
  Memory 直接 len），消息分页不带 `Total`（代价高且无 UI 场景）

```go
type ConversationPage struct {
    Items   []*Conversation `json:"items"`
    Total   int64           `json:"total"`
    HasMore bool            `json:"hasMore"`
}
// FriendPage / BlackPage / GroupPage / GroupMemberPage 同构
type MsgPage struct {
    Items     []*MsgData `json:"items"`
    OldestSeq int64      `json:"oldestSeq"`
    NewestSeq int64      `json:"newestSeq"`
    HasMore   bool       `json:"hasMore"`
}
```

### 5.2 未登录与错误

- owner 固定取当前登录用户；未登录返回 `ErrNotLoggedIn`（Go 侧 `errors.Is` 可判定，
  跨语言调用在 call 层登录态校验处返回 `NotLoggedIn` 错误码），不返回空结果冒充成功
- 单条查询不存在：返回 `(nil, nil)`（便于宿主区分「无数据」与「失败」）
- 分页查询无数据：返回空 `Items`、`Total=0`、`HasMore=false`
- 全部方法**不发网络请求**；缓存由同步引擎/推送维护，Local 层只读（`PurgeMsgs`/`ClearLocalData` 除外）

### 5.3 消息清理与 floor 语义（关键）

`PurgeMsgs` 删除本地消息后，本地 `maxSeq` 可能被清掉；下一次同步若按 `serverMin` 补齐，
会把刚清掉的历史又拉回来。因此需要记录「本机主动放弃的下界」：

- 新增 per-owner 元数据 `msgFloor:<convID>`（复用 `versions` 表，`entity=msgFloor:<convID>`，`version=floor`）
- `SeqSyncEngine` 计算 gap 起点时：`start = max(localMax, msgFloor)`；全量回填下界同理
- `GetHistoryMsgs` 是**显式用户请求**，不受 floor 限制（用户主动翻看历史仍能拉到）
- `ClearLocalData` 等价于对全部会话 `PurgeMsgs(convID, localMax)` + 清空实体表

## 6. Storage 扩展清单

现有 `Storage` 接口之外需要补（三个实现同步：Memory / SQLite / Idb）：

| 能力 | 新增方法 |
|---|---|
| 会话分页/搜索/计数 | `ListConversationsPage(owner, offset, limit)`, `SearchConversations(owner, kw, limit)`, `CountConversations(owner)` |
| 消息分页/直读/搜索/计数 | `ListMsgsPage(owner, convID, beforeSeq, limit, desc)`, `GetMsg`, `GetMsgByClientID`, `SearchMsgs`, `CountMsgs` |
| 好友分页/直读/搜索/计数 | `ListFriendsPage`, `GetFriend`, `SearchFriends`, `CountFriends` |
| 黑名单分页/存在/计数 | `ListBlacksPage`, `GetBlack`, `CountBlacks` |
| 群分页/直读/搜索/计数 | `ListGroupsPage`, `GetGroup`, `SearchGroups`, `CountGroups` |
| 群成员分页/直读/计数 | `ListGroupMembersPage`, `GetGroupMember`, `CountGroupMembers` |
| 消息 floor | `GetMsgFloor(owner, convID)`, `SetMsgFloor(owner, convID, seq)` |
| 清理 | `PurgeMsgs(owner, convID, upToSeq) (int64, error)` |

> P1 已落地 `*Page` / `Get*` 系列；P2 已落地 `Search*`（缓存 JSON 文本大小写不敏感子串匹配，
> SQLite 用 `LOWER(data_json) LIKE`，Memory/IndexedDB 在 JSON 文本上 `includes`）与 `Count*`，
> IndexedDB 的消息分页、单条直读、按 clientMsgID 直读、计数与搜索均已下推，
> `DB_VERSION` 升至 2 并补 `messages.byClientMsgID` / `conversations.byLatestMsgSendTime` 索引（不重建 store）。
> P3 已落地 `GetMsgFloor` / `SetMsgFloor`（复用 `versions`）、`PurgeMsgs`（Memory/SQLite/IndexedDB 三实现）。

存储实现要点：

- **SQLite**：分页用 `LIMIT/OFFSET`；消息分页 `WHERE owner=? AND conversation_id=? AND seq <= ? ORDER BY seq DESC LIMIT ?`；
  搜索对 `data_json` 做 `LIKE`（v1），消息搜索限定单会话避免全表扫
- **IndexedDB**：`messages` store 已有 `["owner","conversationID","seq"]` 复合键，分页直接用
  `IDBKeyRange.bound([owner, conv, minSeq], [owner, conv, seq])`；搜索在 JS 侧遍历 + `includes`（限定会话）
- **Memory**：排序/切片即可
- 新字段/索引涉及 IndexedDB schema 变更 → `DB_VERSION` 升一级，在 `onupgradeneeded` 补索引（不重建 store）
- SQLite 只加查询，不改表结构

## 7. 跨语言桥接

- 反射注册：`Local.*`（会话/消息/好友/黑名单/群/成员/统计/清理全部 JSON 兼容）
- 批量优先：`Local.GetConversations(ids)` / `Local.GetFriend(id)` 用于替代「列表 + 筛选」
- 示例：

```js
// Web / wasm
const page = await goimSdk.syncCall("Local.ListConversations", JSON.stringify([0, 50]));
const conv = await goimSdk.syncCall("Local.GetConversation", JSON.stringify(["si_a_b"]));
const older = await goimSdk.syncCall("Local.ListMsgs", JSON.stringify(["si_a_b", 100, 30]));
```

```c
// cgo / PC
GoimSdkSyncCall("Local.ListConversations", "[]", "...");
```

gomobile 侧由 `Call/SyncCall` 的反射层覆盖，无需额外导出函数。

## 8. 分阶段实施

| 阶段 | 内容 | 验收 |
|---|---|---|
| P0（已完成） | `GetConversation`、`UnreadCount/UnreadTotal`、`GetHistoryMsgs/GetNewerMsgs` | 现有单测 |
| P1（已完成） | `LocalDataMgr` 门面 + 会话/消息/好友/黑名单/群/成员**直读**与**分页** + Storage 扩展（Memory/SQLite，Idb 过渡实现） | Memory/SQLite 单测 + wasm 交叉编译通过 + 跨语言 `Local.*` 已注册 |
| P2（已完成） | 搜索、`Stats`、IndexedDB 下推与 schema 升级 | Memory/SQLite 同套搜索/计数/`Stats` 单测 + wasm 交叉编译通过 + 自测页验证 IDB 分页/搜索/计数 |
| P3（已完成） | `PurgeMsgs` + floor 语义 + 同步引擎联动 + `ClearLocalData` | 清理后重连不回流、显式历史仍可拉 |

## 9. 风险与取舍

- **接口膨胀**：用独立门面 + 前缀隔离；`Client` 旧方法保留兼容，后续版本收敛
- **搜索性能**：SQLite pure-Go 驱动不保证 FTS5，v1 用 LIKE + 限定会话/条数；数据量大时再评估内存索引
- **Total 代价**：IndexedDB 用 `store.count(range)` 计数；只对实体分页提供 `Total`，消息分页不给
- **floor 一致性**：floor 只影响自动同步，不拦截显式历史请求；文档需明确二者差异
- **多端语义**：本地清理只影响本机；其他设备/服务端数据不变，重装后仍可拉回（符合预期）

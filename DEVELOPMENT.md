# goim-sdk-core 开发文档

> GoIM Server SDK 核心库 — 多平台 SDK 的统一底座
> Module: `github.com/PaperMan11/goim-sdk-core`
> Proto 依赖: `github.com/PaperMan11/goim/pkg/protocol`

---

## 1. 项目定位

**SDK 核心库，不直接面向终端用户**。作为统一底座被各平台 SDK 封装：

| 目标平台 | 编译方式 | 封装产物 | 本地存储 |
|---|---|---|---|
| iOS | gomobile bind | .framework | SQLite |
| Android | gomobile bind | .aar | SQLite |
| PC 桌面 | cgo | .dll / .dylib / .so | SQLite |
| Web | wasm | .wasm | IndexedDB（替代 SQLite） |

> 封装方式暂缓实现，SDK 核心库完成后再编译输出。

---

## 2. 技术选型

| 组件 | 选型 | 版本 | 选型理由 |
|---|---|---|---|
| 语言 | Go | 1.22+ | gomobile / cgo / wasm 均原生支持 |
| HTTP | `net/http` + `http.RoundTripper` 接口 | 标准库 | 无外部依赖、Transport 可插拔便于 mock 和中间件（重试、token 刷新） |
| JSON | `encoding/json` | 标准库 | gomobile 对标准库 json 支持好 |
| WebSocket | `github.com/gorilla/websocket` | v1.x | 成熟稳定、API 完善、gomobile 社区广泛验证过 |
| UUID | `github.com/google/uuid` | v1.x | clientMsgID 生成、operationID 生成 |
| Token 存储 | `sync.RWMutex` + 接口抽象 | 标准库 | 多 platformID 并发安全、可 mock |
| 错误模型 | 自定义 `SDKError{Code, Msg, OperationID}` | — | 剥离 rpc/grpc 前缀，直接返回原始业务错误 |
| 本地存储 ORM | `gorm.io/gorm` | v1.x | 表结构少（seq/version 等），gorm 省 CRUD 代码、迁移方便 |
| 本地存储驱动 | `github.com/glebarez/sqlite` | v1.x | **pure-Go** SQLite 驱动，基于 modernc，实现了 `database/sql`，gomobile 和 wasm 均可用 |
| 日志 | `go.uber.org/zap` | v1.x | 高性能结构化日志、支持动态级别、`zap.L()` 全局 Logger 便捷调用 |

> **gorm + sqlite 注意事项**：
> - gomobile 不支持 Go 泛型，gorm v2 的泛型 API（`gorm.G[T]` 等）不能用，全部用非泛型 API（`db.Where().Find()` 等）即可
> - wasm 场景下 sqlite 不可用，storage 接口的 IndexedDB 实现独立于 gorm
> - 单元测试可直接用 `gorm.io/driver/sqlite` 的内存模式（`file::memory:?cache=shared`）

### gomobile 限制与应对

| 限制 | 影响 | 应对 |
|---|---|---|
| 不支持跨语言 channel | event dispatch 无法直接暴露 channel | 封装层做 channel → 原生回调桥接（iOS block / Android lambda） |
| 回调签名限制 | gomobile 只支持基本类型参数 | 事件 payload 序列化为 JSON string 传递 |
| 不支持泛型 | Go 1.18+ 泛型在 gomobile 下不可用 | 全部用具体类型 + interface{} 兜底 |

---

## 3. 架构设计

### 3.1 四层架构

```
┌─────────────────────────────────────────────────────────────┐
│                     pkg/client (Facade)                      │
│   Client 接口，聚合所有 Manager，统一入口，生命周期管理         │
├─────────────────────────────────────────────────────────────┤
│                      pkg/manager                             │
│  AuthMgr │ UserMgr │ MsgMgr │ GroupMgr │ RelMgr │ ConvMgr    │
│  ──── 业务编排层 ────                                        │
│  · seq 同步引擎  · 增量/全量同步策略  · 本地状态缓存           │
├─────────────────────────────────────────────────────────────┤
│                     pkg/transport                            │
│  HTTPClient 接口 (+ 默认实现)   │   WSClient 接口 (+ 默认实现) │
│  · 自动 token/operationID 注入  │   · 自动重连  · 心跳保活    │
│  · 错误码解析  · Token 过期刷新  │   · 事件分发器             │
├─────────────────────────────────────────────────────────────┤
│                      pkg/model                               │
│  DTO + 枚举常量 + 错误码 + SDKError                          │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 依赖方向（严格单向）

```
client → manager → transport → model
                 ↓
              pkg/events   (事件类型定义，WS 推送也依赖这里)
                 ↓
              model
```

- `model` 是最底层，**零依赖**（除 proto 包）
- `transport` 依赖 `model` 和 `events`
- `manager` 依赖 `transport`、`model`、`events`
- `client` 依赖所有层
- 所有层**不反向依赖**，同层之间不互相依赖

### 3.3 核心子系统

#### A. HTTP Client 中间件链

```
HTTPClient.Post(path, body, resp)
    │
    ├─ 1. OperationID 自动生成 (sdk:{uuid})
    ├─ 2. Token 注入 Header
    ├─ 3. 发送请求
    ├─ 4. 解析响应 code
    │      ├─ code == 0 → 返回 data
    │      ├─ code in [1501, 1504] → Token 过期 → 刷新 → 重试一次
    │      └─ code != 0 → 返回 SDKError
    └─ 5. 重试（指数退避，网络层）
```

#### B. WebSocket 连接流程

```
1. 获取 User Token (HTTP)
2. WS Handshake
   Header: token, userID, platformID
3. 认证成功 → OnConnectSuccess 事件
4. 启动心跳保活 (ping/pong)
5. 消息循环
   ├─ 收消息 → 反序列化 → EventDispatcher.Dispatch()
   ├─ 连接断开 → 自动重连（指数退避） → 重连成功后触发全量 Sync
   └─ OnConnectSuccess → 触发 SeqSyncEngine.FullSync()
```

#### C. Seq 同步引擎

```
启动时:
  newest_seq() → 各会话 maxSeq/minSeq
  pull_msg_by_seq([minSeq, maxSeq]) → 全量补齐

运行时增量:
  WS 推送新消息 → OnNewMessage() 更新 maxSeq → 通知上层

重连后:
  丢弃本地缓存 → 重新执行启动流程

已读上报:
  mark_conversation_as_read(conversationID, hasReadSeq)
```

#### D. 增量/全量同步策略

适用于 Group / Friend / Conversation 关系型数据：

| 场景 | 接口 | 输入 | 触发条件 |
|---|---|---|---|
| 常规同步 | `get_incremental_*` | version + versionID | App 前台定期、WS 推送变更后 |
| 冷启动 / 版本回退 | `get_full_*` | idHash 对比 | 无 version、服务端返回 `full=true` |

---

## 4. 包结构

```
goim-sdk-core/
├── DEVELOPMENT.md               # 本文档
├── README.md                    # 项目简介
├── go.mod                       # module: github.com/PaperMan11/goim-sdk-core
├── go.sum
├── Makefile                     # build / test / generate 入口
│
├── pkg/
│   ├── model/                   # DTO + 枚举 + 错误码 + SDKError
│   │   ├── user.go              # UserInfo, PublicUserInfo
│   │   ├── group.go             # GroupInfo, GroupMemberFullInfo, GroupRequest...
│   │   ├── msg.go               # MsgData, OfflinePushInfo, ChatLog...
│   │   ├── conversation.go      # Conversation, MsgInfo, ConversationElem...
│   │   ├── relation.go          # FriendInfo, BlackInfo, FriendRequest...
│   │   ├── error.go             # SDKError + 错误码常量
│   │   └── const.go             # 枚举常量 (sessionType, msgFrom, recvMsgOpt, roleLevel...)
│   │
│   ├── events/                  # WS 推送事件类型定义（单独包避免循环依赖）
│   │   ├── event.go             # EventType 枚举 + Event 通用结构
│   │   ├── msg.go               # MsgData, RevokeMsgTips, MarkAsReadTips...
│   │   ├── group.go             # GroupCreatedTips, MemberEnterTips...
│   │   ├── relation.go          # FriendAddedTips, BlackAddedTips...
│   │   └── conversation.go      # ConversationUpdateTips...
│   │
│   ├── transport/               # HTTP + WS + Auth 接口与默认实现
│   │   ├── http.go              # HTTPClient 接口 + 默认实现（token/opID 注入 + 错误解析 + 重试）
│   │   ├── http_middleware.go   # RoundTripper 实现：TokenRefresh, Retry, Logging
│   │   ├── ws.go                # WSClient 接口 + 默认实现（自动重连 + 心跳 + 事件分发）
│   │   ├── auth.go              # TokenStore 接口 + 默认实现（RWMutex 保护 + 多 platform 支持）
│   │   └── dispatcher.go        # EventDispatcher（channel-based pub/sub）
│   │
│   ├── manager/                 # 六个业务 Manager 接口 + 默认实现
│   │   ├── auth.go              # AuthMgr: GetAdminToken, GetUserToken, ParseToken, ForceLogout
│   │   ├── user.go              # UserMgr: 注册/更新/查询/在线状态/客户端配置
│   │   ├── msg.go               # MsgMgr: 发送/拉取/撤回/已读/搜索/删除
│   │   ├── group.go             # GroupMgr: CRUD/成员/申请/转让/禁言/增量同步
│   │   ├── relation.go          # RelMgr: 好友/黑名单/申请/备注/增量同步
│   │   ├── conversation.go      # ConvMgr: 列表/属性/增量同步
│   │   └── sync.go              # SeqSyncEngine: FullSync / IncrementalSync
│   │
│   ├── client/                  # Facade 顶层 Client
│   │   └── client.go            # Client 接口 + 默认实现（聚合六个 Manager + 生命周期）
│   │
│   └── storage/                 # 本地存储抽象（SQLite / IndexedDB）
│       ├── storage.go           # Storage 接口 + 内存实现（测试用）
│       ├── sqlite.go            # SQLite 实现（桌面/iOS/Android）
│       └── wasm_indexeddb.go    # IndexedDB 实现（Web）
│
├── internal/
│   ├── uuid.go                  # clientMsgID / operationID 生成
│   ├── retry.go                 # 指数退避重试
│   └── backoff.go               # 退避策略
│
├── wasm/                        # Web 端入口（后续实现）
│   ├── cmd/main.go              # wasm 入口
│   └── indexdb/                 # IndexedDB 封装
│
└── tests/                       # 集成测试
    └── integration_test.go      # 完整流程测试（用 mock 服务）
```

---

## 5. 开发阶段与步骤

### Phase 1: 基础层（model + events + transport）

> 优先级最高，后续所有层依赖这些定义。

**步骤 1.1 — 初始化项目**
- `go mod init github.com/PaperMan11/goim-sdk-core`
- 引入 proto 依赖 `github.com/PaperMan11/goim/pkg/protocol`
- 引入第三方依赖：`google/uuid`, `gorilla/websocket`, `gorm.io/gorm`, `glebarez/sqlite`, `go.uber.org/zap`

**步骤 1.2 — pkg/model**
- 按文档第 6 章定义所有 DTO struct（纯 JSON tag，先不加 proto 字段，按需再加）
- `error.go`：定义 `SDKError` 类型，实现 `error` 接口，`Error()` 方法剥离 rpc/grpc 前缀
- 错误码常量（文档 5.2 节全部 30+ 错误码）
- `const.go`：所有枚举常量（sessionType / msgFrom / recvMsgOpt / roleLevel / PullOrder / TokenStatus ...）

**步骤 1.3 — pkg/events**
- 定义 `EventType` 枚举（24 种 WS 推送事件）
- 定义各 Tip 结构体（直接从 proto 对应字段镜像过来）
- 定义通用 `Event` 结构：`{ Type EventType, Payload interface{}, Timestamp int64 }`

**步骤 1.4 — pkg/transport**
- `http.go`：HTTPClient 接口定义（Post/Get + 自动注入 token/opID + 统一错误解析）
- `http_middleware.go`：RoundTripper 中间件链实现
  - TokenRefresh：code 1501/1504 触发刷新后重试一次
  - Retry：指数退避，最多 3 次
  - Logging：debug 级别请求/响应日志
- `auth.go`：TokenStore 接口（按 `(userID, platformID)` 存储）+ RWMutex 默认实现
- `ws.go`：WSClient 接口（Connect / Disconnect / IsConnected / SendMessage）+ 默认实现
  - 自动重连（指数退避，最大 30s）
  - 心跳保活（30s ping）
  - 连接状态回调（OnConnect / OnDisconnect / OnError）
- `dispatcher.go`：EventDispatcher（channel-based pub/sub）
  - `Subscribe(etype EventType) (<-chan Event, UnsubscribeFunc)`
  - 内部用 `map[EventType][]chan<- Event`，每个订阅者独立 channel，buffer=64
  - 派发时用 `select` 防止慢消费者阻塞整个分发

**完成标准**：model 编译通过 + events 编译通过 + transport 编译通过 + `go test ./pkg/...` 能跑通 transport 的 mock 测试。

---

### Phase 2: Manager 接口骨架（先接口后实现）

> 先把六个 Manager 的 interface 全部定义完，锁定边界，再逐个填充实现。

**步骤 2.1 — pkg/manager/auth.go**

```go
type AuthMgr interface {
    GetAdminToken(ctx context.Context, secret, userID string) (*TokenResult, error)
    GetUserToken(ctx context.Context, userID string, platformID int32) (*TokenResult, error)
    ParseToken(ctx context.Context, token string) (*TokenInfo, error)
    ForceLogout(ctx context.Context, userID string, platformID int32) error
}
```

**步骤 2.2 — pkg/manager/user.go**

```go
type UserMgr interface {
    Register(ctx context.Context, users []*model.UserInfo) error
    UpdateUserInfo(ctx context.Context, userInfo *model.UserInfo) error
    UpdateUserInfoEx(ctx context.Context, userInfo *model.UserInfo) error
    SetGlobalMsgRecvOpt(ctx context.Context, userID string, opt int32) error
    GetUsersInfo(ctx context.Context, userIDs []string) ([]*model.UserInfo, error)
    GetAllUsersUID(ctx context.Context, pagination *model.RequestPagination) (total int32, userIDs []string, err error)
    AccountCheck(ctx context.Context, checkUserIDs []string) ([]*model.AccountStatus, error)
    GetUsers(ctx context.Context, pagination *model.RequestPagination, userID, nickName string) (total int32, users []*model.UserInfo, err error)
    GetUsersOnlineStatus(ctx context.Context, userID string, userIDs []string) ([]*model.OnlineStatus, error)
    SubscribeUsersStatus(ctx context.Context, userID string, userIDs []string, genre int32) ([]*model.OnlineStatus, error)
    GetSubscribeUsersStatus(ctx context.Context, userID string) ([]*model.OnlineStatus, error)
    GetUserClientConfig(ctx context.Context, userID string) (map[string]string, error)
    SetUserClientConfig(ctx context.Context, userID string, configs map[string]string) error
    DelUserClientConfig(ctx context.Context, userID string, keys []string) error
}
```

**步骤 2.3 — pkg/manager/msg.go**

```go
type MsgMgr interface {
    SendMsg(ctx context.Context, msg *model.MsgData) (*model.SendMsgResp, error)
    SendSimpleMsg(ctx context.Context, msg *model.MsgData) (*model.SendMsgResp, error)
    BatchSendMsg(ctx context.Context, recvIDList []string, msg *model.MsgData) (*model.SendMsgResp, error)
    NewestSeq(ctx context.Context) (maxSeqs, minSeqs map[string]int64, err error)
    PullMsgBySeq(ctx context.Context, userID string, seqRanges []*model.SeqRange, order int32) (map[string]*model.PullMsgsResult, error)
    RevokeMsg(ctx context.Context, conversationID string, seq int64) error
    MarkMsgsAsRead(ctx context.Context, conversationID string, seqs []int64) error
    MarkConversationAsRead(ctx context.Context, conversationID string, hasReadSeq int64, seqs []int64) error
    GetConversationsHasReadAndMaxSeq(ctx context.Context, conversationIDs []string, returnPinned bool) (*model.SeqInfo, error)
    ClearConversationMsg(ctx context.Context, conversationIDs []string, syncOpt *model.DeleteSyncOpt) error
    DeleteMsgs(ctx context.Context, conversationID string, seqs []int64, syncOpt *model.DeleteSyncOpt) error
    DeleteMsgPhysicalBySeq(ctx context.Context, conversationID string, seqs []int64) error
    DeleteMsgPhysical(ctx context.Context, conversationIDs []string, timestamp int64) error
    SearchMsg(ctx context.Context, sendID, recvID string, contentType int32, sendTime int64, sessionType int32, pagination *model.RequestPagination) (*model.SearchMsgResult, error)
    CheckMsgIsSendSuccess(ctx context.Context, clientMsgID string) (*model.MsgSendStatus, error)
}
```

**步骤 2.4 — pkg/manager/group.go**

```go
type GroupMgr interface {
    CreateGroup(ctx context.Context, groupInfo *model.GroupInfo, memberUserIDs, adminUserIDs []string, ownerUserID string, sendMessage bool) (*model.GroupInfo, error)
    SetGroupInfo(ctx context.Context, groupInfo *model.GroupInfoForSet) error
    SetGroupInfoEx(ctx context.Context, groupID string, groupInfoEx *model.GroupInfoEx) error
    JoinGroup(ctx context.Context, groupID, reqMessage string, joinSource int32, inviterUserID, ex string) error
    QuitGroup(ctx context.Context, groupID, userID string) error
    GroupApplicationResponse(ctx context.Context, groupID, fromUserID, handledMsg string, handleResult int32) error
    TransferGroup(ctx context.Context, groupID, oldOwnerUserID, newOwnerUserID string) error
    GetGroupsInfo(ctx context.Context, groupIDs []string) ([]*model.GroupInfo, error)
    KickGroup(ctx context.Context, groupID string, kickedUserIDs []string, reason string, sendMessage *bool) error
    GetGroupMembersInfo(ctx context.Context, groupID string, userIDs []string) ([]*model.GroupMemberFullInfo, error)
    GetGroupMemberList(ctx context.Context, pagination *model.RequestPagination, groupID string, filter int32, keyword string) (total uint32, members []*model.GroupMemberFullInfo, err error)
    InviteUserToGroup(ctx context.Context, groupID, reason string, invitedUserIDs []string, sendMessage *bool) error
    GetJoinedGroupList(ctx context.Context, pagination *model.RequestPagination) (total int32, groups []*model.GroupInfo, err error)
    DismissGroup(ctx context.Context, groupID string, deleteMember bool, sendMessage *bool) error
    MuteGroupMember(ctx context.Context, groupID, userID string, mutedSeconds uint32) error
    MuteGroup(ctx context.Context, groupID string) error
    CancelMuteGroup(ctx context.Context, groupID string) error
    CancelMuteGroupMember(ctx context.Context, groupID, userID string) error
    SetGroupMemberInfo(ctx context.Context, members []*model.SetGroupMemberInfo) error
    GetGroupAbstractInfo(ctx context.Context, groupIDs []string) ([]*model.GroupAbstractInfo, error)
    GetIncrementalJoinGroups(ctx context.Context, userID, versionID string, version uint64) (*model.IncrementalGroups, error)
    GetIncrementalGroupMembers(ctx context.Context, groupID, versionID string, version uint64) (*model.IncrementalMembers, error)
    GetFullGroupMemberUserIDs(ctx context.Context, groupID string, idHash uint64) (*model.FullMemberIDs, error)
}
```

**步骤 2.5 — pkg/manager/relation.go**

```go
type RelMgr interface {
    AddFriend(ctx context.Context, fromUserID, toUserID, reqMsg, ex string) error
    AddFriendResponse(ctx context.Context, fromUserID, toUserID, handleMsg string, handleResult int32) error
    DeleteFriend(ctx context.Context, ownerUserID, friendUserID string) error
    GetFriendList(ctx context.Context, userID string, pagination *model.RequestPagination) (total int32, friends []*model.FriendInfo, err error)
    GetFriendApplyList(ctx context.Context, userID string, pagination *model.RequestPagination, handleResults []int32) (total int32, requests []*model.FriendRequest, err error)
    GetSelfFriendApplyList(ctx context.Context, userID string, pagination *model.RequestPagination, handleResults []int32) (total int32, requests []*model.FriendRequest, err error)
    SetFriendRemark(ctx context.Context, ownerUserID, friendUserID, remark string) error
    AddBlack(ctx context.Context, ownerUserID, blackUserID, ex string) error
    RemoveBlack(ctx context.Context, ownerUserID, blackUserID string) error
    GetBlackList(ctx context.Context, userID string, pagination *model.RequestPagination) (total int32, blacks []*model.BlackInfo, err error)
    IsFriend(ctx context.Context, userID1, userID2 string) (*model.FriendRelation, error)
    GetFriendIDs(ctx context.Context, userID string) ([]string, error)
    GetIncrementalFriends(ctx context.Context, userID, versionID string, version uint64) (*model.IncrementalFriends, error)
    GetIncrementalBlacks(ctx context.Context, userID, versionID string, version uint64) (*model.IncrementalBlacks, error)
    ImportFriend(ctx context.Context, ownerUserID string, friendUserIDs []string) error
}
```

**步骤 2.6 — pkg/manager/conversation.go**

```go
type ConvMgr interface {
    GetSortedConversationList(ctx context.Context, userID string, conversationIDs []string, pagination *model.RequestPagination) (*model.SortedConversationList, error)
    GetConversation(ctx context.Context, conversationID string) (*model.Conversation, error)
    GetConversations(ctx context.Context, conversationIDs []string) ([]*model.Conversation, error)
    GetAllConversations(ctx context.Context) ([]*model.Conversation, error)
    SetConversations(ctx context.Context, userIDs []string, conversationReq *model.ConversationReq) error
    GetFullConversationIDs(ctx context.Context, idHash uint64) (*model.FullConversationIDs, error)
    GetIncrementalConversations(ctx context.Context, versionID string, version uint64) (*model.IncrementalConversations, error)
    DeleteConversations(ctx context.Context, needDeleteTime int64, conversationIDs []string) error
    UpdateConversationsByUser(ctx context.Context, userID string, ex string) error
}
```

**完成标准**：六个 Manager 接口全部定义完毕，无实现代码，`go build ./pkg/manager/...` 通过（interface-only 包可以编译通过）。

---

### Phase 3: Manager 实现（逐个填充）

> 按依赖顺序实现，从底层向上：Auth → User → Msg → Relation → Group → Conversation

**步骤 3.1 — manager/auth.go 实现**
- 直接对应 4 个 /auth HTTP 接口
- TokenStore 联动：GetUserToken 成功后自动存入 TokenStore

**步骤 3.2 — manager/user.go 实现**
- 14 个 /user HTTP 接口
- 无复杂同步逻辑，纯 CRUD

**步骤 3.3 — manager/msg.go 实现 + sync.go**
- 最复杂的 Manager，核心是 `SeqSyncEngine`
- SeqSyncEngine 需要本地状态存储（依赖 storage 包）
- WS 推送 → OnNewMessage → 触发增量更新本地 seq
- 实现 `sync.go`：
  - `FullSync()`: newest_seq → pull_msg_by_seq → 写本地存储
  - `IncrementalSync()`: 对比本地 seq 与服务端，补齐缺口
  - 重连后自动触发 FullSync

**步骤 3.4 — manager/relation.go 实现**
- 15 个 /relation HTTP 接口
- 增量同步：get_incremental_friends / get_incremental_blacks

**步骤 3.5 — manager/group.go 实现**
- 22 个 /group HTTP 接口，最多的一个
- 增量同步：get_incremental_join_groups / get_incremental_group_members

**步骤 3.6 — manager/conversation.go 实现**
- 13 个 /conversation HTTP 接口
- 增量同步：get_incremental_conversations

**完成标准**：六个 Manager 全部有可用实现，transport 层 mock 测试覆盖每个 Manager 的核心路径。

---

### Phase 4: Client Facade + Storage + Events 桥接

**步骤 4.1 — pkg/storage**
- `storage.go`: Storage 接口（定义本地持久化抽象，供 SeqSyncEngine 和本地缓存使用）
- `sqlite.go`: SQLite 实现（gorm + glebarez/sqlite，pure-Go 驱动）
- `wasm_indexeddb.go`: IndexedDB 实现（后续 wasm 阶段补充）

**步骤 4.2 — pkg/client/client.go**
- `Client` 接口：聚合六个 Manager
- 默认实现：`DefaultClient` 持有 `HTTPClient`, `WSClient`, `TokenStore`, 六个 Manager, `Storage`
- 生命周期：
  ```
  NewClient(cfg *Config) → 构造
  Init()                 → 获取 Admin Token → 初始化状态
  Login(userID, platformID) → 获取 User Token → 启动 WS 连接 → SeqSyncEngine.FullSync()
  Logout()               → WS Disconnect → 清理本地状态
  ```
- `Config` 结构：baseURL, wsURL, adminUserID, adminSecret, logLevel, storageDir

**步骤 4.3 — Events 桥接层**
- 将 WS EventDispatcher 的 channel 输出桥接到 Client 层的事件订阅 API
- `client.go` 暴露 `Subscribe(EventType) (<-chan Event, UnsubscribeFunc)`
- gomobile 封装层后续把 channel 适配为原生回调

**完成标准**：`NewClient(cfg).Init().Login()` 完整跑通，WS 连接 + Seq 全量同步 + 消息推送。

---

### Phase 5: 封装编译（暂缓）

> 等 Phase 1-4 全部完成且测试充分后再动手。

| 平台 | 入口 | 产物 | 命令 |
|---|---|---|---|
| iOS | — | .framework | `gomobile bind -target=ios ./pkg/client` |
| Android | — | .aar | `gomobile bind -target=android -o goim-sdk.aar ./pkg/client` |
| Windows | — | .dll | CGO 编译 + 导出 C 函数 |
| macOS | — | .dylib | 同上 |
| Linux | — | .so | 同上 |
| Web | `wasm/cmd/main.go` | .wasm | `GOOS=js GOARCH=wasm go build -o goim.wasm ./wasm/cmd` |

---

## 6. SDKError 错误模型

遵循你的偏好：**剥离 rpc/grpc 前缀，直接返回原始业务错误**。

```go
type SDKError struct {
    Code        int32   // 业务错误码（0=成功）
    Msg         string  // 原始业务消息
    OperationID string  // 请求追踪 ID
}

func (e *SDKError) Error() string {
    // 直接返回 Msg，不添加 "rpc error: ..." 前缀
    return e.Msg
}

// Unwrap 支持 errors.Is / errors.As
func (e *SDKError) Unwrap() error { return nil }
```

**错误码常量**（文档 5.2 节全部覆盖）：

| 范围 | 分类 | 示例 |
|---|---|---|
| 0 | 成功 | `SuccessCode = 0` |
| 500 | 通用 | `ErrCodeServerInternalError = 500` |
| 1001-1050 | 参数/权限 | `ErrCodeArgsError = 1001`, `ErrCodeNoPermissionError = 1002` |
| 1101-1102 | 用户 | `ErrCodeUserIDNotFoundError = 1101` |
| 1201-1210 | 群组 | `ErrCodeGroupIDNotFoundError = 1201`, `ErrCodeGroupMutedError = 1208` |
| 1301-1305 | 关系链 | `ErrCodeBlockedByPeer = 1302` |
| 1401-1404 | 消息 | `ErrCodeMsgAlreadyRevoke = 1404` |
| 1501-1507 | Token | `ErrCodeTokenExpiredError = 1501`, `ErrCodeTokenKickedError = 1506` |
| 1601-1605 | 连接 | `ErrCodeConnOverMaxNumLimit = 1601` |
| 90001-90007 | 底层 | `ErrCodeInternalError = 90001`, `ErrCodeDatabaseError = 90002` |

---

## 7. 通用枚举常量

```go
// SessionType
const (
    SessionTypeC2C  = 1  // 单聊
    SessionTypeGroup = 2  // 群聊
)

// MsgFrom
const (
    MsgFromUser   = 100  // 用户消息
    MsgFromSystem = 200  // 系统通知
)

// RecvMsgOpt / GlobalRecvMsgOpt
const (
    RecvMsg           = 0  // 接收
    NotRecvMsg        = 1  // 不接收
    RecvMsgNotNotify  = 2  // 接收不推送
)

// RoleLevel (群成员角色)
const (
    RoleMember      = 10
    RoleAdmin       = 20
    RoleOwner       = 50
)

// PullOrder
const (
    PullOrderAsc  = 0  // 升序
    PullOrderDesc = 1  // 降序
)

// TokenStatus
const (
    NormalToken    = 0
    InValidToken   = 1
    KickedToken    = 2
    ExpiredToken   = 3
)
```

---

## 8. EventType 枚举（WS 推送）

完整覆盖文档 10.2 节的 24 种事件：

| EventType | Proto 消息 | 触发时机 |
|---|---|---|
| `NewMsg` | MsgData | 收到新消息 |
| `RevokeMsg` | RevokeMsgTips | 消息被撤回 |
| `DeleteMsgs` | DeleteMsgsTips | 消息被删除 |
| `MarkAsRead` | MarkAsReadTips | 对端标记已读 |
| `ConversationUpdate` | ConversationUpdateTips | 会话属性变更 |
| `ConversationDelete` | ConversationDeleteTips | 会话被删除 |
| `FriendApplication` | FriendApplicationTips | 好友申请 |
| `FriendAdded` | FriendAddedTips | 好友关系建立 |
| `FriendDeleted` | FriendDeletedTips | 好友关系解除 |
| `FriendInfoChanged` | FriendInfoChangedTips | 好友备注/置顶变更 |
| `BlackAdded` | BlackAddedTips | 被加入黑名单 |
| `BlackDeleted` | BlackDeletedTips | 被移出黑名单 |
| `GroupCreated` | GroupCreatedTips | 群组创建 |
| `GroupInfoSet` | GroupInfoSetTips | 群名称/公告变更 |
| `JoinGroupApplication` | JoinGroupApplicationTips | 入群申请 |
| `GroupApplicationAccepted` | — | 入群申请通过 |
| `GroupApplicationRejected` | — | 入群申请拒绝 |
| `MemberEnter` | MemberEnterTips | 成员主动加入 |
| `MemberInvited` | MemberInvitedTips | 被邀请入群 |
| `MemberQuit` | MemberQuitTips | 成员主动退出 |
| `MemberKicked` | MemberKickedTips | 成员被踢出 |
| `GroupOwnerTransferred` | GroupOwnerTransferredTips | 群主转让 |
| `GroupDismissed` | GroupDismissedTips | 群组解散 |
| `GroupMemberMuted` | GroupMemberMutedTips | 成员禁言 |
| `GroupMemberCancelMuted` | GroupMemberCancelMutedTips | 成员解禁 |
| `GroupMuted` | GroupMutedTips | 全员禁言 |
| `GroupCancelMuted` | GroupCancelMutedTips | 取消全员禁言 |
| `UserInfoUpdated` | UserInfoUpdatedTips | 用户资料变更 |
| `UserStatusChange` | UserStatusChangeTips | 订阅用户上下线 |
| `ForceLogout` | — | Token 被踢下线（code=1506） |

---

## 9. 本地存储设计（storage 包）

### 为什么需要本地存储

Seq 同步引擎和增量/全量同步需要本地持久化：
- 本地已知的最大 seq → 决定从哪里开始拉
- 本地 version / versionID → 决定增量同步起点
- 本地 idHash → 决定是否触发全量同步

### Storage 接口

```go
type Storage interface {
    // Seq 相关
    SaveLocalMaxSeq(conversationID string, seq int64) error
    GetLocalMaxSeq(conversationID string) (int64, error)
    GetAllLocalMaxSeqs() (map[string]int64, error)

    // 版本相关
    SaveVersion(storageKey, versionID string, version uint64) error
    GetVersion(storageKey string) (versionID string, version uint64, err error)

    // ID Hash 相关
    SaveIDHash(storageKey string, idHash uint64) error
    GetIDHash(storageKey string) (uint64, error)

    // 全量消息缓存（可选，用于消息去重）
    SaveMsg(conversationID string, msg *model.MsgData) error
    GetMsg(conversationID, clientMsgID string) (*model.MsgData, error)
    HasMsg(clientMsgID string) (bool, error)

    // 会话/好友/群的本地缓存（可选）
    SaveConversation(conv *model.Conversation) error
    GetConversation(conversationID string) (*model.Conversation, error)

    // 生命周期
    Close() error
    Clear() error  // 退出登录时调用
}
```

### 存储实现选择

| 平台 | 实现 | 理由 |
|---|---|---|
| iOS/Android/桌面 | `gorm` + `glebarez/sqlite` | ORM 省代码，底层 pure-Go sqlite，gomobile 直接可用 |
| Web/wasm | IndexedDB | wasm 下 SQLite 不可用，IndexedDB 是浏览器原生 |
| 单元测试 | 内存实现 | 零依赖、快速 |

---

## 10. 测试策略

| 测试类型 | 工具 | 覆盖目标 |
|---|---|---|
| 单元测试 | `testing` + `testify/mock` | model 序列化、transport 中间件、dispatcher、seq 引擎 |
| 接口测试 | httptest / net/http/httptest | HTTPClient 的 mock 服务测试 |
| 集成测试 | 本地 mock 服务 | Client 完整生命周期：Init → Login → WS → SeqSync → SendMsg → Logout |
| 平台编译 | gomobile build + wasm build | Phase 5 阶段验证编译通过 |

### Mock 策略

```go
// HTTPClient mock
type MockHTTPClient struct { ... }
func (m *MockHTTPClient) Post(ctx context.Context, path string, body, resp interface{}) error {
    // 根据 path 返回预定义的响应或错误
}

// WSClient mock
type MockWSClient struct { ... }
func (m *MockWSClient) Connect(ctx context.Context, url string, headers map[string]string) error {
    // 连接成功后，从预设队列推送事件到 dispatcher
}

// Storage mock (内存实现)
type MemoryStorage struct { ... }
```

---

## 11. Makefile 目标

```makefile
.PHONY: build test lint vet clean

build:      # go build ./...
test:       # go test ./pkg/... -v -race
lint:       # staticcheck ./...
vet:        # go vet ./...
clean:      # rm -f *.wasm *.a *.so *.dylib *.dll

# Phase 5 目标（后续启用）
gomobile-ios:       # gomobile bind -target=ios ./pkg/client
gomobile-android:    # gomobile bind -target=android ./pkg/client
wasm:                # GOOS=js GOARCH=wasm go build -o goim.wasm ./wasm/cmd
```

---

## 12. 依赖清单

```
github.com/google/uuid           # clientMsgID / operationID 生成
github.com/gorilla/websocket     # WebSocket
gorm.io/gorm                     # ORM
github.com/glebarez/sqlite       # pure-Go SQLite 驱动（gomobile/wasm 均可用）
go.uber.org/zap                  # 日志（高性能结构化）
github.com/PaperMan11/goim/pkg/protocol  # proto 定义（镜像）
github.com/stretchr/testify      # 测试 mock + assert（仅 test 依赖）
```

---

## 13. 开发顺序总览

```
Phase 1  model → events → transport     (基础层，零 Manager 代码)
Phase 2  六个 Manager interface 骨架     (接口先行，锁定边界)
Phase 3  逐个实现 Manager               (Auth→User→Msg→Relation→Group→Conv)
Phase 4  Storage → Client Facade → Events 桥接   (打通全链路)
Phase 5  gomobile / cgo / wasm 编译输出  (暂缓)
```

**每个 Phase 的完成判定**：编译通过 + 对应单元测试覆盖核心路径 + `go vet` 无告警。

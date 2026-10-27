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
| 语言 | Go | 1.26+ (自动升级) | gomobile / cgo / wasm 均原生支持 |
| HTTP | `net/http` | 标准库 | 无外部依赖，重试 / token 刷新逻辑内嵌 HTTPClient |
| JSON | `encoding/json` | 标准库 | gomobile 对标准库 json 支持好 |
| WebSocket | `github.com/gorilla/websocket` | v1.x | 成熟稳定、API 完善、gomobile 社区广泛验证过 |
| UUID | `pkg/utils/uuidx` (封装 `google/uuid`) | v1.x | 统一封装，所有 UUID 生成走 `uuidx.New()` |
| 并发模型 | `pkg/utils/run` (actor-runner) | — | 替代手动 stopCh + WaitGroup，任一 actor 退出自动 interrupt 其余 |
| 对象池 | **不使用 sync.Pool** | — | 客户端单连接消息量小，池化收益为零，反而引入 use-after-free 风险；Request/Response 由 GC 管理 |
| 错误模型 | 自定义 `SDKError{Code, Msg, OperationID}` | — | **剥离 rpc/grpc 前缀**，`Error()` 直接返回原始 Msg |
| 日志 | `pkg/logger` 接口 + zap 适配器 | — | printf 风格（`Debugf/Infof/Warnf/Errorf`），方便替换 |
| 日志实现 | `go.uber.org/zap` | v1.x | 高性能，`SugaredLogger` 天然适配 printf 风格 |
| Token 存储 | `sync.RWMutex` + 接口抽象 | 标准库 | 按 `(userID, platformID)` 存储，Get 过期分支写锁内复查避免竞态 |
| 传输层编解码 | `pkg/transport/encoder` 工厂 | — | json / gob 等，按 SDKType 选 |
| 传输层压缩 | `pkg/transport/compressor` 工厂 | — | none / gzip / zlib，两端对齐 |
| 通用 Pub/Sub | `pkg/utils/eventbus` | — | 完全通用的 channel-based pub/sub，未来 manager/client 内部复用 |
| 本地存储 ORM | `gorm.io/gorm` | v1.x | 表结构少（seq/version 等），gorm 省 CRUD 代码、迁移方便 |
| 本地存储驱动 | `github.com/glebarez/sqlite` | v1.x | **pure-Go** SQLite 驱动，基于 modernc，gomobile 和 wasm 均可用 |

> **gorm + sqlite 注意事项**：
> - gomobile 不支持 Go 泛型，gorm v2 的泛型 API（`gorm.G[T]` 等）不能用，全部用非泛型 API
> - wasm 场景下 sqlite 不可用，storage 接口的 IndexedDB 实现独立于 gorm
> - 单元测试可直接用 `gorm.io/driver/sqlite` 的内存模式（`file::memory:?cache=shared`）

### gomobile 限制与应对

| 限制 | 影响 | 应对 |
|---|---|---|
| 不支持跨语言 channel | event dispatch 无法直接暴露 channel | 封装层做 channel → 原生回调桥接（iOS block / Android lambda） |
| 回调签名限制 | gomobile 只支持基本类型参数 | 事件 payload 序列化为 JSON string 传递 |
| 不支持泛型 | Go 1.18+ 泛型在 gomobile 下不可用 | 全部用具体类型 + interface{} 兜底（eventbus 未使用泛型） |

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
│  ┌────────────────────────────────────────────────────┐     │
│  │ MessageTransport 接口                               │     │
│  │   Connect / Disconnect / IsConnected / Status       │     │
│  │   SendRequest / SendRequestAndWait / Router        │     │
│  │                                                    │     │
│  │ baseTransport (嵌入基类)                            │     │
│  │   run.Group actor 管理 / 重连循环 / 状态机          │     │
│  │                                                    │     │
│  │ DefaultWSClient     (未来: DefaultTCPClient)        │     │
│  │   WS 读写分离 · encoder/compressor · 双向心跳       │     │
│  └────────────────────────────────────────────────────┘     │
│  HTTPClient  (+ 默认实现)  ·  MessageRouter  ·  TokenStore  │
│  encoder 工厂 · compressor 工厂 · protocol (Request/Response)│
├─────────────────────────────────────────────────────────────┤
│                      pkg/model                               │
│  DTO + 枚举常量 + 错误码 + SDKError                          │
└─────────────────────────────────────────────────────────────┘

辅助包：
  pkg/events        EventType 枚举 + Event 结构（WS 推送事件类型定义）
  pkg/logger        Logger 接口 + zap 适配器（printf 风格）
  pkg/utils/run     actor-runner 并发模型（baseTransport 用）
  pkg/utils/eventbus 通用 channel-based pub/sub（未来 manager/client 复用）
  pkg/utils/uuidx   UUID 统一封装
  pkg/transport/encoder   传输层编解码工厂
  pkg/transport/compressor 传输层压缩工厂
```

### 3.2 依赖方向（严格单向）

```
client → manager → transport → model
                 ↓
              pkg/events   (事件类型定义)
                 ↓
              model
```

- `model` 是最底层，**零业务依赖**
- `transport` 依赖 `model`、`logger`、`utils/run`、`utils/uuidx`、`transport/encoder`、`transport/compressor`
- `manager` 依赖 `transport`、`model`、`events`、`logger`
- `client` 依赖所有层
- 所有层**不反向依赖**，同层之间不互相依赖

### 3.3 核心子系统

#### A. HTTPClient

```
HTTPClient.Post(ctx, path, body, resp)
    │
    ├─ 1. operationID 自动生成 ("sdk:" + uuidx.New())
    ├─ 2. Token 注入 Header（从 TokenStore.Get 取）
    ├─ 3. 发送 POST，统一解析 {code, msg, data}
    │      ├─ code == 0 → Unmarshal data → 返回
    │      ├─ code in [1501, 1504] → Token 过期 → 刷新 → 重试一次
    │      └─ code != 0 → 返回 SDKError
    └─ 4. 网络层重试（指数退避 + jitter，最多 3 次）
```

#### B. WebSocket 协议链路（完全对齐服务端）

```
写流程:
  proto.Marshal(业务对象)
    → Request.Data (bytes)
    → encoder.Marshal(Request)
    → compressor.Compress
    → sendCh 投递
    → writeLoop goroutine 串行消费
    → conn.WriteMessage(BinaryMessage, ...)

读流程（完全对称）:
  conn.ReadMessage(BinaryMessage, ...)
    → compressor.Decompress
    → encoder.Unmarshal(Response)
    → MessageRouter.Route(resp)
        ├─ OperationID 命中 pending → SendRequestAndWait 等待方
        └─ ReqIdentifier 命中 PushHandler → 同步调用 handler
```

#### C. WebSocket 连接 & 重连

```
Connect(ctx):
  1. 参数校验 → 默认值填充 (SDKType=go, Compression=none, OperationID=uuid)
  2. url.Parse + encodeQuery → wsURL 拼接认证参数
     (userID, token, platformID, deviceID, operationID, sendResponse,
      isBackground, compression, sdkType, sdkVersion)
  3. dialWS → gorilla/websocket.DialContext
  4. setState(Connected) → startActors(readLoop, writeLoop)

连接断开（被动）:
  readLoop / writeLoop 任一退出 → run.Group.interrupt → 全部 actor 退出
  → actorsExited() → failAllPending() → 指数退避重连 (1s→30s)
  → 重连成功 setState(Connected) → startActors + OnReconnected 回调

心跳（双向）:
  客户端→服务端: writeLoop 里 30s PingMessage
  服务端→客户端: SetPingHandler 自动回复 Pong + 续 read deadline
```

#### D. 数据同步引擎（✅ Phase 4.5 已实现）

```
消息 Seq 同步（SeqSyncEngine，pkg/sync/msg.go）:
  登录后/重连后:
    newest_seq() → 各会话服务端 maxSeq/minSeq
    与本地 local_seqs 对比 → gap = [localMax+1, serverMax]
    gap > 0 → pull_msg_by_seq 分批拉取 → 入库 → 前移游标

运行时增量:
  WS 推送新消息 → PushHandler(2001) → MsgData 入库 + local_seqs 前移

实体版本同步（EntitySyncEngine，pkg/sync/entity.go）:
  好友/黑名单/群/群成员/会话 五类，version + versionID 游标增量，
  full/IDHash 不匹配 → 全量回退（详见 Phase 4.5.4）

事件: 全部同步完成后 Publish(EventSeqSynced)；EntityLoginSuccess 在同步链路完成后才发布
```

#### E. 同步请求-响应（MessageRouter）

```
SendRequestAndWait(ctx, req):
  1. 生成 OperationID (uuid)，注册到 router.waitResponse
  2. SendRequest(ctx, req) 发出
  3. select {
       case resp := <-ch:     → ErrCode==0 返回 resp，否则转 SDKError
       case <-ctx.Done():     → router.cancelWait → 返回 ctx.Err
       case <-15s 超时:       → router.cancelWait → 返回 timeout
     }
  4. Connection 断开 → failAllPending → 所有等待方收到 nil → notConnectedError
```

---

## 4. 包结构

```
goim-sdk-core/
├── README.md                    # 项目入口
├── docs/
│   ├── DEVELOPMENT.md           # 本文档
│   └── INTEGRATION_TEST_PLAN.md # 服务端对接测试方案
├── go.mod                       # module: github.com/PaperMan11/goim-sdk-core
├── go.sum
│
├── pkg/
│   ├── model/                   # ✅ 已完成 — DTO + 枚举 + 错误码 + SDKError
│   │   ├── user.go              # UserInfo, PublicUserInfo, OnlineStatus...
│   │   ├── group.go             # GroupInfo, GroupMemberFullInfo...
│   │   ├── msg.go               # MsgData, ChatLog, PullMsgsResult...
│   │   ├── conversation.go      # Conversation, MsgInfo, SeqInfo...
│   │   ├── relation.go          # FriendInfo, BlackInfo, FriendRequest...
│   │   ├── common.go            # RequestPagination, DeleteSyncOpt...
│   │   ├── error.go             # SDKError + 全部错误码常量
│   │   └── const.go             # 枚举常量 (sessionType, msgFrom, recvMsgOpt, roleLevel...)
│   │
│   ├── events/                  # ✅ 已完成 — WS 推送事件类型定义
│   │   ├── event.go             # EventType 枚举 + Event 通用结构
│   │   ├── msg.go               # RevokeMsgTips, MarkAsReadTips...
│   │   ├── group.go             # GroupCreatedTips, MemberEnterTips...
│   │   ├── relation.go          # FriendAddedTips, BlackAddedTips...
│   │   ├── conversation.go      # ConversationUpdateTips...
│   │   └── user.go              # UserInfoUpdatedTips...
│   │
│   ├── logger/                  # ✅ 已完成 — 通用日志接口
│   │   ├── logger.go            # Logger 接口 (printf 风格) + NopLogger
│   │   └── zap.go               # zap 适配器 (ZapLogger)
│   │
│   ├── transport/               # ✅ Phase 1 核心 — HTTP + WS + Router + Auth
│   │   ├── transport.go         # MessageTransport 接口 + baseTransport 嵌入基类
│   │   ├── protocol.go          # Request/Response (对齐服务端) + WS 协议常量
│   │   ├── router.go            # MessageRouter (按 ReqIdentifier/OperationID 路由)
│   │   ├── ws.go                # DefaultWSClient (读写分离 + encoder/compressor + 双向心跳)
│   │   ├── http.go              # DefaultHTTPClient (token/opID 注入 + 错误解析 + 重试)
│   │   ├── auth.go              # TokenStore 接口 + 默认实现 (Get 过期写锁内复查)
│   │   ├── encoder/             # 传输层编解码工厂 (json/gob)
│   │   ├── compressor/          # 传输层压缩工厂 (none/gzip/zlib)
│   │   └── *_test.go            # 单元测试 (5 文件, 33 用例)
│   │
│   ├── utils/
│   │   ├── run/                 # actor-runner (baseTransport 并发模型)
│   │   ├── eventbus/            # 通用 channel-based pub/sub
│   │   ├── uuidx/               # UUID 统一封装
│   │   ├── httpclient/          # 函数式 HTTP 客户端 (独立库，sdk 暂未直接用)
│   │   └── ...                  # hash, bcrypt, convert, timex 等工具
│   │
│   ├── manager/                 # ⏳ 待实现 — 六个业务 Manager
│   │   ├── auth.go
│   │   ├── user.go
│   │   ├── msg.go
│   │   ├── group.go
│   │   ├── relation.go
│   │   ├── conversation.go
│   │   └── sync.go              # SeqSyncEngine
│   │
│   ├── client/                  # ⏳ 待实现 — Facade 顶层 Client
│   │   └── client.go
│   │
│   └── storage/                 # ⏳ 待实现 — 本地存储抽象
│       ├── storage.go
│       ├── sqlite.go
│       └── wasm_indexeddb.go
│
├── internal/                    # utils 已吸收所有功能，internal 暂空
├── wasm/                        # Phase 5 目标
└── tests/                       # 集成测试 (Phase 4 目标)
```

---

## 5. 开发阶段与步骤

### Phase 1: 基础层 ✅ **已完成**

**1.1 初始化项目** ✅
- module: `github.com/PaperMan11/goim-sdk-core`
- `go.mod tidy` 拉齐依赖，toolchain 自动升级到 `go1.26.10`

**1.2 pkg/model** ✅
- 8 文件全部 DTO struct（纯 JSON tag）
- `SDKError{Code, Msg, OperationID}`：`Error()` 直接返回 Msg
- 错误码常量覆盖文档全部 30+ 个
- 枚举常量：sessionType / msgFrom / recvMsgOpt / roleLevel / PullOrder / TokenStatus

**1.3 pkg/events** ✅
- EventType 枚举（iota+1，30 个值）
- Event 结构：`{Type, Payload, RawPayload, Timestamp}`
- 5 文件各 Tip payload 结构体

**1.4 pkg/transport** ✅ （实际做的远超原计划）

| 原计划 | 实际实现 |
|--------|---------|
| `WSClient` 单体接口 | `MessageTransport` 通用接口 + `baseTransport` 嵌入基类，支持未来 TCP |
| EventDispatcher（EventType pub/sub） | MessageRouter（按 ReqIdentifier/OperationID 路由），pending 请求同步匹配 |
| 心跳 loop（单向 ping） | 双向心跳：SetPingHandler 自动回复 pong，SetPongHandler 续期 read deadline |
| 单 actor（混合读写） | 读写分离：readLoop + writeLoop 独立 goroutine，writeLoop 串行消费 sendCh |
| 无编解码/压缩 | encoder + compressor 工厂，WriteMessage(BinaryMessage, compressed) 完全对齐服务端 |
| Header 认证 | URL query 认证（10 字段，服务端统一定义） |
| `sync.Pool` 对象池 | **已移除**：客户端池化收益为零，引入所有权/use-after-free 风险 |
| 手动 stopCh + WaitGroup | run.Group actor-runner，任一退出自动 interrupt 其余 |
| 无 SendRequestAndWait | 同步请求-响应：注册 pending → 发送 → 等响应/ctx/超时，断连时 failAllPending 唤醒 |
| HTTP middleware 链 | 重试/刷新内嵌 HTTPClient（3 次指数退避 + jitter，Token 过期刷新后重试一次） |

**1.5 pkg/logger** ✅
- 独立 Logger 接口（printf 风格）+ NopLogger + zap 适配器
- transport 层零 zap import

**1.6 单元测试** ✅
- 5 测试文件，33 用例全部通过（3.27s）
- 覆盖：MessageRouter 路由逻辑、TokenStore 过期删除竞态、HTTPClient 错误解析/重试/token header、WS 端到端（httptest 起真服务端）、ConnectParams 默认值、baseTransport actor 生命周期、重连

---

### Phase 2: Manager 接口骨架 ✅ **已完成**

> 六个 Manager interface 全部定义完毕，`go build ./pkg/manager/...` 通过。

**步骤 2.1 — manager/auth.go**

```go
type AuthMgr interface {
    GetAdminToken(ctx context.Context, secret, userID string) (*TokenResult, error)
    GetUserToken(ctx context.Context, userID string, platformID int32) (*TokenResult, error)
    ParseToken(ctx context.Context, token string) (*TokenInfo, error)
    ForceLogout(ctx context.Context, userID string, platformID int32) error
}
```

**步骤 2.2 — manager/user.go**

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

**步骤 2.3 — manager/msg.go**

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

**步骤 2.4 — manager/group.go**

```go
type GroupMgr interface {
    CreateGroup(ctx context.Context, groupInfo *model.GroupInfo, memberUserIDs, adminUserIDs []string, ownerUserID string, sendMessage bool) (*model.GroupInfo, error)
    SetGroupInfo(ctx context.Context, groupInfo *model.GroupInfoForSet) error
    SetGroupInfoEx(ctx context.Context, groupID string, groupInfoEx *model.GroupInfoEx) error
    JoinGroup(ctx context.Context, groupID, reqMessage string, joinSource int32, inviterUserID, ex string) error
    QuitGroup(ctx context.Context, groupID, userID string) error
    GroupApplicationResponse(ctx context.Context, groupID, fromUserID, handledMsg string, handleResult int32) error
    TransferGroup(ctx context.Context, groupID string, oldOwnerUserID, newOwnerUserID string) error
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

**步骤 2.5 — manager/relation.go**

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

**步骤 2.6 — manager/conversation.go**

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

**完成标准**：六个 Manager 接口全部定义完毕，`go build ./pkg/manager/...` 通过。

---

### Phase 2.5: Client 接口骨架 + 回调监听体系 ✅ **已完成**

> Manager 接口锁定后，先把 Client 层统一入口和事件监听 API 定义出来。
> 后续 Manager 实现时就知道要派发哪些事件，避免边写边改监听接口。

**步骤 2.5.1 — pkg/client/client.go 接口骨架**

```go
type Client interface {
    // —— 生命周期 ——
    Login(ctx context.Context, userID string, platformID int32) error
    Logout(clearData bool) error
    Connect(ctx context.Context) error
    Disconnect() error
    Status() transport.TransportStatus

    // —— Manager 聚合 ——
    Auth() manager.AuthMgr
    User() manager.UserMgr
    Msg() manager.MsgMgr
    Group() manager.GroupMgr
    Rel() manager.RelMgr
    Conv() manager.ConvMgr

    // —— 事件订阅（统一走 eventbus）——
    Subscribe(eventType events.EventType) (<-chan events.Event, eventbus.UnsubscribeFunc)
    SubscribeAll() (<-chan events.Event, eventbus.UnsubscribeFunc)

    // —— 便捷生命周期 Handler ——
    OnConnect(h func())                 // WS 连接成功
    OnDisconnect(h func(err error))     // WS 连接断开（被动）
    OnReconnect(h func())               // 自动重连成功
    OnLoginSuccess(h func())
    OnLoginFailed(h func(err error))
    OnForceLogout(h func())             // WSKickOnlineMsg / WSLogoutMsg 触发
}
```

> ⏳ **Phase 4.0b 扩展**：便捷回调从 6 个扩至 10 个（补 OnNewMessage / OnConversationChanged / OnMessageRevoked / OnConnecting，见 Phase 4.0b）。
> 原则：On* 只是高频路径语法糖，不扩成全量 42 个；完整能力始终由 Subscribe / SubscribeAll 承担。

**步骤 2.5.2 — 统一事件总线架构**

事件来源分四类，全部汇聚到 `pkg/utils/eventbus`：

| 事件来源 | 触发方式 | 接入点 |
|---|---|---|
| **WS 推送** | MessageRouter.PushHandler 注册 → proto.Unmarshal → Publish | DefaultClient 构造时集中注册 |
| **传输层状态** | transport 回调（OnReconnected / actorsExited） | 同 Phase 3 实现 DefaultClient 时注入 |
| **Manager 操作** | Manager 实现里 Publish（操作成功/失败） | Phase 3 逐个 Manager 实现里加 |
| **系统事件** | Storage / TokenStore / SeqSync 内部 | Phase 4 |

WS 推送 → EventType 映射（DefaultClient 构造函数里集中注册）：

```go
router.RegisterPush(WSPushMsg=2001,       → proto.Unmarshal → EventNewMsg)
router.RegisterPush(WSKickOnlineMsg=2002, → EventForceLogout)
router.RegisterPush(WSLogoutMsg=2003,     → EventForceLogout)
router.RegisterPush(WSSetBackgroundStatus=2004, → EventBackgroundStatusChanged)
router.RegisterPush(WSSubUserOnlineStatus=2005, → EventUserStatusChange)
```

**步骤 2.5.3 — EventType 扩展（events/event.go）**

现有 30 个 WS 推送事件外，补充 Client 层专用事件类型：

```go
// 传输层 / 生命周期
EventConnect              // WS 连接成功
EventDisconnect           // WS 连接断开
EventReconnect            // 重连成功
EventLoginSuccess         // 登录成功（Token 获取 + WS 连接 + Seq 同步）
EventLoginFailed          // 登录失败
EventTokenRefreshed       // Token 刷新成功
EventSeqSynced            // 全量同步完成
```

**完成标准**：`Client` 接口骨架编译通过 + EventType 枚举完整 + eventbus 订阅示例验证。

---

### Phase 3: Manager 实现（逐个填充）✅ **已完成**

> 按依赖顺序实现，从底层向上：Auth → User → Msg → Relation → Group → Conversation

**步骤 3.1 — manager/auth.go 实现**
- 直接对应 4 个 /auth HTTP 接口
- TokenStore 联动：GetUserToken 成功后自动存入 TokenStore
- GetUserToken 成功 → eventbus.Publish(EventLoginSuccess)
- Refresh token 成功 → eventbus.Publish(EventTokenRefreshed)
- ForceLogout 成功 → MessageRouter 标记连接需要清理

**步骤 3.2 — manager/user.go 实现**
- 14 个 /user HTTP 接口，纯 CRUD
- SubscribeUsersStatus / GetUsersOnlineStatus 需配合 WS（WSSubUserOnlineStatus / WSSetBackgroundStatus）
- WS 推送 UserStatusChange → eventbus.Publish(EventUserStatusChange)

**步骤 3.3 — manager/msg.go 实现 + sync.go**
- 最复杂的 Manager，核心是 SeqSyncEngine
- SendMsg / SendSimpleMsg / BatchSendMsg → WS (WSSendMsg) + SendRequestAndWait
- PullMsgBySeq / NewestSeq → WS (WSPullMsg / WSGetNewestSeq)
- **PushHandler(2001) 注册**：proto.Unmarshal → 映射 EventType → eventbus.Publish
  - 映射表在 DefaultClient 构造时定义，Manager 实现只负责解析和转事件
- SendMsg 成功 → eventbus.Publish(EventMsgSent)
- RevokeMsg 成功 → eventbus.Publish(EventMsgRevoked)（WS 也会推 EventRevokeMsg，两处都发）
- SeqSyncEngine → OnReconnected 触发 FullSync → 完成后 eventbus.Publish(EventSeqSynced)
- SeqSyncEngine 需要本地 Storage 配合（Phase 4 实现，Phase 3 先留接口）

**步骤 3.4 — manager/relation.go 实现**
- 15 个 /relation HTTP 接口
- GetIncrementalFriends / GetIncrementalBlacks 需本地 versionID 配合
- AddFriendResponse 成功 → eventbus.Publish(EventFriendAdded)
- DeleteFriend 成功 → eventbus.Publish(EventFriendDeleted)

**步骤 3.5 — manager/group.go 实现**
- 22 个 /group HTTP 接口
- GetIncrementalJoinGroups / GetIncrementalGroupMembers 需本地 versionID 配合
- CreateGroup 成功 → eventbus.Publish(EventGroupCreated)
- KickGroup 成功 → eventbus.Publish(EventGroupMemberKicked)

**步骤 3.6 — manager/conversation.go 实现**
- 9 个 /conversation HTTP 接口
- GetIncrementalConversations 需本地 versionID 配合
- SetConversations 成功 → eventbus.Publish(EventConversationCreated)
- DeleteConversations 成功 → eventbus.Publish(EventConversationDeleted)

---

### Phase 4: Storage + DefaultClient 完整实现 ✅ **已完成**

**步骤 4.1 — pkg/storage（✅ 全部完成，业务数据部分在 Phase 4.5 落地）**
- ✅ Storage 接口（GetMaxSeqs/SetMaxSeqs、GetVersion/SetVersion、Clear）+ MemoryStorage
- ✅ SQLite 实现（gorm + glebarez/sqlite）+ 业务数据表 → **Phase 4.5 完成**

**步骤 4.2 — pkg/client/client.go 完整实现**
- DefaultClient 聚合：HTTPClient、MessageTransport、TokenStore、六个 Manager、Storage、eventbus
- 构造时注册全部事件源：
  - MessageRouter.PushHandler × 5 个 WS 推送 → eventbus.Publish
  - baseTransport.OnReconnected → eventbus.Publish(EventReconnect)
  - actorsExited failAllPending → EventDisconnect
- 生命周期实现：
  ```
  NewClient(cfg)      → 构造 transport + manager 依赖
  Login(userID, pid)  → Auth.GetUserToken → TokenStore.Set → WS Connect → SeqSyncEngine.FullSync
                      → 成功 eventbus.Publish(EventLoginSuccess)
  Logout(clearData)   → MessageTransport.Disconnect → TokenStore.Delete
                      → clearData=true 时 Storage.Clear(owner)（默认保留，下次登录游标增量）
                      → eventbus.Publish(EventForceLogout)
  Subscribe(et)       → eventbus.Subscribe(et)
  OnConnect(h)        → eventbus.Subscribe(EventConnect) → 回调
  ```
- **OnConnect 触发时机**：WS `connect.OnConnected` 回调里 Publish，而非 transport.connect 方法成功返回。
  原因：SendRequestAndWait / PushHandler 注册后才能保证事件链完整。
- ✅ **EventLoginSuccess 发布时机已调整（Phase 4.5）**：Login 现在在 SeqSync + 五类 EntitySync 全部完成后才发布；
  同步失败 → Publish(EventLoginFailed) + 返回 error。重连后由 OnReconnected 钩子异步重跑 runSyncFlow。

**步骤 4.3 — Events 桥接层（Phase 3 已部分完成）**
- Manager 实现里操作成功 → eventbus.Publish（Phase 3 已嵌入）
- MessageRouter.PushHandler → proto.Unmarshal → EventType → eventbus.Publish
  - PushHandler 在 DefaultClient 构造函数里集中注册，映射表清晰可维护
- Subscribe / SubscribeAll 暴露给上层业务调用
- lifecycle handler（OnConnect/OnDisconnect 等）是 Subscribe 的语法糖

**Phase 4 完成判定**：

```
NewClient(cfg).Login(userID, platformID)
  → Auth.GetUserToken → TokenStore.Set → WS Connect → SeqSync → EventLoginSuccess
  
WS 推送消息
  → MessageRouter.Route → PushHandler(2001) → proto.Unmarshal → EventNewMsg → eventbus.Publish
  
上层业务
  → client.Subscribe(EventNewMsg) → 收到 chan 事件 → 业务处理
```

---

### Phase 4.0b: 高频便捷回调补齐 ✅ **已完成**

> **评估结论**（2026-10）：不扩成全量 42 个 On*（接口爆炸、与 Subscribe 语义重复），只补 IM 客户端 80% 场景必用的 4 个高频回调；
> 跨语言的"callback 全量契约"放到 Phase 5 的 call 层用单方法 dispatcher 解决（gomobile 不支持 Native 实现 Go 多方法 interface）。

> ⚠️ **实现中发现并修复的设计缺陷**（重要协议事实）：
> 原 handleMessagePush 用 multi-type probe（依次尝试 Unmarshal 到 10 种 proto 类型）——这是错的：
> proto 按字段编号解码，RevokeMsgTips 与 MsgData 前两个字段都是 string，Unmarshal 互串（撤回通知被误判成新消息）。
> 服务端真实协议（对齐 pkg/msgdispatcher / im-rpc msg notification.go）：**2001 推送唯一形态是 sdkws.MsgData**，
> 普通消息 ContentType ∈ [100,999]；系统通知 ContentType ≥ 1000，Tips 本体在
> `Content = JSON(NotificationElem{Detail: JSON(tips)})`。
> 已重写为 MsgData 信封解析 + ContentType 分流（24 种通知映射，对齐 goim pkg/protocol/constant），
> 高频 tips（撤回/删除/已读/用户状态/群创建/群信息/好友申请/用户信息）解码为具体 proto 类型，
> 其余 payload = Detail JSON string，RawPayload 保留原始 bytes。

**基建（transport 层 + events 层）**：

| 改动 | 说明 |
|------|------|
| events/event.go 加 `EventConnecting` | 连接中间态（connect 进行中 / 重连循环进入 dial 前），UI 可显示"连接中" |
| MessageTransport 加 `SetOnConnecting(func())` | 与 SetOnDisconnected / SetOnReconnected 同构；baseTransport 在 setState(Connecting) 处触发（锁外回调，避免与 Status() 互锁） |
| 传输层零 events import 约束不变 | client 层负责把钩子转成 eventbus.Publish |

**Client interface 新增 4 个便捷回调**：

| 方法 | 底层 | 说明 |
|------|------|------|
| `OnNewMessage(h func(msg *model.MsgData))` | Subscribe(EventNewMsg) | IM 最高频，manager.ConvProtoMsgData 做类型转换 |
| `OnConversationChanged(h func())` | Subscribe(EventConversationUpdate) | 会话列表 UI 刷新信号 |
| `OnMessageRevoked(h func(clientMsgID string))` | Subscribe(EventRevokeMsg) | 2101 通知 tips 解码后取 ClientMsgID |
| `OnConnecting(h func())` | Subscribe(EventConnecting) | 依赖上面的 EventConnecting 基建 |

**明确不做**：
- ❌ 全量 42 个 On* 清单 —— Subscribe/SubscribeAll 已是完整能力
- ❌ 多方法 Callback interface —— Phase 5 无法跨语言，改用单方法 dispatcher（见 Phase 5.3）

**完成判定**：✅ client 包 7 测试 PASS（OnConnecting/OnConnect/OnNewMessage/OnMessageRevoked/NotificationUnmapped 等，
mock transport 走真实 MessageRouter + 服务端真实推送形态构造）；`go build/vet ./pkg/...` 通过。

---

### Phase 4.5: 本地存储落地 + 数据同步引擎 ✅ **已完成**

> **为什么需要这个阶段**：服务端已有完整 data-sync 协议 —— `version + versionID` 增量同步、`IDHash` 全量回退（见服务端 docs/data-sync.md，10 个专用接口）。SDK 必须实现对应的本地缓存和同步引擎，否则上层业务拿不到离线数据、重连后状态不一致。
>
> Phase 4 只交付了 Storage 接口 + MemoryStorage（元数据级），本阶段补齐：业务数据持久化（SQLite）+ 双同步引擎（消息 Seq 同步 / 实体版本同步）。

#### 4.5.1 存储层结构（✅ 已落地）

```
pkg/storage/
  ├── storage.go        # Storage 接口（大幅扩展）+ MemoryStorage 全实现 ✅
  ├── sqlite.go         # SQLiteStorage（gorm + glebarez/sqlite，纯 Go 无 CGO，全平台可用）✅
  ├── models.go         # gorm 表模型（8 张表，见 4.5.2）✅
  └── sqlite_test.go    # 8 个单测（Seqs/Versions/Messages/Conversations/FriendsBlacks/GroupsMembers/ClearOwnerIsolation/FilePath）✅
```

- `ClientConfig.Storage` 注入优先；`SessionDataPath` 非空 → 内部建 SQLiteStorage；都空 → MemoryStorage
- IDHash 直接复用 `pkg/utils/hash.HashStringSet`（order-invariant），未单独建 idhash.go
- 所有表带 `OwnerUserID` 隔离（多账号共存，Clear 只清当前 owner）
- wasm：接口不变，Phase 5 换 IndexedDB 实现（见 §5.4）

#### 4.5.2 SQLite 表设计（8 张表）

| 表 | 联合主键 | 关键列 | 用途 |
|----|---------|--------|------|
| local_seqs | (owner_user_id) | max_seqs JSON, min_seqs JSON | NewestSeq 持久化（现有接口对齐） |
| versions | (owner_user_id, entity) | version_id, version | 五类实体同步游标 |
| conversations | (owner_user_id, conversation_id) | conv_type, recv_opt, unread, latest_msg_json, max_seq, min_seq, attached_info, version | 会话列表 |
| messages | (owner_user_id, client_msg_id) | server_msg_id, **seq**, conversation_id, sender_id, content_json, status, send_time | 消息缓存 |
| friends | (owner_user_id, friend_user_id) | remark, is_pinned, add_source | 好友 |
| blacks | (owner_user_id, black_user_id) | | 黑名单 |
| groups | (owner_user_id, group_id) | group_name, member_count, status | 加入的群 |
| group_members | (owner_user_id, group_id, user_id) | role_level, join_time, nickname | 群成员 |

关键索引：`messages(conversation_id, seq)` 唯一、`conversations(latest_msg_send_time DESC)`（排序会话列表）。

#### 4.5.3 SeqSyncEngine（消息同步，pkg/sync/msg.go）

```
初始化（登录后 / 重连后）:
  1. NewestSeq(WS 1001) → 服务端 maxSeq/minSeq（按会话）
  2. 与 Storage.local_seqs 对比 → 缺口 = [localMax+1, serverMax]
  3. gap > 0 → PullMsgBySeqList(WS 1002) 分批拉取（每批 ≤100 seq）
     → messages 表 upsert → local_seqs 逐批落盘（commit-last：游标写在入库后，
     失败重试只重拉未完成批次/会话）
  4. 单会话失败不中断其余会话（错误聚合返回）；全部成功才 Publish(EventSeqSynced)

运行时:
  WS 2001 推送 MsgData → seq > localMax → 直接入库 + local_seqs 前移
  （EventNewMsg 由 PushHandler 派发，Phase 4 已做；引擎只管持久化与游标）

重连后:
  OnReconnected → 重新走初始化流程做 gap 拉取（不再整库丢弃，§3.D 旧方案废弃）
```

#### 4.5.4 EntitySyncEngine（实体版本同步，pkg/sync/entity.go）

**五类实体 × 服务端接口**（/api/v1 前缀，对齐服务端 data-sync.md）：

| 实体 | 增量接口 | 全量 ID 校验接口 | 本地表 |
|------|---------|----------------|--------|
| 好友 | /relation/get_incremental_friends | /relation/get_full_friend_user_ids | friends |
| 黑名单 | /relation/get_incremental_blacks | （复用好友 ID 空间判定） | blacks |
| 加入的群 | /group/get_incremental_join_groups | /group/get_full_join_group_ids | groups |
| 群成员 | /group/get_incremental_group_members(+_batch) | /group/get_full_group_member_user_ids | group_members |
| 会话 | /conversation/get_incremental_conversations | /conversation/get_full_conversation_ids | conversations |

**协议**（model DTO：IncrementalFriends / IncrementalBlacks / IncrementalGroups / IncrementalMembers / IncrementalConversations）：
- 请求：`{versionID, version}`（✅ 已核实服务端 goim 源码：**无 syncCount 分批**，一次返回全部增量）
- 响应：`{versionID, version, insert[], update[], delete[], full}`

**同步算法（单实体，✅ 已实现）**：
```
1. Storage.GetVersion(entity) → (versionID, version)
2. 携带游标请求增量（一次返回全部变更，无需分批循环）
3. full == true（版本线变更）→ GetFull*IDs(本地IDHash) 校验：
   equal == true  → 本地 ID 集合与服务端一致（历史增量内容可信）→ 仅对齐游标
   equal == false → fetch-then-swap：分页全量拉取（仅收集，失败则本地旧数据不动）
                    → 清本地表 → 一次性写入 → 写新游标
                    （先收集后清表：拉取中途失败不产生本地列表空窗）
   黑名单例外：无 IDHash 校验接口 → full 时直接全量重拉换表
4. full == false：应用 insert/update/delete 三列表（upsert / 硬删）
5. SetVersion(entity, versionID, version) 写回游标
6. Publish 对应实体事件（EventFriendListSynced / EventBlackListSynced /
   EventGroupListSynced / EventGroupMemberSynced / EventConversationSynced）
```

**群成员特殊**：按群维度游标（groupID + version + versionID），`get_incremental_group_members_batch` 一次拉多群；先同步 `get_incremental_join_groups` 拿到摘要变更群（GroupAbstractInfo），再按需拉成员。

#### 4.5.5 触发时机矩阵

| 时机 | 动作 | 状态 |
|------|------|------|
| Login 成功（WS Connect 后） | SeqSyncEngine.Sync + EntitySyncEngine.SyncAll 顺序执行 → 全部完成才 Publish(EventLoginSuccess)；失败 → EventLoginFailed + error | ✅ |
| Reconnect（OnReconnected） | 异步重跑 runSyncFlow：SeqSync gap 拉取 + 五类实体带游标增量（通常 0 或小增量），失败仅 warn | ✅ |
| Logout | 默认仅 Disconnect + TokenStore.Delete（本地数据保留，下次登录走游标增量）；clearData=true 时额外 Storage.Clear(owner)（共享设备隐私/注销场景，下次登录 full 重拉） | ✅ |
| WS 2001 推送消息 | EventNewMsg 已派发（Phase 4.0b）；推送路径的消息入库 + 游标前移 | ⏳ 后续增强 |
| WS 实体 Tips（群变更/好友变更等 2001 Tips） | 标记实体脏 → 防抖合并 → 触发对应实体增量 | ⏳ 后续增强 |

> 注：推送路径未入库期间，消息以 SeqSync 引擎拉取为准（登录/重连触发），事件派发不受影响。

#### 4.5.6 完成判定 + 交付物（✅ 全部达成）

```
pkg/storage/sqlite.go + models.go   8 表 CRUD（:memory: SQLite 单测覆盖，8 用例 PASS）✅
pkg/sync/msg.go + entity.go         gap 拉取 / 游标增量 / IDHash 全量回退（mock 服务端 16 用例 PASS）✅
client 集成                          Login → SeqSync → EntitySync → EventLoginSuccess 全链路 ✅
                                     同步失败 → EventLoginFailed ✅
                                     Logout(clearData=false) → 数据保留 + TokenStore.Delete ✅

build / vet / 全量 go test ./pkg/... 全绿（含修复 hash 包缺失 test.txt fixture）
⏳ 集成验证（需真实服务端）：多账号隔离 Login(A)→Logout→Login(B)、断网重连 gap 拉取无丢消息
```

---

### Phase 5: 跨语言封装 ⏳

> 等 Phase 1-4 全部完成且测试充分后再动手。
>
> **为什么需要 call 层？** gomobile / cgo / wasm 只能传 Go 基本类型（string/int/bool/[]byte），不能传 Go channel、泛型、复杂 interface。Phase 1-4 的 Client 接口返回 `(*model.SendMsgResp, error)`、`<-chan events.Event`、interface{} 载荷——这些在跨语言边界都无法直接导出。
>
> **核心 SDK 和 call 层是两件事**：goim-sdk-core 定位是 Go 语言内部使用，所以保持类型安全的 Manager 接口；call 层是为跨语言边界准备的**薄反射适配层**，它不参与 core 层设计，Phase 5 时纯增量添加。

#### 5.1 架构总图

```
┌───────────────────────────────────────────────────────────┐
│              跨语言桥接层（平台边界）                        │
│  gomobile .framework / .aar   ·   cgo .dll / .dylib       │
│  wasm .wasm (syscall/js)                                  │
│                                                           │
│  ↓ 只能传 string / int / bool / []byte                    │
├───────────────────────────────────────────────────────────┤
│              pkg/call 反射调用内核                          │
│                                                           │
│  · call(method, argsJSON)        异步，回调 OnSuccess / OnError
│  · syncCall(method, argsJSON)    同步，返回 JSON string
│  · messageCall(method, argsJSON) 消息类方法，带进度回调
│                                                           │
│  反射分发:                                                 │
│    1. method string → Client 方法名查找                    │
│    2. argsJSON → 参数 struct 反序列化                      │
│    3. operationID / 登录态校验                              │
│    4. panic recover + 错误归一化                           │
│    5. 返回值 struct → JSON 序列化                           │
│                                                           │
│  ↓ Go 内部调用                                             │
├───────────────────────────────────────────────────────────┤
│             pkg/client DefaultClient                       │
│  Login / Logout / Connect / Disconnect / Status            │
│  Auth() / User() / Msg() / Group() / Rel() / Conv()        │
│  Subscribe / SubscribeAll / OnConnect / OnDisconnect       │
├───────────────────────────────────────────────────────────┤
│             pkg/manager 六个 Manager 实现                  │
├───────────────────────────────────────────────────────────┤
│             pkg/transport + pkg/storage                    │
├───────────────────────────────────────────────────────────┤
│             pkg/model DTO + pkg/events EventType           │
└───────────────────────────────────────────────────────────┘
```

#### 5.2 pkg/call 包设计

```
pkg/call/
  ├── call.go           # Dispatcher 主入口（反射分发）
  ├── converter.go      # 参数/返回值 ↔ JSON 编解码
  ├── middleware.go     # operationID 校验 / 登录态校验 / panic recover
  ├── error.go          # 统一错误归一化（SDKError → call error code + msg）
  └── handler.go        # 跨语言回调桥接（gomobile 闭包 ↔ Go channel）
```

**核心 API（全部是 gomobile 可绑定的基本类型签名）**：

```go
// Dispatcher 反射分发器（全局单例）
type Dispatcher struct {
    client *client.DefaultClient  // Phase 4 构造完成后注入
}

// call 异步调用：method="MsgMgr.SendMsg", argsJSON='{"sendID":"...","content":"..."}'
// gomobile 导出后变成：
//   iOS:    dispatcher.call(..., onSuccess: (String) -> Void, onError: (Int, String) -> Void)
//   Android: dispatcher.call(..., (String) -> Unit, (Int, String) -> Unit)
func (d *Dispatcher) call(method string, argsJSON string, onSuccess func(string), onError func(int32, string))

// syncCall 同步调用，返回 JSON 字符串（少数非事件型场景用）
func (d *Dispatcher) syncCall(method string, argsJSON string) (string, error)

// messageCall 消息发送专用，带进度回调
func (d *Dispatcher) messageCall(method string, argsJSON string,
    onSuccess func(string), onError func(int32, string), onProgress func(int32))

// SetClient 注入 DefaultClient（单例模式，call 层持有唯一 client 实例）
func (d *Dispatcher) SetClient(c *client.DefaultClient)

// Subscribe 跨语言订阅事件
// gomobile 导出后变成：
//   iOS:    dispatcher.subscribe(eventType, onEvent: (Event) -> Void) -> Unsubscriber
//   Android: dispatcher.subscribe(eventType, (Event) -> Unit) -> () -> Unit
//   Event 结构必须全是 gomobile 可绑定类型，payload 用 []byte / string
func (d *Dispatcher) Subscribe(eventType int32, onEvent func(eventJSON string)) func()

// —— SDK 生命周期（gomobile 导出的顶层 API 也可以直接调 DefaultClient）——

// Login / Logout / Connect / Disconnect 也可以直接导出为具体方法，
// 因为 gomobile 能直接绑定 func() / func(error error) 这种基本签名
func (d *Dispatcher) Login(userID string, platformID int32, onSuccess func(), onError func(int32, string))
func (d *Dispatcher) Logout(onSuccess func(), onError func(int32, string))
func (d *Dispatcher) Connect(onSuccess func(), onError func(int32, string))
func (d *Dispatcher) Disconnect(onSuccess func(), onError func(int32, string))
func (d *Dispatcher) Status() int32
```

**反射分发表**（call 层内部维护，method string → (receiver, methodName) 映射）：

| method string | receiver | 方法 | args struct | 返回 struct |
|--------------|----------|------|------------|------------|
| `"Login"` | Dispatcher | Login | - | - |
| `"AuthMgr.GetUserToken"` | AuthMgr | GetUserToken | `{userID, platformID}` | `{token, expireTimeSeconds}` |
| `"MsgMgr.SendMsg"` | MsgMgr | SendMsg | `MsgData` (全部是具体字段) | `SendMsgResp` |
| `"MsgMgr.NewestSeq"` | MsgMgr | NewestSeq | - | `{maxSeqs, minSeqs}` (JSON object) |
| `"MsgMgr.PullMsgBySeq"` | MsgMgr | PullMsgBySeq | `{userID, seqRanges, order}` | `{msgsByConv}` |
| `"GroupMgr.CreateGroup"` | GroupMgr | CreateGroup | `{groupInfo, memberIDs, adminIDs, ownerID, sendMessage}` | `GroupInfo` |
| ... | ... | ... | ... | ... |

**参数/返回值 struct 约束**：
- 全部用具体 struct（**不能用 interface{} / interface / 嵌套 interface 字段**）
- gomobile 不支持泛型，所有 slice 必须是具体类型：`[]string`、`[]*MsgData`（而不是 `[]interface{}`）
- map 允许（gomobile 可绑定 `map[string]string`）
- `interface{}` 字段用 `json.RawMessage` / `[]byte` / `string` 替代

**operationID / 登录态 / panic recover 中间件**：

```go
// middleware.go
func (d *Dispatcher) withMiddleware(method string, fn func() (interface{}, error)) (interface{}, error) {
    // 1. panic recover（反射调用中 panic 不能直接传到 C/ObjC/Swift）
    defer func() {
        if r := recover(); r != nil {
            err = fmt.Errorf("panic in %s: %v", method, r)
        }
    }()

    // 2. 登录态校验（方法白名单跳过：Login / ParseToken / Register）
    if !d.isLoggedIn() && !isAuthBypass(method) {
        return nil, ErrNotLoggedIn
    }

    // 3. 反射调用
    return fn()
}
```

#### 5.3 Subscribe 事件桥接

```
eventbus (core 层)                     call 层 handler
  chan events.Event    →  range 监听  →  event.Timestamp / event.Type / payload JSON
                                          → gomobile 闭包回调 onEvent(JSONString)
                                               ↓
                                         iOS Swift block / Android Kotlin lambda
```

**events.Event 在 gomobile 下的 JSON 结构**（不能直接 Export，因为 Payload 是 `interface{}`）：

```go
// call/event_payload.go 专门给 gomobile 导出的事件载荷
type EventPayload struct {
    Type      int32  `json:"type"`
    Timestamp int64  `json:"timestamp"`
    Payload   string `json:"payload,omitempty"` // 具体事件的 JSON 序列化，比如 MsgData
}
```

call 层内部做 `Event → EventPayload → JSON string`，跨语言边界只传 string。

**Native 侧注册形态 —— 单方法 CallBack dispatcher**（gomobile 硬约束：Native 无法实现 Go 多方法 interface，
所以不给 Native 暴露 42 个 On*，也不暴露多方法 Callback，而是一条注册通道覆盖全部事件）：

```go
// call/callback.go —— 唯一的跨语言回调契约（gomobile 可绑定：单方法 + 纯 string 参数）
type CallBack interface {
    // OnEvent 全部事件经此分发
    //   eventType: events.EventType 的字符串形式（如 "EventNewMsg"）
    //   payloadJSON: EventPayload 序列化结果（空串表示无载荷）
    OnEvent(eventType string, payloadJSON string)
}

// call 层暴露的注册 API
func (d *Dispatcher) RegisterCallback(cb CallBack) // 内部 = SubscribeAll → range → JSON → cb.OnEvent
```

iOS / Android 侧实现 CallBack 代理对象（Java interface / OC protocol）注册一次即可收全量事件；
按 eventType 字符串自行分发到各 UI 处理器。区分事件的枚举值清单随 SDK 文档生成。

#### 5.4 四平台桥接

##### iOS / Android（gomobile bind）

```bash
# iOS: .framework
gomobile bind -target=ios -o goim-sdk.framework -v ./pkg/call

# Android: .aar
gomobile bind -target=android -o goim-sdk.aar -v ./pkg/call
```

导出的接口全部用 **基本类型**（string / int32 / int64 / bool / []byte / map），gomobile 自动生成 ObjC / Java 绑定。

**Swift 端示例**：
```swift
let dispatcher = GoimSdkNewDispatcher()
dispatcher.login("user-123", platformID: 1,
    onSuccess: { print("login ok") },
    onError: { code, msg in print("login failed: \(code) \(msg)") })

dispatcher.call("MsgMgr.SendMsg", argsJSON: "{\"content\":\"hello\"}",
    onSuccess: { respJSON in print("sent: \(respJSON)") },
    onError: { code, msg in print("send failed: \(code) \(msg)") })

let unsub = dispatcher.subscribe(EventNewMsg) { eventJSON in
    print("new msg: \(eventJSON)")
}
```

##### PC 桌面（cgo）

```bash
# Windows DLL
CGO_ENABLED=1 GOOS=windows GOARCH=amd64 go build -buildmode=c-shared -o goim-sdk.dll -v ./cmd/cgo

# macOS dylib
CGO_ENABLED=1 GOOS=darwin GOARCH=amd64 go build -buildmode=c-shared -o goim-sdk.dylib -v ./cmd/cgo
```

导出 C 函数：

```c
// cmd/cgo/main.go 里用 //export 注释导出
extern void GoimSdkCall(const char* method, const char* argsJSON,
                        void (*onSuccess)(const char*),
                        void (*onError)(int32_t, const char*));
extern void GoimSdkLogin(const char* userID, int32_t platformID,
                        void (*onSuccess)(void),
                        void (*onError)(int32_t, const char*));
extern void GoimSdkSubscribe(int32_t eventType, void (*onEvent)(const char*));
```

##### Web（wasm + syscall/js）

```bash
GOOS=js GOARCH=wasm go build -o goim.wasm -v ./wasm/cmd/main.go
cp "$(go env GOROOT)/misc/wasm/wasm_exec.js" ./
```

**特殊约束**：
- Storage 不能用 SQLite（wasm 没有文件系统），用 IndexedDB（`go-proton-idb` 或 `syscall/js` 封装）
- `gorilla/websocket` 在 wasm 下自动降级为 `syscall/js.WebSocket`（gorilla 已内置支持）
- Logger 输出到 `console.log`（`syscall/js.Global().Get("console").Call("log", ...)`）
- glebarez/sqlite 在 wasm 下走 JS backend（pure-Go SQLite 有 wasm 支持）

**JS 调用入口**（暴露到 `globalThis`）：

```go
// wasm/cmd/main.go
func init() {
    // JS 端: globalThis.goimSdk.call("MsgMgr.SendMsg", argsJSON, (resp)=>{}, (code,msg)=>{})
    js.Global().Set("goimSdk", js.ValueOf(map[string]interface{}{
        "call":        js.FuncOf(callJS),
        "subscribe":   js.FuncOf(subscribeJS),
        "login":       js.FuncOf(loginJS),
        "logout":      js.FuncOf(logoutJS),
    }))
}
```

#### 5.5 gomobile 约束清单（call 层必须遵守）

| 特性 | gomobile 支持 | call 层处理方式 |
|------|--------------|----------------|
| Go channel | ❌ 不支持 | Subscribe 返回 Go func() 类型（gomobile 支持），内部监听 channel |
| 泛型 `<T>` | ❌ 不支持 | 全部用具体 slice：`[]*model.MsgData` 代替 `[]interface{}` |
| `interface{}` | ❌ 不支持作为返回值 | 跨语言边界只用具体 struct / string / []byte |
| closure / func 类型 | ✅ 支持 | `func(string)` / `func(int32, string)` 直接导出 |
| `map[string]string` | ✅ 支持 | 直接导出 |
| `map[string]*MsgData` | ❌ value 不能是 pointer struct | JSON string 传递 |
| 方法接收者是 interface | ⚠️ 不稳定 | call 层统一持有具体类型 `*DefaultMsgMgr`，不用 interface 作为反射 receiver |
| 多返回值 | ⚠️ 有限支持 | gomobile 只支持最多 2 个返回值，call 层内部可以更多，但导出方法要拍平 |
| `(T, error)` | ✅ 支持 | Go 惯用模式，gomobile 能正确映射 error |
| `context.Context` | ✅ 支持（作为参数） | call 层内部传 `context.Background()` 或带 timeout 的 context |
| goroutine | ✅ 支持 | call 层内部正常起 goroutine，回调切回主线程由桥接层负责 |

#### 5.6 Phase 5 完成判定

| 判定 | 验证方式 |
|------|---------|
| gomobile bind 无编译错误 | `gomobile bind -target=ios ./pkg/call` |
| iOS 能 build | Xcode 打开 .framework，写 Swift 代码 import 并调用 Login + SendMsg |
| Android 能 build | gradle 引入 .aar，Kotlin 调用 |
| cgo dll 能 link | C# P/Invoke 测试 |
| wasm 浏览器能跑 | chrome://inspect → 调试 JS 调用 |
| 事件能送达 | iOS `onEvent` 回调里打印 payload，收到新消息推送 |
| 错误能正确归一化 | Mock error code 1501 → iOS `onError(1501, "token expired")` |
| panic 不穿透 | call 层 middleware 捕获 panic → onError(90001, "internal") |

#### 5.7 交付产物

```
pkg/call/                      # 反射调用层（gomobile / cgo / wasm 共用）
  ├── call.go
  ├── converter.go
  ├── middleware.go
  ├── error.go
  └── handler.go

cmd/cgo/main.go                # PC 平台 C 导出函数入口
wasm/cmd/main.go               # Web 平台 syscall/js 入口
wasm/storage/idb.go            # IndexedDB 实现（Storage 接口的 wasm 版本）
```

---

## 6. SDKError 错误模型

遵循用户偏好：**剥离 rpc/grpc 前缀，直接返回原始业务错误**。

```go
type SDKError struct {
    Code        int32   // 业务错误码（0=成功）
    Msg         string  // 原始业务消息
    OperationID string  // 请求追踪 ID
}

func (e *SDKError) Error() string { return e.Msg }
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
const (
    SessionTypeC2C   = 1  // 单聊
    SessionTypeGroup = 2  // 群聊

    MsgFromUser   = 100  // 用户消息
    MsgFromSystem = 200  // 系统通知

    RecvMsg          = 0  // 接收
    NotRecvMsg       = 1  // 不接收
    RecvMsgNotNotify = 2  // 接收不推送

    RoleMember = 10  // 群成员
    RoleAdmin  = 20  // 管理员
    RoleOwner  = 50  // 群主

    PullOrderAsc  = 0  // 升序
    PullOrderDesc = 1  // 降序

    NormalToken  = 0
    InValidToken = 1
    KickedToken  = 2
    ExpiredToken = 3
)
```

---

## 8. WebSocket 协议常量（transport/protocol.go）

```go
const (
    WSGetNewestSeq     = 1001  // 获取最新序列号
    WSPullMsgBySeqList = 1002  // 按序列号拉消息
    WSSendMsg          = 1003  // 发送消息
    WSSendSignalMsg    = 1004  // 发送信号消息
    WSPullMsg          = 1005  // 拉取消息
    WSGetConvMaxReadSeq = 1006 // 会话已读 seq
    WsPullConvLastMessage = 1007 // 会话最后一条消息
    WSPushMsg          = 2001  // 推送消息（服务端主动）
    WSKickOnlineMsg    = 2002  // 踢下线
    WSLogoutMsg        = 2003  // 登出
    WSSetBackgroundStatus = 2004
    WSSubUserOnlineStatus = 2005
    WSDataError        = 3001
)
```

---

## 9. EventType 枚举（events/event.go）

覆盖文档 10.2 节全部推送事件，与 transport 解耦（transport 只负责路由到 PushHandler，EventType 判定由 handler 内 proto.Unmarshal 完成）。

---

## 10. ConnectParams（transport/transport.go）

WS / TCP 等所有连接类型共用的认证参数（服务端统一定义）：

```go
type ConnectParams struct {
    UserID       string
    Token        string
    PlatformID   int
    DeviceID     string
    OperationID  string  // 空值自动 uuid
    SDKType      string  // 空默认 "go"
    SDKVersion   string
    Compression  string  // 空默认 "none"
    SendResponse bool
    IsBackground bool
}
```

WS 场景拼到 URL query（encodeQuery），TCP 场景放在握手包中。

---

## 11. 本地存储设计

### 11.1 已实现（Phase 4 ✅）— pkg/storage/storage.go

```go
type Storage interface {
    // Seq 元数据（SeqSyncEngine 游标）
    GetMaxSeqs(userID string) (maxSeqs, minSeqs map[string]int64, err error)
    SetMaxSeqs(userID string, maxSeqs, minSeqs map[string]int64) error

    // 版本游标（EntitySyncEngine；entity ∈ friends/blacks/joinGroups/groupMembers/conversations）
    GetVersion(entity string) (versionID string, version uint64, err error)
    SetVersion(entity, versionID string, version uint64) error

    // 生命周期
    Clear(ctx context.Context, ownerUserID string) error // Logout(clearData=true) 时按 owner 清理
}
```

当前默认实现 MemoryStorage；`ClientConfig.Storage` 支持注入。

### 11.2 已实现（Phase 4.5 ✅）

SQLite 8 张业务表（models.go + sqlite.go）、Storage 接口扩展 + MemoryStorage 全实现、
IDHash 复用 pkg/utils/hash.HashStringSet、双同步引擎（pkg/sync）——
交付明细见 **Phase 4.5（§4.5.1 - §4.5.6）**。wasm IndexedDB 方案仍归 Phase 5。

---

## 12. 测试策略

| 测试类型 | 工具 | 状态 |
|---|---|---|
| 单元测试 | `testing` | ✅ transport 33 用例通过 |
| 接口测试 | httptest / websocket upgarde | ✅ WS 端到端（真服务端 echo） |
| 竞态交错 | 3000 次迭代 stress | ✅ TokenStore 过期删除竞态已修复 |
| 集成测试 | mock 服务 | ⏳ Phase 4 目标 |
| 平台编译 | gomobile + wasm | ⏳ Phase 5 目标 |

### 已覆盖的测试点

| 文件 | 覆盖 |
|------|------|
| router_test.go | Push 注册/注销/路由、pending 命中/一次性交付/failAllPending、nil 防护、并发 Route+Register |
| transport_test.go | ConnectParams 默认值、TransportStatus String、baseTransport actor 退出联动、stopActors 生命周期、断连唤醒 pending |
| auth_test.go | Token 存取/过期清理/多用户隔离/删除/Refresh 注入、**并发竞态（3000 次交错）** |
| http_test.go | POST/GET 成功解析、业务错误→SDKError、token header 注入、ctx 超时取消、token 过期重试一次 |
| ws_test.go | 端到端：认证 query 校验、请求-响应 echo、服务端 push 路由、未连接/断开后发送报错、**自动重连**、断开唤醒 SendRequestAndWait |

---

## 13. 依赖清单

```
github.com/gorilla/websocket         # WebSocket
github.com/google/uuid               # 通过 uuidx 封装间接依赖
go.uber.org/zap                      # 日志（通过 logger 接口间接依赖）
gorm.io/gorm                         # ORM (Phase 4)
github.com/glebarez/sqlite           # pure-Go SQLite 驱动 (Phase 4)
github.com/PaperMan11/goim/pkg/protocol  # proto 定义（镜像）
golang.org/x/crypto                  # utils/bcrypt 依赖
github.com/pkg/errors                # utils/hash 依赖
github.com/golang-jwt/jwt/v4         # utils/jwtx 依赖
github.com/PaperMan11/goim/pkg/utils/convert  # utils/convert 依赖
github.com/PaperMan11/goim/pkg/utils/timex     # utils/timex 依赖
```

---

## 14. 开发进度总览

```
✅ Phase 1    model → events → logger → transport (33 单元测试)
✅ Phase 2    六个 Manager interface 骨架
✅ Phase 2.5  Client interface 骨架 + EventType 扩展（42 枚举）
✅ Phase 3    六个 Manager 实现（WS 链路 + 事件派发）
✅ Phase 4    Storage 接口 + DefaultClient 完整实现（生命周期钩子入 interface、推送路由）
✅ Phase 4.0a time → timex 全量替换（26 文件）+ 业务转换统一走 utils/convert（7 文件）
✅ Phase 4.0b 高频便捷回调补齐（10 个 On* + EventConnecting + 推送路由重写为 MsgData 信封 + ContentType 分流）
✅ Phase 4.5  本地存储落地（SQLite 8 表，storage 8 测）+ 数据同步引擎（SeqSyncEngine + EntitySyncEngine，sync 10 测）+ Login 链路集成
⏳ Phase 5    gomobile / cgo / wasm 编译输出（暂缓）  ← 下一步
```

**每个 Phase 的完成判定**：编译通过 + 对应单元测试覆盖核心路径 + `go vet` 无告警。

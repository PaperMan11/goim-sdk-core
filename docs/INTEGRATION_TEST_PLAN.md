# goim-sdk-core 服务端对接测试方案

> 目标：在跨语言封装（Phase 5）之前，验证 SDK 与 goim 服务端真实链路的端到端正确性。
> 服务端仓库：`goim`（im-api / im-msggateway / im-msgtransfer / im-push / im-rpc / im-cron）

**当前进度**：✅ E0 冒烟通过（2026-10-28）→ ⏳ E1 进行中：E1.1（错误 secret 拒绝，R9 回归 ✅）、E1.3（重复登录 ✅）、E1.4（服务端**拒绝**未认证 WS 连接，bad handshake，结论已记录 ✅）通过；E1.5（token 过期刷新）、E1.6（Logout，主动断连不发 EventDisconnect 按设计，测试预期已修正）待回归 → E2-E6 待执行。风险 R1/R7/R8/R9/R10/R11 已闭环，R2 已修复待回归，详见 §4。

---

## 1. 目标与范围

| 项 | 说明 |
|---|---|
| 测试对象 | `client.NewClient` 全链路：Login → 同步 → 收发消息 → 实体同步 → 事件 → 异常恢复 |
| 对接面 | HTTP API（im-api，`/auth` `/user` `/relation` `/group` `/conversation` `/msg`）+ WS（im-msggateway，ReqIdentifier 1001/1002/1003 请求、2001-2005 推送） |
| 不在范围 | 跨语言封装（gomobile/wasm）、性能压测（列为可选附加）、服务端自身功能测试 |

**验证目标（按优先级）**
1. Login 全链路一次跑通（Token + WS + SeqSync + EntitySync）
2. 消息收发与推送事件正确投递
3. 五类实体（好友/黑名单/群组/群成员/会话）增量同步与游标推进
4. 断线重连、被踢下线、多端互踢等异常场景恢复
5. SQLite 本地存储落盘与重启恢复

---

## 2. 环境准备

### 2.1 服务端

1. 按 goim 仓库 README/Makefile 启动全部组件，记录实际地址：
   - `GOIM_API_URL`：im-api HTTP 基础地址（SDK `BaseURL`）
   - `GOIM_WS_URL`：im-msggateway WS 地址（SDK `Endpoint`，形如 `ws://host:port/ws`）
2. 记录 admin secret（`/auth/get_admin_token` 注册用户必需），环境变量 `GOIM_SECRET`
3. 确认依赖组件（MySQL/Redis/MQ 等）健康，能直接查库取证
4. 准备服务端日志实时输出（gateway 推送日志、api 请求日志）

### 2.2 SDK 侧测试基建

新建 `test/integration/` 目录，Go 集成测试形式（与单元测试隔离）：

```
test/integration/
├── main_test.go        # TestMain：环境变量解析 + 环境自检（E0 前置校验）
├── helper_test.go      # 公共基建：newTestClient / waitEvent / assertSQLite / 事件收集器
├── e0_smoke_test.go    # E0 冒烟
├── e1_auth_test.go     # E1 认证与连接
├── e2_msg_test.go      # E2 消息链路
├── e3_push_test.go     # E3 推送事件
├── e4_sync_test.go     # E4 实体同步
├── e5_reconnect_test.go# E5 异常与重连
└── e6_storage_test.go  # E6 存储与多账号
```

**执行方式**：

```bash
# 不使用 build tag（保证 IDE/gopls 可索引），靠环境变量注入 + requireEnv 自动 Skip 隔离
# 需显式指定服务端地址（BaseURL 需包含 /api/v1 前缀，SDK 在其上直接拼接业务路径）
go test ./test/integration/... -v -count=1
```

**环境变量约定**：

| 变量 | 含义 | 缺省行为 |
|---|---|---|
| `GOIM_API_URL` | im-api 地址 | 缺失则 Skip（本地单测不受影响） |
| `GOIM_WS_URL` | 网关 WS 地址 | 同上 |
| `GOIM_SECRET` | admin secret | 同上 |
| `GOIM_LOG_LEVEL` | SDK 日志级别 | `debug` |

**公共基建（helper_test.go）**：

- `newTestClient(t, userID)`：构造 `ClientConfig{BaseURL, Endpoint, Logging: debug console, SessionDataPath: t.TempDir() 下 SQLite}`，注册 `OnConnect/OnDisconnect/OnNewMessage/OnForceLogout/...` 到事件收集器
- `waitEvent(t, collector, eventType, timeout)`：带超时等待指定事件，超时 Fatal 并 dump 已收事件
- 双客户端 fixture：A/B 两个 Client 实例互发（集成测试至少需要 2 个连接，群聊场景 3 个）

### 2.3 账号与数据

| 资源 | 数量 | 用途 |
|---|---|---|
| 用户 | 3 个（u1/u2/u3） | 单聊互发、群聊、互踢 |
| 群 | 1-2 个 | 群聊、群成员同步、群事件 |
| 平台 ID | 2 个平台 | 多端登录/互踢（同账号不同 platformID） |

每次测试运行使用带时间戳的账号前缀（如 `it_20261027_u1`），避免历史数据干扰；或测试开始前 `Logout(clearData=true)`。

---

## 3. 测试用例

### E0 冒烟（必须最先通过，全部后续用例的前置）

| # | 用例 | 步骤 | 预期 |
|---|---|---|---|
| E0.1 | HTTP 连通 + GetAdminToken | `Auth().GetAdminToken(secret, "imAdmin")` | 返回 token；响应 `code=0` |
| E0.2 | 用户注册 | 用 admin token 调 `User().Register(u1/u2/u3)` | 成功；服务端库可查到用户 |
| E0.3 | GetUserToken | `Login` 内部触发 | token 写入 TokenStore，`EventLoginSuccess`（AuthMgr 层） |
| E0.4 | WS 连接 | `Connect(ctx)` | `Status()==Connected`，`OnConnecting`→`OnConnect` 事件顺序正确 |
| E0.5 | Login 全链路 | `Login(ctx, u1, platformID)` | `EventLoginSuccess`；SeqSync + EntitySync 无报错；SDK debug 日志无 error/warn |

> ⚠️ E0.1/E0.2 大概率率先暴露 §4 风险 R1（HTTP token 注入选路），先修复再继续。

> ✅ **E0 已完成（2026-10-28）**：4 个集成用例全绿 —— `TestE0_AdminToken`（E0.1）、`TestE0_RegisterUsers`（E0.2）、`TestE0_LoginFullFlow`（E0.3-E0.5，含五类实体同步全链路）、`TestE0_LoginFailedWrongAdmin`（E1.2 前置，错误凭证路径）。
> E0 阶段累计暴露并修复 5 项对接问题：R1（token 选路）、R7（admin bootstrap）、R8/R9（服务端鉴权语义）、R10（proto 字段名 10 处 + 响应包装）；另外补齐 deviceID 自动生成持久化、sdkVersion 注入、`Close()` 句柄释放。

### E1 认证与连接

| # | 用例 | 步骤 | 预期 |
|---|---|---|---|
| E1.1 | 错误 secret | GetAdminToken 传错 secret | 返回 `NoPermissionError`；服务端已恢复 secret 校验（常量时间比较），E1.2 错误 AdminUserID 与错误 secret 均可触发登录失败 |
| E1.2 | 错误管理员凭证登录（原"未注册用户登录"） | Login + 不在服务端管理员列表的 AdminUserID | `EventLoginFailed`，错误信息可读。**实测发现**：R8（get_user_token 不校验用户注册态）、R9（GetAdminToken 不校验 secret，仅校验 userID 是否管理员） |
| E1.3 | 重复 Login | 已登录状态下再次 Login | 状态一致，不产生脏游标 |
| E1.4 | 未登录直接 Connect | 不 Login，直接 Connect（DefaultPlatformID 兜底） | WS 握手行为符合服务端约定（明确接受/拒绝即可，记录结论） |
| E1.5 | Token 过期 | 服务端配置超短有效期（如 60s）→ 等 90s → 发 HTTP 请求 | 触发 401 类错误码 → SDK 刷新/重试路径；最终成功或给出明确错误 |
| E1.6 | Logout | `Logout(false)` / `Logout(true)` | 断连 + `EventDisconnect`；clearData=true 后 SQLite 该 owner 数据清空 |

### E2 消息链路（核心）

| # | 用例 | 步骤 | 预期 |
|---|---|---|---|
| E2.1 | 单聊发送（WS 1003） | A Login 后 `Msg().SendMsg` → B 在线 | A 收到 `SendMsgResp{ServerMsgID, ClientMsgID, SendTime}` + `EventMsgSent`；B 触发 `OnNewMessage`，payload 字段逐项比对（sendID/recvID/content/clientMsgID） |
| E2.2 | 群聊发送 | A 发群消息，B/C 在线 | B、C 均收到，recvGroupID 正确 |
| E2.3 | 离线消息 + SeqSync 补齐 | B 登出 → A 发 10 条 → B 登录 | B 登录流程内 SeqSync 完成，`OnNewMessage`/本地库补齐 10 条，maxSeqs 游标与服务端一致 |
| E2.4 | NewestSeq（1001） | 登录后比对 SDK 存储游标与服务端 | maxSeqs/minSeqs 一致 |
| E2.5 | PullMsgBySeq（1002） | 指定 seq 范围/分页/order 拉取 | 返回条数与范围一致，`IsEnd`/`EndSeq` 正确 |
| E2.6 | 历史消息落库 | 收发 N 条后查 SQLite | 消息与 seq 完整、可按会话查询 |
| E2.7 | 消息持久化参数 | 验证服务端在线/离线推送均可达 | 在线走 gateway 推送，离线登录后走 sync，两条路径都拿到同一 clientMsgID |
| E2.8 | 推送消息落库（2001 实时持久化） | B 在线，A 发 1 条消息 | B 收到 `OnNewMessage` 后直接查 B 的 SQLite：该消息已存在（clientMsgID 匹配、内容一致）；且 maxSeqs 游标**未**被推送推进（推送可能丢失/乱序，游标仅由 gap 拉取推进） |

### E3 推送事件（WS 2001 系统通知分流）

| # | 用例 | 触发方 | 预期事件（接收方断言） |
|---|---|---|---|
| E3.1 | 普通消息推送 | A 发消息 | B `EventNewMsg` |
| E3.2 | 撤回（2101） | A `Msg().RevokeMsg` | B `OnMessageRevoked(clientMsgID)`；且 B 的 SQLite 中该 seq 消息 Status=MsgStatusRevoked（SDK 先应用缓存再发布事件） |
| E3.3 | 已读（2200） | B `Msg().MarkConversationAsRead` | A `EventMarkAsRead`，seq 正确；且 A 的 SQLite 中对应消息 IsRead=true（seqs 列表与 HasReadSeq 兜底两种形态均验证） |
| E3.4 | 删除（2102） | `DeleteMsgs` | `EventDeleteMsgs`；且接收方 SQLite 中该 seq 消息已删除 |
| E3.5 | 好友事件（1201-1210） | A 加 B 好友 / 同意 / 备注 / 删除 | B 对应 Friend 事件（EventFriendApplication / EventFriendAdded / EventFriendInfoChanged / EventFriendDeleted） |
| E3.6 | 群事件（1501-1515） | 建群 / 邀请 / 踢人 / 退群 / 禁言 / 解散 | 各 Group 事件 payload 中群 ID 与成员正确 |
| E3.7 | 用户资料变更（1303） | A 更新资料 | 已订阅方 `EventUserInfoUpdated` |
| E3.8 | 在线状态订阅（2005） | B `User().SubscribeUsersStatus(u1)` → u1 上/下线 | `EventUserStatusChange`（2004 后台状态同法验证） |
| E3.9 | 未映射 contentType | 人为构造未知通知（或记录服务端既有类型） | SDK warn 日志 `unmapped contentType`，不 panic |
| E3.10 | 实体类通知触发增量同步 | A 加好友 / 建群（产生 1201-1210/1501-1515 通知） | B **不重连**，约 500ms 去抖后 B 的 SQLite friend/group 缓存自动更新（通知 Detail 仅含 ID 列表，SDK 须自行拉取增量补全实体）；与登录同步互斥不冲突 |

### E4 五类实体增量同步（EntitySync）

每类实体走「登录首拉 → 对端操作产生增量 → 重连/重登增量补齐」三步：

| # | 实体 | 对端操作 | 断言 |
|---|---|---|---|
| E4.1 | 好友 | A 加 B、同意 | B 登录后 SQLite friend 表有记录；A 侧再加好友后增量 insert；删除好友 → delete 应用 |
| E4.2 | 黑名单 | A 拉黑/移除 | blacks 增量同步；full=true 时清表重拉 |
| E4.3 | 群组 | A 建群 / 邀请 B / 转让 / 解散 | joinGroups 增量 insert/update/delete |
| E4.4 | 群成员 | 邀请/踢人/退群 | groupMembers 按 groupID 增量；成员列表与实际一致 |
| E4.5 | 会话 | 收发消息 / 置顶 / 已读 | conversations 增量 + 排序正确（`GetSortedConversationList` 比对） |
| E4.6 | full=true 全量回退 | `Logout(true)` 清游标 → 重登 | 触发 full 拉取 + IDHash 校验分支，数据最终一致 |
| E4.7 | 游标推进 | 任意增量后查 Storage | `GetVersion` 返回的 versionID/version 与服务端响应一致且单调递增 |

### E5 异常与重连

| # | 用例 | 步骤 | 预期 |
|---|---|---|---|
| E5.1 | 服务端主动断连 | gateway 侧 kill 连接（或重启 msggateway） | SDK 自动重连 → `EventReconnect` → 触发 resync（gap 拉取 + 五类增量） |
| E5.2 | 断连期间消息补齐 | 断 A 连接 → B 发 5 条 → A 重连 | 重连后 5 条全部补齐 |
| E5.3 | 多端互踢 | u1 在 platformID=1 在线 → 同账号 platformID=2 登录 | 旧端收到 `OnForceLogout` + 断连（2002/2003），新端正常 |
| E5.4 | 心跳保活 | 连接静置 3 分钟不做任何操作 | 30s ping 周期维持，连接不断（pong 续期 deadline 生效） |
| E5.5 | 网络中断恢复 | 断网 30s → 恢复 | 重连成功，无 goroutine 泄漏（`Status()` 流转 Disconnected→Connecting→Connected） |
| E5.6 | 重连期间请求 | 重连窗口内调 `SendMsg` | 返回 notConnected 类明确错误，不挂死（SendRequestAndWait 被唤醒） |
| E5.7 | 服务端 5xx | im-api 瞬时不可用（可停 api 组件数秒） | HTTP 层重试 3 次（指数退避），最终错误信息含 operationID |

### E6 存储与多账号

| # | 用例 | 步骤 | 预期 |
|---|---|---|---|
| E6.1 | SQLite 重启恢复 | 收发消息 → 销毁 Client → 新建同路径 Client → Login | 游标保留，登录走增量（对比首登日志确认未全量重拉），消息完整 |
| E6.2 | 多账号 owner 隔离 | 同一 DB 路径先后登录 u1、u2 | 数据按 owner 隔离，互不污染；u1/u2 同 C2C 会话同 seq 的消息共存（消息唯一索引含 owner_user_id，C2C 会话 ID 双方一致、seq 服务端共享） |
| E6.3 | clearData 全量重同步 | `Logout(true)` → 重登 | 本地清空后走 full 同步，数据最终一致 |
| E6.4 | 内存存储模式 | `ClientConfig` 不配 Storage/SessionDataPath | 功能可用，重启数据丢失（行为符合文档） |

### E7 稳定性（可选，对接通过后执行）

| # | 用例 | 说明 |
|---|---|---|
| E7.1 | 长稳收发 | 双端持续互发 1 小时（含心跳周期），无断连、无内存持续增长、无事件丢失 |
| E7.2 | 并发发送 | 10 goroutine × 100 条并发 SendMsg，全部 ack 且对端全部收到、seq 连续 |
| E7.3 | 重连风暴 | 循环 kill gateway 连接 50 次，重连均恢复且数据最终一致 |

---

## 4. 已知对接风险点（代码走查，测试时重点关注）

| # | 风险 | 位置 | 影响 | 建议动作 |
|---|---|---|---|---|
| R1 | ~~**HTTP token 注入选路失效**~~ **已修复并经 E0 验证**：`HTTPClient` 新增 `CurrentUserSetter`，Login 后注入当前 `(userID, platformID)`，token 按 `key(userID, platformID)` 精确选路；Logout 清除 | `transport/http.go` Get 注入处 vs `transport/auth.go` key() | ~~P0~~ 已闭环 | ✅ `TestE0_RegisterUsers`/`TestE0_LoginFullFlow` 回归通过 |
| R2 | ~~HTTP token 过期重试逻辑~~ **E1 实测后真正修复**：原实现 `tokenRefreshed` 置位后空转重试（不刷新）；且只识别 1501/1504，服务端短 TTL 场景实际返回 1507（token 记录不存在，Redis TTL 先于 JWT exp）→ 直接失败。现：`model.IsTokenRefreshable` 覆盖 1501/1502/1503/1504/1507（1506 被踢不可刷新），Post/Get 收到认证错误 → `TokenStore.Refresh(当前用户)` 真正取新 token → 重试一次；刷新闭包未配置或失败 → 立即返回原错误不空转 | `transport/http.go` Post/Get、`model/error.go`、单测 `http_refresh_test.go` | E1.5 回归确认 | 服务端保持短 TTL 重跑 E1.5 |
| R3 | ~~WS 认证参数字段名比对~~ **已验证**：E0.4/E0.5 握手成功且全链路稳定，query 字段名（`userID/token/platformID/deviceID/sendResponse/isBackground/compression/sdkType/sdkVersion`）与服务端网关一致；`deviceID` 已补自动生成 + SQLite 持久化，`sdkVersion` 从 `version/VERSION` 注入 | `transport/ws.go` encodeQuery | 已闭环 | ✅ E0.4 握手日志核对（`deviceID=<uuid>`、`sdkVersion=1.0.0`） |
| R4 | ~~compression 默认不一致风险~~ **已验证**：`compression=none` 与服务端一致，同步链路 JSON 解析无乱码 | `transport/encoder` `compressor` | 已闭环 | ✅ E0.5 五类实体同步解析正常 |
| R5 | `handleMessagePush` 只按 `MsgData` 信封解析，通知走 `ContentType >= 1000` 分流；服务端若有信封外推送形态（如 2004/2005 直接推送 tips）需要核实 | `client/client_impl.go` handleMessagePush | 推送事件丢失 | E3.8 用真实服务端验证 |
| R6 | 事件订阅 channel 满时静默丢弃（与 eventbus 策略一致），测试断言必须先起订阅者再触发操作 | `client/client_impl.go` Subscribe | 测试假阳性/漏收 | helper 统一在操作前订阅，并留足 buffer |
| R7 | ~~**Login 需要 admin bootstrap**~~ **已实现双模式并经 E0 验证**：`ClientConfig.AdminSecret`（受信环境自动 bootstrap：GetAdminToken → 挂载 → GetUserToken）+ `LoginWithToken`（终端模式，宿主下发）+ `TokenProvider`（终端模式过期续期回调，见 docs/DEVELOPMENT.md §5.3） | `im-api/middlewares/auth.go`、`im-rpc/auth/logic/get_user_token.go` | ~~P0~~ 已闭环 | ✅ `TestE0_LoginFullFlow` bootstrap 全链路回归通过 |
| R8 | **`get_user_token` 不校验用户存在性**：未注册用户也能获取 token 并完成 WS 连接（E0 实测），"未注册登录失败"场景不成立；SDK 层登录成功与否不构成注册校验手段 | E0 实测（TestE0_LoginFailedUnregistered 日志） | 低：业务侧需自行保证先注册后登录 | 已确认服务端行为；E1.2 改为错误凭证场景 |
| R9 | ~~`get_admin_token` 不校验 secret~~ **已修复**：服务端恢复 secret 校验（`subtle.ConstantTimeCompare` 常量时间比较 + 管理员列表双重校验）。遗留建议：① 启动时校验 AdminSecret 非空且 >= 16 字符（空 secret 与空请求 `ConstantTimeCompare` 返回 1）；② secret 错误与非管理员错误对外统一返回 `NoPermissionError`，避免差异成为 secret 猜测的 oracle | 服务端 `get_admin_token.go` 代码评审 | 中：已消除；两点建议落地后闭环 | E0/E1 用例两种失败路径均可用；部署后回归 |
| R10 | **HTTP 请求字段名/响应包装必须与服务端 proto 一致**：服务端字段名不统一（`fromUserID`/`ownerUserID`/`userID` 混用），且所有列表响应均为包装对象（`conversations`/`usersInfo`/`groupInfos` 等）而非裸数组，SDK 发错名字段会被静默忽略并报 "xxx is required" | E0 实测 + `pkg/protocol/**/*.pb.go` 比对 | 同步链路各接口逐一踩坑；跨语言封装后更难排查 | ✅ 已全量核对并修复：3 处请求字段名 + 10 处响应包装解析；后续新增接口须先查 pb.go 字段名与 Resp 结构再写 tag |
| R11 | **404 假成功（已修复，保留防护）**：SDK 曾对非 200 响应仍按 `{code,msg,data}` 解析 → `code=0` 零值 + 空 data → **"假成功"**（如 get_user_token 404 后返回空 token 仍存入 TokenStore，表现为"登录成功但 token 未保存"）。前缀约定：**`ClientConfig.BaseURL` 需包含 `/api/v1`**（如 `http://host:port/api/v1`），HTTPClient 直接在其上拼接业务路径，不做二次拼接 | E1 阶段用 httptest 复现（`TestAcquireUserTokenStoresToken`） | **P1**：token/任意接口静默空结果，排查困难 | ✅ 已修复：非 200 状态码直接返回 SDKError（含 URL + body 片段），杜绝假成功；token 存取链路有确定性单测守护 |

---

## 5. 观测与取证手段

| 层 | 手段 |
|---|---|
| SDK | `Logging: &logger.Config{Level: debug, Format: console}` 全量日志；必要时 `OutputPaths` 写文件留档 |
| 服务端 | im-api 访问日志（operationID 贯穿）、msggateway 连接/推送日志 |
| 数据库 | 直查服务端 DB：用户、消息 seq、会话版本号、好友/群组版本号，与 SDK SQLite（`SessionDataPath`）比对 |
| WS 抓包 | 需要时用 wsdump/代理抓 gateway 原始帧，核对 ReqIdentifier 与编解码 |

---

## 6. 通过标准与交付物

**通过标准**
1. E0-E6 全部用例通过（E3.9、E7 允许标注跳过/可选）
2. 无 P0/P1 级缺陷遗留；P2 级缺陷有记录和结论
3. §4 风险逐条有验证结论（修复或确认不适用）

**交付物**
1. `test/integration/` 可重复执行的集成测试代码
2. 缺陷记录表（编号 / 用例 / 现象 / 根因 / 修复 / 回归结果）
3. 服务端对接结论备忘：实际确认的接口路径、字段名、错误码、推送形态差异清单（后续跨语言封装的输入）

**缺陷记录表模板**

| 编号 | 发现用例 | 现象 | 根因 | 修复 | 回归 |
|---|---|---|---|---|---|
| BUG-001 | E0.1 | ... | ... | ... | ✅/❌ |

---

## 7. 执行顺序

```
① 环境搭建 + 测试基建（test/integration 骨架 + helper）  ✅ 已完成
② E0 冒烟（含 R1/R7 修复回归）                          ✅ 已完成（2026-10-28）
③ E1 → E2 → E3 → E4（核心功能，双客户端为主）           ⏳ 当前阶段 ← 下一步
④ E5 异常场景（需要操作服务端组件）
⑤ E6 存储（依赖 E2/E4 数据）
⑥ E7 稳定性（可选）
⑦ 缺陷回归 + 对接结论备忘整理
```

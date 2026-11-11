# goim-sdk-core 联调结论与缺陷回归备忘

> 对应 [INTEGRATION_TEST_PLAN.md](./INTEGRATION_TEST_PLAN.md) §6 交付物 2/3、§7 执行顺序第 ⑧ 步。
> 整理日期：2026-11-11；范围：E0-E6、E8（E7 稳定性为可选，未执行）。
> 真实环境：im-api `192.168.241.128:18880/api/v1`、网关 `192.168.241.128:50001/ws`、RustFS `192.168.241.128:9000`（bucket `goim-files`）。

## 1. 缺陷回归执行记录

### 1.1 本轮回归（2026-11-11，改动未提交）

| 检查项 | 命令 | 结果 |
|---|---|---|
| 编译 | `go build ./...` | ✅ 通过 |
| 静态检查 | `go vet ./...` | ✅ 无告警 |
| 全量测试 | `go test ./... -count=1` | ✅ 全绿（集成用例缺 `GOIM_*` 时 Skip，不 panic） |
| 会话相关单测 | `go test ./pkg/manager -count=1` | ✅ 通过（含新增 `TestSetConversationFillsOwnerAndPath`） |
| 集成包空环境 | `go test ./test/integration -count=1` | ✅ 全部 Skip，无 panic |

本轮定位并修复 2 项缺陷：BUG-012（E5 用例空环境 panic，回归基建问题）、BUG-013（用户态会话设置无 HTTP 入口，服务端补路由 + SDK 封装）；E4.5 已切换用户态并完成真实环境回归（2026-11-11 通过）。另确认 E8 中 `access_url` 偶发 500 为服务端缺陷，已修复（BUG-014）。

### 1.2 真实环境执行记录（历史）

| 阶段 | 范围 | 结果 | 关联缺陷 / 备注 |
|---|---|---|---|
| E0 | 冒烟 + 基础链路 | ✅ 2026-10-28 | BUG-001/003/004/005 |
| E1 | 认证 / token | ✅ 已完成 | BUG-002/005 |
| E2 | 消息收发与同步 | ✅ 2026-11-03 | BUG-008（R12 服务端 write-through 闭环） |
| E3 | 通知（撤回/已读/删除） | ✅ 已完成 | R13 决策保留 |
| E4 | 好友/黑名单/群/群成员/会话增量 | ✅ 2026-11-06（E4.1-E4.7） | BUG-009/010 闭环；E4.5 用户态切换后于 2026-11-11 回归通过 |
| E5 | 异常与重连 | ✅ 2026-11-06 | BUG-006/007；R17 策略保留 |
| E6 | 存储与多账号 | ✅ 2026-11-06（4/4） | 重启恢复 / 隔离 / clearData / 内存模式 |
| E8 | 第三方存储上传 | ✅ 2026-11-10 | BUG-011/014；E8.1 FormData 全链路通过，Multipart/续传/Abort 同轮执行 |
| E7 | 稳定性（长稳/并发） | 未执行 | 可选 |

### 1.3 本轮改动清单（未提交）

| 文件 | 改动 |
|---|---|
| `pkg/manager/conversation.go` | ConvMgr 新增 `SetConversation` 接口 |
| `pkg/manager/conversation_impl.go` | 实现 `POST /conversation/set_conversation`（填 owner、完整 upsert 语义注释） |
| `pkg/manager/conversation_impl_test.go` | 新增路径 / owner / payload / nil 入参单测 |
| `test/integration/e4_sync_test.go` | E4.5 切换用户态；删除 admin helper 及无用导入 |
| `test/integration/e5_reconnect_test.go` | BUG-012：`wsUpstreamAddr(t)` 内部 `requireEnv`，空环境 Skip |
| `docs/INTEGRATION_TEST_PLAN.md`、`docs/DEVELOPMENT.md`、本文件 | R16 闭环、接口与协议坑同步 |

## 2. 缺陷记录表

共 14 项：SDK 侧 8 项（001/002/004/005/006/007/011/012）、服务端主导 5 项（003/008/009/010/014）、双端协作 1 项（013）。**全部闭环**。

| 编号 | 关联风险 | 发现用例 | 现象 | 根因 | 修复 | 回归 |
|---|---|---|---|---|---|---|
| BUG-001 | R1 | E0 | Login 后 HTTP 请求 token 选路错误 / 401 | HTTPClient 未注入当前用户，token 按空 key 取 | 新增 `CurrentUserSetter`，Login 注入 `(userID, platformID)`，Logout 清除 | ✅ E0 回归 |
| BUG-002 | R2 | E1.5 | token 过期后重试空转、最终失败 | 刷新标记置位但不刷新；仅识别 1501/1504，实测返回 1507 | `IsTokenRefreshable` 覆盖 1501/1502/1503/1504/1507；真正 Refresh 后重试一次；1506 不刷新 | ✅ E1.5 短 TTL 回归 |
| BUG-003 | R9 | E0/E1 | 错误 secret 也能拿到 admin token | 服务端未校验 secret | 服务端恢复 `subtle.ConstantTimeCompare` + 管理员列表双重校验 | ✅ E0/E1 两种失败路径 |
| BUG-004 | R10 | E0 | 同步接口报 "xxx is required" / 列表解析为空 | 请求字段名与服务端 proto 不一致；列表响应为包装对象而非裸数组 | 核对 pb.go：修正 3 处请求字段名 + 10 处响应包装解析 | ✅ E0 全量回归 |
| BUG-005 | R11 | E1 | 404 被解析为成功（code=0 零值），token 未保存 | 非 200 仍按 `{code,msg,data}` 解析 | 非 200 直接返回 SDKError（含 URL/body 片段） | ✅ 单测 `TestAcquireUserTokenStoresToken` |
| BUG-006 | E5 注 | E5.1 | 被踢/Logout 时订阅方收不到 `EventDisconnect` | `Disconnect` 先置 Disconnected，`actorsExited` 误判主动路径跳过派发 | 主动断开时补派发（置位前取样防重复） | ✅ E5.1/E5.3 回归 |
| BUG-007 | E5 注 | E5.7 | im-api 瞬时 5xx 直接失败 | HTTP 层仅网络错误重试，5xx 不重试 | 5xx 纳入指数退避重试 3 次，4xx 立即失败，错误统一带 operationID | ✅ E5.7 回归 |
| BUG-008 | R12 | E2.7 | 发送 ack 后立即重登/查询可能查不到消息 | 服务端 seq 分配/落库异步于 ack；SDK 游标无法感知滞后 | 服务端 write-through（ack 前落库+推游标）；SDK `fillHole` 缺口补拉 3×200ms，耗尽 Warn 跳过，失败不越游标 | ✅ E2.7 严格版 + 3 个 sync 单测 |
| BUG-009 | R14 | E4.2 | 拉黑后无法移除（black record does not exist） | `RemoveBlack` 存在性检查参数写反（查"对方是否拉黑我"） | 服务端改为 `IsBlack(ownerUserID, blackUserID)` | ✅ E4.2 回归 |
| BUG-010 | R15 | E4.3/E4.4 | 存量成员群信息/成员列表永久过期 | 邀请/踢人/退群/转让未推进存量成员 `JoinGroupDID` 版本日志 | 服务端四处补 `IncrVersionLogBatchDIDs(存量成员, groupID, Update)` | ✅ E4.3/E4.4 回归 |
| BUG-011 | R18 | E8.1 | FormData 上传 RustFS 返回 400 | 客户端把 `InitiateFormDataResp.File`（对象 key）当 multipart 字段名；S3 规定文件字段名必须为 `file` 且最后 | `CreateFormFile("file", opts.Name)`；非 2xx 补打响应体 | ✅ E8.1 回归 |
| BUG-012 | — | 本轮 `go test ./...` | 无环境变量时 E5 用例 panic，而非 Skip | `startWSProxy(t, wsUpstreamAddr())` 先于 `requireEnv` 执行 | `wsUpstreamAddr(t)` 内部 `requireEnv`，解析失败改 `t.Fatalf` | ✅ `go test ./...` 全绿 |
| BUG-013 | R16 | E4.5 | 普通用户无会话设置 HTTP 入口（置顶/免打扰不可用） | im-api 仅注册 admin 批量 `set_conversations`，用户态单会话 `set_conversation` RPC 未暴露路由 | 服务端补 `POST /conversation/set_conversation`（`requireSelfOrAdmin` + 1300 通知）；SDK 新增 `ConvMgr.SetConversation`；E4.5 切换用户态并先取回会话再改字段 | ✅ SDK 单测 + E4.5 真实环境回归通过（2026-11-11） |
| BUG-014 | R18 | E8.1 | `/third/access_url` 偶发 500、空 body，预签名下载校验失败 | 服务端该接口缺陷（具体修复实现由服务端完成） | 服务端修复 | ✅ 已修复（2026-11-11 确认）；E8.1 预签名下载校验通过 |

### 未闭环项（非代码缺陷，保留结论）

| 编号 | 事项 | 结论 | 后续 |
|---|---|---|---|
| R13 | 通知归属 `n_` 独立会话 | 接受 best-effort：通知不占业务 seq，实时推送是唯一到达路径 | 若产品要求通知必达，改回原会话归属（SDK 零改动） |
| R17 | 部署为 `allow_multi` | 跨平台不互踢属策略选择；同平台第 4 连接踢最旧 | 若要求单端互踢，服务端改 `LoginStrategy: replace` |
| R8 | `get_user_token` 不校验用户注册态 | 服务端既定行为 | 业务侧自行保证"先注册后登录" |

## 3. 服务端对接结论备忘

### 3.1 传输与鉴权

| 项 | 结论 |
|---|---|
| BaseURL | **必须包含 `/api/v1`**，SDK 直接在其上拼接业务路径，不做二次拼接 |
| 响应约定 | 统一 `{code, msg, data}`；非 200 直接报错，不做"假成功"解析 |
| 重试 | 5xx / 网络错误指数退避重试 3 次；4xx 不重试；错误含 `operationID` |
| admin token | `POST /auth/get_admin_token`，入参 `secret` + `userID`（管理员列表内）；服务端常量时间比较 secret |
| user token | `POST /auth/get_user_token`；TokenStore key 为 `(userID, platformID)` |
| 鉴权头 | HTTP 请求头 **`token: <token>`**（不是 `Authorization: Bearer`） |
| token 刷新 | 可刷新错误码：1501/1502/1503/1504/1507；1506（被踢）不可刷新 |
| WS 握手 | query：`userID/token/platformID/deviceID/sendResponse/isBackground/compression/sdkType/sdkVersion`；`compression=none` |

### 3.2 已确认 HTTP 路径（节选）

| 域 | 路径 |
|---|---|
| 认证 | `/auth/get_admin_token`、`/auth/get_user_token`、`/auth/parse_token`、`/auth/force_logout` |
| 用户 | `/user/user_register`、`/user/update_user_info`、`/user/get_users_info`、`/user/get_users_online_status`、`/user/subscribe_users_status` |
| 关系 | `/relation/add_friend`、`/relation/add_friend_response`、`/relation/delete_friend`、`/relation/get_incremental_friends`、`/relation/get_incremental_blacks`、`/relation/add_black`、`/relation/remove_black` |
| 群组 | `/group/create_group`、`/group/invite_user_to_group`、`/group/kick_group`、`/group/quit_group`、`/group/dismiss_group`、`/group/transfer_group`、`/group/get_incremental_join_groups`、`/group/get_incremental_group_members` |
| 会话 | `/conversation/get_all_conversations`、`/conversation/get_sorted_conversation_list`、`/conversation/get_incremental_conversations`、`/conversation/set_conversation`（用户态，owner 须为本人）、`/conversation/set_conversations`（admin） |
| 消息 | `/msg/revoke_msg`；发送/拉取走 WS（1003/1002/1001） |
| 第三方存储 | `/third/part_limit`、`/third/part_size`、`/third/multipart/init`、`/third/multipart/sign`、`/third/multipart/complete`、`/third/multipart/abort`、`/third/formdata/init`、`/third/formdata/complete`、`/third/access_url`（GET query `name`/`urlPrefix`）、`/third/delete_outdated` |

### 3.3 WS 协议号

| 编号 | 含义 |
|---|---|
| 1001 / 1002 / 1003 | NewestSeq / PullMsgBySeq / SendMsg |
| 2001 | 新消息推送 |
| 2002 / 2003 | 被踢下线通知 |
| 2004 / 2005 | 用户在线状态变更 / 订阅状态响应 |
| 2101 / 2102 | 消息撤回 / 消息删除 |
| 2200 | 会话已读 |
| 1201-1210 | 好友事件（申请/同意/备注/删除等） |
| 1300 / 1303 | 会话设置变更 / 用户资料变更 |
| 1501-1515 | 群事件（建群/邀请/踢人/退群/禁言/解散等） |

### 3.4 字段与协议坑（跨语言封装必须注意）

1. **用户字段名不统一**：`fromUserID` / `ownerUserID` / `userID` 按接口而异，错名会被服务端静默忽略并报 "xxx is required"。
2. **列表响应均为包装对象**（`conversations` / `usersInfo` / `groupInfos` 等），不是裸数组。
3. **通知内容类型 `ContentType >= 1000`**，与业务消息分流；通知归属 `n_<a>_<b>` 会话，不占业务 seq。
4. **FormData 上传**：签名字段写完后，文件必须以字段名 `file` 作为最后一个字段；`InitiateFormDataResp.File` 是对象 key，不是字段名。
5. **私有桶**：`Complete` 返回裸对象 URL，匿名 GET 返回 403（对象不存在同样 403）；下载必须走 `/third/access_url` 预签名。
6. **ExpireTime 单位不一致**：`AccessURLResp.ExpireTime` 为 Unix 秒，`InitiateFormDataResp.Expires` 实测为毫秒；客户端需兼容判断（>1e12 视为毫秒）。
7. **预签名 URL 有有效期**：业务侧只持久化对象 key，展示/下载时重新签发，不可把 URL 落库长期复用。
8. **单会话设置是完整 upsert**：`POST /conversation/set_conversation` 按请求体整体覆盖写入，未传字段会被清零。修改置顶/免打扰等单项属性前，先 `GetConversations` 取回当前会话，改目标字段后再提交；批量 `set_conversations`（admin）才是部分更新语义。

### 3.5 推送与同步语义

| 项 | 结论 |
|---|---|
| 登录 | `Login` 返回时仅 Token + WS 就绪；同步在后台，`SyncState: None→Syncing→Synced` |
| 事件序列 | `EventLoginSuccess` → `EventSyncStart` → `EventSyncFinish`（失败为 `EventSyncFailed`） |
| 游标推进 | 仅 gap 拉取推进；实时推送可能丢失/乱序，**不推进游标** |
| 缺口补拉 | 批内 seq 不连续时有界重试 3×200ms；耗尽视为永久缺失并 Warn；补拉失败游标停在缺口前 |
| 实体增量 | 收到 1201-1210/1501-1515 通知后约 500ms 去抖，自动拉取增量补全实体 |
| 重连 | 自动重连 → `EventReconnect` → 后台 resync（gap + 五类增量） |
| 多端 | `allow_multi`：每用户 10 连接、每平台 3 连接；超限踢最旧 |

### 3.6 存储与本地数据

| 项 | 结论 |
|---|---|
| 存储优先级 | 注入 `Storage` > `SessionDataPath` SQLite > 默认内存 |
| 多账号 | 数据按 owner 隔离，消息唯一索引含 `owner_user_id`；C2C 会话 ID 双方一致、seq 服务端共享 |
| Logout | `Logout(false)` 保数据；`Logout(true)` 清 owner 数据并触发 full 重同步 |
| E6 | 4/4 通过（重启恢复、多账号隔离、clearData 全量、内存模式） |

### 3.7 遗留待办

> 原待办 1（E4.5 用户态回归）与 4（`access_url` 500）均已闭环：前者 2026-11-11 回归通过，后者确认为服务端缺陷并修复。

1. E7 稳定性（长稳/并发/重连风暴）按需执行。
2. E8.2-E8.4 建议在 CI 环境固化（需要 RustFS + im-api 可用）。

### 3.8 跨语言封装接入检查清单

1. BaseURL 以 `/api/v1` 结尾；HTTP 请求头使用 `token`，不要用 `Authorization`。
2. 所有 HTTP 响应按 `{code,msg,data}` 解析，**HTTP 非 200 一律视为失败**（禁止零值假成功）。
3. token 失效按 1501/1502/1503/1504/1507 触发刷新并重试一次；1506 直接走被踢流程。
4. WS 握手 query 必带 `userID/token/platformID/deviceID`，`compression=none`。
5. 请求字段名按接口区分 `fromUserID`/`ownerUserID`/`userID`；列表取包装对象内的数组。
6. 会话单项设置先 `get_conversations` 取回，再改字段提交（完整 upsert）。
7. 上传：FormData 文件字段名固定 `file` 且最后；下载只走 `access_url` 预签名，并处理过期重签。
8. 本地只持久化对象 key 与同步游标；游标仅由 gap 拉取推进，实时推送不推进。

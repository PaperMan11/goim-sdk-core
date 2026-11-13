# goim-sdk-core

> GoIM 服务端的多平台 SDK 统一底座（Go 实现），支持 **iOS / Android（gomobile）**、**PC 桌面（cgo）**、**Web（wasm）** 与 **原生 Go 直连**。

本仓库是 SDK 核心库，不直接面向终端用户：它把登录鉴权、WebSocket 长连接、消息收发、群/好友/会话管理、本地存储与增量同步等能力沉淀为一套可跨语言调用的内核，各平台封装（.framework / .aar / .dll / .wasm）在此基础上产出。

## 特性

- **统一四层架构**：`client`（门面）→ `manager`（业务编排）→ `transport`（WS/HTTP）→ `model`（DTO/枚举/错误），依赖严格单向
- **登录与同步解耦**：`Login` 成功即返回，全量/增量同步在后台执行，进度通过 `SyncState()` 与同步事件暴露
- **两种凭证模式**：受信环境 `AdminSecret` 自动 bootstrap；终端 App 用 `LoginWithToken` + `TokenProvider` 自动续期（App 不接触 secret）
- **数据同步引擎**：Seq 同步（消息）与 Entity 同步（好友/黑名单/群/群成员/会话），支持游标增量、`IDHash` 全量校验、服务端 `full` 回退
- **本地存储可插拔**：`SQLiteStorage`（gorm + 纯 Go sqlite，iOS/Android/PC）、`IdbStorage`（浏览器 IndexedDB）、`MemoryStorage`（单测/无持久化）
- **传输层**：WebSocket 读写分离 + 心跳/重连 + 请求-响应路由；原生 `gorilla/websocket` 与浏览器原生 WebSocket（`syscall/js`）双实现
- **完整事件体系**：53 个 `EventType`，`Subscribe` / `SubscribeAll` + 生命周期回调（连接、登录、同步、被踢下线、新消息…）
- **跨语言调用层**：`pkg/call` 用反射 + JSON 暴露统一 API，规避 gomobile 的泛型/channel/多方法 interface 限制
- **日志可注入可开关**：`logger.Config` 配置驱动；`SetLogCallback` 支持宿主侧接收 SDK 日志（含 cgo / wasm）
- **统一错误模型**：`SDKError{Code, Msg, OperationID}`，`Error()` 直接返回剥离 RPC 前缀后的原始信息

## 平台支持

| 目标平台 | 构建方式 | 产物 | 本地存储 | 入口 |
|---|---|---|---|---|
| 原生 Go | `go build` | 可执行/库 | SQLite 或内存 | `pkg/client` |
| iOS / Android | `gomobile bind` | `.framework` / `.aar` | SQLite | `pkg/call` |
| PC 桌面 | `CGO_ENABLED=1 go build -buildmode=c-shared` | `.dll` / `.dylib` / `.so` | SQLite | `cmd/cgo` |
| Web | `GOOS=js GOARCH=wasm go build` | `.wasm` | IndexedDB | `cmd/wasm` |

> gomobile bind 与 cgo c-shared 链接需要对应工具链（Xcode / Android NDK / gcc），本机环境未实测；wasm 交叉编译已通过。

## 架构

```
┌──────────────────────────────────────────────────────┐
│ pkg/client   Facade：聚合所有 Manager、事件订阅、生命周期 │
├──────────────────────────────────────────────────────┤
│ pkg/manager  Auth / User / Msg / Group / Rel / Conv   │
│              Third（第三方存储签名 API）│ FileUploader │
│              业务编排 · pkg/sync 同步引擎 · pkg/storage │
├──────────────────────────────────────────────────────┤
│ pkg/transport  MessageTransport · HTTPClient · Router │
│                DefaultWSClient（原生 / js-wasm）       │
│                encoder 工厂 · compressor 工厂          │
├──────────────────────────────────────────────────────┤
│ pkg/model  DTO · 枚举 · 错误码 · SDKError              │
└──────────────────────────────────────────────────────┘

辅助包：pkg/events · pkg/logger · pkg/storage · pkg/sync · pkg/call
        pkg/utils/{run,eventbus,uuidx,parallel,workerpool,convert,timex,...}
```

## 目录结构

```
pkg/
  call/      跨语言反射调用层（gomobile / cgo / wasm 共用）
  client/    Client 门面与默认实现
  manager/   六个业务 Manager + ThirdMgr + FileUploader
  sync/      SeqSyncEngine / EntitySyncEngine
  storage/   Storage 接口 + Memory / SQLite / IndexedDB
  transport/ WS/HTTP 传输、路由、编解码、压缩
  events/    EventType 与事件结构
  logger/    Logger 接口 + zap 适配器
  model/     DTO、枚举、错误模型
  utils/     通用工具（actor-runner、eventbus、convert、timex 等）
cmd/
  cgo/       PC 平台 C ABI 入口（buildmode=c-shared）
  wasm/      Web 平台 syscall/js 入口 + 自测页 index.html
examples/
  token_provider/  终端模式 TokenProvider 示例（hostapp + hostserver）
test/integration/  真实服务端集成测试（E0-E8）
docs/              开发文档（架构、同步、存储、集成测试）
```

## 快速开始（原生 Go）

```go
cfg := &client.ClientConfig{
    BaseURL:         "https://api.example.com/api/v1",
    Endpoint:        "wss://api.example.com/ws",
    SessionDataPath: "sdk_data/data.db", // 留空则用内存存储
    Logging: &logger.Config{
        Level:  logger.LevelDebug,
        Format: logger.FormatConsole,
    },
    TokenProvider: fetchTokenFromHost, // 终端模式：token 过期自动续期
}

c := client.NewClient(cfg)
defer c.Close()

if err := c.LoginWithToken(ctx, userID, 1, token, expireSeconds); err != nil {
    log.Fatal(err)
}

ch, unsub := c.Subscribe(events.EventNewMsg)
defer unsub()
for ev := range ch {
    msg, _ := ev.Payload.(*model.MsgData)
    log.Printf("new msg: %+v", msg)
}
```

受信环境（服务端/运维工具）可直接 `Login`，配置 `AdminSecret` 由 SDK 自动 bootstrap。完整示例见 [examples/token_provider/hostapp/main.go](examples/token_provider/hostapp/main.go)。

## 跨语言使用（pkg/call 统一 API）

三种跨语言封装共用同一套 API（`pkg/call` 为 gomobile 绑定目标，`cmd/cgo` / `cmd/wasm` 在其上做语言侧适配）：

| API | 说明 |
|---|---|
| `Init(configJSON string) error` | 按 JSON 配置构建 Client（见下表） |
| `Call(method, argsJSON, onSuccess, onError)` | 异步反射调用，结果 JSON |
| `SyncCall(method, argsJSON) (string, error)` | 同步反射调用 |
| `Login` / `LoginWithToken` / `Logout` | 登录与登出（回调式，不阻塞宿主线程） |
| `Connect` / `Disconnect` / `Status` | 连接管理 |
| `Subscribe(eventType, onEvent) func()` | 订阅事件，事件 JSON：`{"type","timestamp","payload"}` |
| `RegisterCallback(CallBack)` | 全量事件回调（单方法 + JSON string，适配 gomobile） |
| `SetTokenProvider` / `UpdateToken` | 终端模式 token 桥 / 预取 token 注入 |
| `SetLogCallback(LogCallback)` | 宿主接收 SDK 日志（`OnLog(level, msg)`） |
| `SetCallTimeoutSec` / `Methods()` | 调用超时 / 已注册方法清单 |

可用方法名见 `Dispatcher.Methods()`，例如 `MsgMgr.SendMsg`、`UserMgr.GetUsersInfo`、`ConvMgr.GetAllConversations`。

### Init 配置（configJSON）

| 字段 | 类型 | 说明 |
|---|---|---|
| `baseURL` | string | HTTP API 地址（含 `/api/v1` 前缀） |
| `endpoint` | string | WS 连接地址（`ws://` / `wss://`） |
| `platformID` | int32 | 默认平台 ID |
| `deviceID` | string | 设备 ID（空则自动生成并持久化） |
| `sdkVersion` | string | SDK 版本（空则用编译期版本） |
| `compression` | string | 压缩算法：`none` / `gzip` / `zlib` |
| `sessionDataPath` | string | SQLite 路径；空/在 wasm 下用内存或 IndexedDB |
| `adminSecret` | string | 应用密钥（仅受信环境） |
| `adminUserID` | string | 管理员账号（默认 `imAdmin`） |
| `logging` | object | 日志开关，见下节 |

### 构建

```bash
# iOS / Android（gomobile）
gomobile bind -target=ios     -o goim-sdk.framework ./pkg/call
gomobile bind -target=android -o goim-sdk.aar      ./pkg/call

# PC 桌面（cgo C ABI，导出 GoimSdkInit / GoimSdkCall / GoimSdkSetLogCallback 等）
CGO_ENABLED=1 go build -buildmode=c-shared -o goim-sdk.dll ./cmd/cgo

# Web（wasm）
GOOS=js GOARCH=wasm go build -o cmd/wasm/dist/goim.wasm ./cmd/wasm
```

Web 侧把 `wasm_exec.js`、`idb_wrapper.js`、`goim.wasm` 放到同一目录，打开 `cmd/wasm/index.html` 可跑自测页（init / 状态 / IDB 建表 / 事件订阅 / 日志回调）。

## 日志

三级来源优先级：**注入 `ClientConfig.Logger` > `ClientConfig.Logging` 配置构建 zap > 静默**。

跨语言场景（cgo / wasm / gomobile）常用 `logging` 配置 + 日志回调：

```json
{
  "logging": { "level": "debug", "format": "console", "outputPaths": ["sdk.log"] }
}
```

- `disable=true`：整体静默（含回调）
- `level`：`debug` / `info` / `warn` / `error`，缺省 `info`
- `outputPaths`：`["stdout"]` / `["stderr"]` / 文件路径（可多个）
- 注册日志回调后日志优先转发宿主，可在 `Init` 前后注册

```c
// C：同线程安全、不可阻塞
void on_log(const char* level, const char* msg);
GoimSdkSetLogCallback(on_log);
```

```js
// Web：浏览器 wasm 无 stdout，必须经回调输出
goimSdk.setLogCallback((level, msg) => console.log(`[goim-sdk][${level}] ${msg}`));
```

## 本地存储与同步

| 平台 | 实现 |
|---|---|
| iOS / Android / PC | `SQLiteStorage`（gorm + `glebarez/sqlite`，9 张表：消息/会话/好友/黑名单/群/群成员/seq/version/meta） |
| Web | `IdbStorage`（IndexedDB 扁平 row + `dataJSON` 模式，与 SQLite 表结构同构） |
| 未配置 / 单测 | `MemoryStorage` |

- 登录成功后后台执行 `runFullSync`：消息按 seq 增量，实体按 versionID/version 增量
- 本地 `IDHash` 与服务端比对，不一致时服务端返回 `full=true` 触发全量重建
- 推送消息直接落库并触发增量事件，UI 无需等待同步完成

## 错误模型

```go
type SDKError struct {
    Code        int32
    Msg         string
    OperationID string
}
```

`Error()` 返回剥离 `rpc error` 前缀后的原始错误信息；跨语言侧统一序列化为 JSON（`code` / `msg`）。

## 测试

```bash
go test ./pkg/...            # 单元测试（含 storage / sync / transport / call）
go vet ./...

# 集成测试：环境变量缺失时自动 Skip
set GOIM_API_URL=http://host/api/v1
set GOIM_WS_URL=ws://host/ws
set GOIM_SECRET=...
go test ./test/integration -v
```

可选变量：`GOIM_LOG_LEVEL`（日志级别）、`GOIM_RUSTFS_URL`（E8 上传测试）、`GOIM_SHORT_TTL=1`（短 TTL 续期测试）。

## 文档

- [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) — 架构、包结构、开发阶段与跨语言封装细节
- [docs/SYNC_ARCHITECTURE.md](docs/SYNC_ARCHITECTURE.md) — 数据同步引擎设计
- [docs/IDB_STORAGE.md](docs/IDB_STORAGE.md) — wasm IndexedDB 存储桥接
- [docs/INTEGRATION_TEST_PLAN.md](docs/INTEGRATION_TEST_PLAN.md) — 集成测试用例与环境配置
- [docs/INTEGRATION_CONCLUSIONS.md](docs/INTEGRATION_CONCLUSIONS.md) — 集成测试结论

## 版本

版本号权威定义在 `version/VERSION`（`go:embed` 注入，同时随 WS 握手 `sdkVersion` 上报），编译期可覆盖：

```bash
go build -ldflags "-X github.com/PaperMan11/goim-sdk-core/pkg/client.Version=x.y.z" ./...
```

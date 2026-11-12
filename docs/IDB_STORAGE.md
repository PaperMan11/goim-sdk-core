# IndexedDB 存储架构

> 面向 wasm 平台的持久化实现，与 `pkg/storage/sqlite.go`（`//go:build !(js && wasm)`）同构，共享 `Storage` 接口。

---

## 1. 问题背景

| 平台 | 持久化方案 | 选型 |
|---|---|---|
| iOS / Android（gomobile bind） | SQLite | `glebarez/sqlite`（pure-Go，gomobile 社区广泛验证） |
| PC（cgo） | SQLite | 同上 |
| Web（wasm） | **IndexedDB** | glebarez/sqlite 依赖 modernc libc，无 `js/wasm` 后端；IndexedDB 是浏览器唯一通用持久化（50MB+，异步 API） |

核心矛盾：Go `Storage` 接口是**全同步**方法，IndexedDB 是**异步 Promise API**。桥接代价是一层 `awaitPromise` 同步阻塞。

---

## 2. 文件

```
pkg/storage/
├── storage.go            # Storage 接口定义
├── sqlite.go             # SQLiteStorage（gorm + glebarez/sqlite，非 wasm）
├── sqlite_test.go        # //go:build !(js && wasm)
└── idb_storage.go        # //go:build js && wasm — Go 侧 Storage 实现

pkg/client/
├── storage_sqlite.go     # //go:build !(js && wasm) — newSQLiteStorage → storage.NewSQLiteStorage
└── storage_wasm.go       # //go:build js && wasm — newSQLiteStorage → storage.NewIdbStorage()

cmd/wasm/
├── main.go               # //go:build js && wasm — syscall/js 入口（注册全局 goimSdk）
└── idb_wrapper.js        # 浏览器侧 JS 桥接（宿主页面随 wasm 产物一起加载）
```

> `idb_wrapper.js` 放在 `cmd/wasm/`：它与 `goim.wasm`、`wasm_exec.js` 同属 Web 运行时资源，
> 构建产物和宿主 `index.html` 都落在这个目录，一次拷贝即可部署。

**不需要单独目录**。wasm 平台只有这一对实现，和 SQLite 对称，走 build tag 区分即可。`wasm/storage/idb.go` 的原始设计（已废弃）是多此一举。

---

## 3. 架构

```
浏览器页面                                   Go wasm 运行时
┌─────────────────────────────────────────────────────────────────┐
│ <script src="idb_wrapper.js"></script>   ← 先加载（注册 goimIdb）  │
│ <script src="wasm_exec.js"></script>   ← Go wasm 引导            │
│ <script>WebAssembly.instantiateStreaming(...)</script>          │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│   Go Storage 接口（全同步）                                       │
│     └── IdbStorage                                                │
│         ├── op(operation, args)                                   │
│         │     → json.Marshal args                                 │
│         │     → goimIdb.op(operation, argsJSON)                      │
│         │     → awaitPromise(promise)                             │
│         │     → 解析 {"ok":true/false, "data"/"error"}            │
│         │                                                         │
│         └── awaitPromise(promise)                                 │
│               ├── js.FuncOf(then)  → resultCh                     │
│               ├── js.FuncOf(catch) → errCh                        │
│               └── select { case <-resultCh: return; case <-errCh: error } │
│                                                                   │
│   JS goimIdb.op(operation, argsJSON)                                  │
│     └── OPS[operation](db, args)                                  │
│           → runTx(db, store(s), mode, asyncFn)                   │
│             ├── db.transaction(storeNames, mode)                  │
│             ├── Promise.resolve(asyncFn(tx, ...storeRefs))       │
│             │     └── reqPromise(IDBRequest).then(...)  → 串行 await │
│             └── tx.oncomplete → resolve(result)                   │
│                                                                   │
│   IndexedDB API                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 3.1 awaitPromise：同步阻塞等待 Promise

**为什么安全**：wasm 运行时是单线程事件循环。`js.FuncOf` 注册的回调会被调度到微任务队列，在 `select` 阻塞的同一个 goroutine 上执行。不会出现"回调永远无法被调度"的死锁。

```go
func awaitPromise(promise js.Value) (js.Value, error) {
    resultCh := make(chan js.Value, 1)
    errCh    := make(chan string, 1)

    then := js.FuncOf(func(this js.Value, args []js.Value) any {
        resultCh <- args[0]; return nil
    })
    catch := js.FuncOf(func(this js.Value, args []js.Value) any {
        errCh <- args[0].String(); return nil
    })
    defer then.Release()
    defer catch.Release()

    promise.Call("then", then)
    promise.Call("catch", catch)

    select {
    case v := <-resultCh:  return v, nil
    case msg := <-errCh:   return js.Undefined(), fmt.Errorf("js promise rejected: %s", msg)
    }
}
```

JS 侧 `op` 已把所有异常 catch 成 `{"ok":false,"error":"..."}` 正常返回，所以 Go 侧几乎不会走 `catch` 分支——只有 JS Promise reject（idb_wrapper.js 内部代码 bug）才触发。

### 3.2 runTx：JS 侧事务封装

IndexedDB 事务在当前微任务队列空时自动 commit，**不能在事务回调里 await Promise-wrapped 请求**（否则请求还没发出去事务就已经结束了）。

解决方案：用 `Promise.resolve(asyncFn(...)).then(...)` 把 asyncFn 内部的 await 串起来，`runTx` 返回的 Promise 等 asyncFn 内部所有 reqPromise 完成后才 resolve——此时事务一定还活着。

```js
function runTx(db, stores, mode, asyncFn) {
  return new Promise((resolve, reject) => {
    const tx = db.transaction(stores, mode);
    const refs = stores.map(n => tx.objectStore(n));
    let result, failed = false;
    tx.oncomplete = () => { if (!failed) resolve(result); };
    tx.onerror    = () => { failed = true; reject(tx.error); };
    // Promise.resolve 包一层，asyncFn 内所有 await 会按序串行
    Promise.resolve(asyncFn(tx, ...refs)).then(
      (v) => { result = v; },
      (e) => { failed = true; reject(e); }
    );
  });
}
```

---

## 4. Store 设计

9 个 object store 与 SQLiteStorage 的 8 张 gorm 表**完全同构**（加一个 meta 替代 sdk_meta 表）。

| IndexedDB Store | keyPath | 对应 SQLite 表 | 存什么 |
|---|---|---|---|
| `seqs` | `[owner]` | `seqs` (owner PK) | `{owner, maxSeqsJSON, minSeqsJSON}` — 游标 map 序列化成字符串 |
| `versions` | `[owner, entity]` | `sync_versions` (owner+entity UK) | `{owner, entity, versionID, version}` |
| `messages` | `[owner, conversationID, seq]` | `chat_logs` (owner+conv+seq UK) | 扁平 row（见 §4.1） |
| `conversations` | `[owner, conversationID]` | `conversations` | 扁平 row（见 §4.1） |
| `friends` | `[owner, friendUserID]` | `friend_infos` | 扁平 row（见 §4.1） |
| `blacks` | `[owner, blackUserID]` | `black_infos` | 扁平 row（见 §4.1） |
| `groups` | `[owner, groupID]` | `group_infos` | 扁平 row（见 §4.1） |
| `groupMembers` | `[owner, groupID, userID]` | `group_members` | 扁平 row（见 §4.1） |
| `meta` | `[key]` | `sdk_meta` | `{key, value}` — SDK 级配置，**无 owner**（跨账号共享，如 device_id） |

### 4.1 扁平 row + dataJSON 模式

IndexedDB **复合主键不能深入嵌套对象**（keyPath 只能指向顶层属性），而 `FriendInfo.FriendUserID()` 实际取的是 `friendUser.userID`。解决方法：

- **写入时**：Go 侧提前提取查询所需的 key 列（如 `friendUserID: f.FriendUserID()`），和完整 model 的 `json.Marshal` 结果一起组装成扁平 row
- **读取时**：Go 侧反序列化 row 的 `dataJSON` 字段还原完整 model

示例（friends store）：

```
IndexedDB row:
{
  owner:        "u1",              ← keyPath[0]
  friendUserID: "u2",              ← keyPath[1]（提前从 FriendUser.userID 提取）
  dataJSON:     "{...完整 FriendInfo JSON...}"
}
```

这与 SQLiteStorage 的 `data_json` 列设计完全一致（`chat_logs.data_json` 存完整 `MsgData`）。

### 4.2 meta store 例外

`meta` store 存 SDK 级配置，**不加 owner 字段**。`keyPath = ["key"]`，存储全局唯一键：

| key | value | 来源 |
|---|---|---|
| `device_id` | UUID 字符串 | `NewClient` 首次启动生成并持久化 |

---

## 5. Go ↔ JS 契约

### 5.1 goimIdb.open()

```js
// 预热 db 连接（首次触发 onupgradeneeded 创建 store），返回 null
async function open() { await openDB(); return null; }
```

`openDB()` 内部有 `_dbPromise` 单例缓存，后续 `op` 直接复用。IndexedDB 连接是轻量的（每个事件循环最多维护 1 个连接），不需要句柄表。

### 5.2 goimIdb.op(operation, argsJSON)

```js
// 返回 Promise<string> —— JSON 字符串
// 成功：{"ok": true,  "data": <结果 JSON 或 null>}
// 失败：{"ok": false, "error": "人类可读错误信息"}
async function op(operation, argsJSON) { ... }
```

`argsJSON` 是 Go 侧 `json.Marshal(args)` 生成的 JSON 字符串，JS 侧 `JSON.parse` 后作为 `args` 参数传给具体 handler。

### 5.3 operation 枚举

共 34 个 operation，按 store 分组：

**元数据（seqs / versions / meta）**
- `getMaxSeqs` / `setMaxSeqs`
- `getVersion` / `setVersion` / `listVersions`
- `getMeta` / `setMeta`

**messages**
- `upsertMsgs` — 批量写，args: `{rows: [...]}`（扁平 row）
- `listMsgsByConversation` — 按 owner + conversationID 拉全量，按 seq 升序
- `deleteMsgs` — 按精确 seq 列表删
- `markMsgsRead` — 按精确 seq 列表置 `isRead=true`
- `markMsgsReadUpTo` — 按 `seq <= hasReadSeq` 条件置 `isRead=true`（条件过滤）
- `markMsgsRevoked` — 按精确 seq 列表置 `status=4`（`MsgStatusRevoked`，与 `pkg/model/const.go` 对齐）

**conversations / friends / blacks / groups / groupMembers** — 各自对应 `upsert*` / `delete*` / `list*`（list 完整 model，返回扁平 row 列表）和 `list*IDs`（只返回 ID 字符串数组，避免 Go 侧反序列化完整 model）

**生命周期**
- `clearOwner` — 清掉指定 owner 在 seqs / versions / messages / conversations / friends / blacks / groups / groupMembers 里的全部记录，**保留 meta**（SDK 级数据跨账号共享）

### 5.4 List vs listIDs 的区别

| 操作 | Go 返回类型 | JS 返回 | 场景 |
|---|---|---|---|
| `listFriends` | `[]*model.FriendInfo` | 完整扁平 row 列表 | 同步引擎拉取后需要反序列化完整 model |
| `listFriendIDs` | `[]string` | 只含 `friendUserID` 列的 JSON 数组 | 同步引擎做 diff 只需要 ID，省一次反序列化 |

---

## 6. 使用

### 6.1 页面加载顺序

```html
<!-- 1. 业务脚本（可选，用 Go 对象暴露的 wasm main） -->
<script src="idb_wrapper.js"></script>          <!-- ← goimIdb 全局对象注册 -->
<script src="wasm_exec.js"></script>            <!-- ← Go wasm 引导库 -->
<script>
  const go = new Go();
  WebAssembly.instantiateStreaming(
    fetch('goim.wasm'),
    go.importObject
  ).then(({ instance }) => {
    go.run(instance);
    // 此时全局 goimSdk 已可用（wasm/cmd/main.go 注册）
    goimSdk.init(JSON.stringify({
      baseURL: 'https://api.goim.io/api/v1',
      endpoint: 'wss://api.goim.io:10001/ws',
      sessionDataPath: 'any-non-empty-string',   // ← wasm 下自动走 IndexedDB
    }));
  });
</script>
```

顺序不能反——`wasm_exec.js` 和 `goim.wasm` 初始化时，Go 侧 `NewIdbStorage()` 需要在 `js.Global()` 找到 `goimIdb`。

### 6.2 Go 侧调用

**自动生效（推荐）**：

```go
// wasm build 下 newSQLiteStorage 会自动走 NewIdbStorage()
client.NewClient(&client.ClientConfig{
    BaseURL:        "...",
    Endpoint:       "...",
    SessionDataPath: "web",  // wasm 下被忽略，触发 IndexedDB 持久化
})
```

**手动注入**：

```go
s, err := storage.NewIdbStorage()
client.NewClient(&client.ClientConfig{
    Storage: s,
    ...
})
```

### 6.3 浏览器 DevTools 调试

Chrome DevTools → Application → Storage → IndexedDB：

```
Database: goim_sdk
├── seqs          (按 owner 查看游标)
├── versions      (按 owner+entity 查看实体游标)
├── messages      (完整 MsgData JSON 藏在 dataJSON 列)
├── conversations
├── friends
├── blacks
├── groups
├── groupMembers
└── meta          (查看 device_id 是否持久化)
```

---

## 7. 事务模型

| 场景 | 事务范围 | 说明 |
|---|---|---|
| `upsertMsgs` | `messages` (readwrite) | 批量写在单事务里，原子性保证 |
| `markMsgsRead` | `messages` (read) → `messages` (readwrite) | 先全量拉取 → 修改 dataJSON → 全量写回。**跨两个事务**，理论上有竞态窗口（另一个 goroutine 同时写入），但 wasm 单线程串行，实际不会发生 |
| `deleteGroups` | `groups` + `groupMembers` (readwrite) | 多 store 单事务——删 group 同时删其所有成员 |
| `clearOwner` | 8 个 store 单事务 | 保留 meta |

### 竞态安全性

wasm 单线程事件循环 → 所有 Go 方法调用在主 goroutine 串行执行。虽然 IndexedDB 事务是异步的，但 Go 侧 `awaitPromise` 阻塞直到事务完成才返回。**不存在两个 Go goroutine 同时持有同一个 IndexedDB 事务的情况**。

---

## 8. 与 SQLiteStorage 的差异

| 维度 | SQLiteStorage | IdbStorage |
|---|---|---|
| 存储引擎 | glebarez/sqlite（pure-Go SQLite） | 浏览器 IndexedDB |
| 表 vs store | 8 张 gorm 表 | 9 个 object store（多 meta） |
| 写入 API | gorm `Create` / `Update` | `store.put(row)` |
| 读取 API | gorm `Find` / `Where` | `store.getAll(keyRange)` |
| 条件查询 | SQL `WHERE` | `IDBKeyRange` 或**全量拉取后 JS 侧 filter** |
| 事务 | gorm `Transaction` → 单连接同步 | `db.transaction(stores, mode)` → async + `awaitPromise` 同步 |
| dataJSON 列 | ✅ gorm `data_json` 字段 | ✅ 扁平 row 的 `dataJSON` 顶层属性 |
| 条件 markMsgsRead | `UPDATE ... WHERE seq IN (...)` SQL | 全量拉取 → 条件修改 dataJSON → 全量写回（跨两事务，但安全） |
| 跨账号 | SQLite 文件可多账号复用（owner 区分） | IndexedDB 按 DB_NAME 全局共享，owner 是主键一部分 |

---

## 9. 已知限制与 TODO

| # | 限制 | 影响 | 计划 |
|---|---|---|---|
| L1 | **条件 mark 用全量拉取+写回** | 消息量大时（>10000）慢 | 加 IndexedDB 索引 `[owner, conversationID, seq]`，用 `openCursor` 游标遍历只修改命中项 |
| L2 | **db 连接无缓存** | `_dbPromise` 只缓存一次，但每次 op 都 `openDB()` | 实测轻量，暂时不动 |
| L3 | **未封装为 Promise 风格** | JS 原生 Promise 在 Go 侧用 channel 阻塞，调试体验差 | wasm main.go 暴露一个 `dumpDB(operation)` 调试方法 |
| L4 | **无 migration 机制** | DB_VERSION 硬编码 1，后续加新 store 时需 bump 版本 | 在 `idb_wrapper.js` `openDB()` 里加 `onupgradeneeded` 逻辑 |
| L5 | **`markMsgsReadUpTo` 条件过滤** | JS 侧 `seq <= hasReadSeq` 条件，Go 侧传 `hasReadSeq` 参数 | 已实现（L1 改善游标遍历） |
| L6 | ~~真机浏览器测试~~ | 已通过端到端验证（见 §11） | ✅ 已完成 |

---

## 11. 端到端验证（2026-11-12 ✅）

### 11.1 构建与运行

```powershell
# 1) 构建 wasm（产物被 .gitignore 忽略）；Windows PowerShell 写法
New-Item -ItemType Directory -Force cmd\wasm\dist | Out-Null
$env:GOOS="js"; $env:GOARCH="wasm"
go build -o cmd\wasm\dist\goim.wasm ./cmd/wasm
Copy-Item "$(go env GOROOT)\lib\wasm\wasm_exec.js" cmd\wasm\dist\wasm_exec.js -Force
# Go 1.26 的 wasm_exec.js 在 lib/wasm（早期版本在 misc/wasm）

# 2) 起本地静态服务（wasm 不能用 file:// 加载；.wasm 必须是 application/wasm）
#    在项目根目录执行，无需安装依赖
node -e "const h=require('http'),f=require('fs'),p=require('path');const root='cmd/wasm';h.createServer((q,s)=>{const u=q.url.split('?')[0];const fp=p.join(root,u==='/'?'index.html':u);f.readFile(fp,(e,d)=>{if(e){s.writeHead(404);return s.end('not found')}const m={'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.wasm':'application/wasm'}[p.extname(fp)]||'application/octet-stream';s.writeHead(200,{'Content-Type':m,'Cache-Control':'no-store'});s.end(d)})}).listen(8765,'127.0.0.1',()=>console.log('http://127.0.0.1:8765/index.html'))"

# 3) 浏览器打开 http://127.0.0.1:8765/index.html
#    页面加载后自动跑 10 项自测，结果写 window.__selftest
```

**页面判读**：

- 顶部摘要变绿并显示 `全部通过（10 项）` → 全过；出现红底 `存在失败项` 或 `自测异常` → 看表格哪一行是 FAIL
- 表格逐行列出 10 项，右侧为实际值（如 `device_id = xxxx-...`、9 个 store 名）
- DevTools → Application → IndexedDB → `goim_sdk`，应看到 9 个 store，`meta` 里有 `device_id`
- 控制台可手动验证优雅退出：`goimSdk.shutdown(() => console.log('shutdown done'))`（执行后实例退出，需刷新页面重新加载）

`cmd/wasm/index.html` 是自测/演示页，script 顺序：`idb_wrapper.js` → `dist/wasm_exec.js` → 实例化 `dist/goim.wasm`。
Node 侧自测脚本额外覆盖 2 项 shutdown 验证（页面不自动跑：`shutdown` 会结束 Go 运行时，实例不可再用）。

### 11.2 覆盖项（12 项，Node 全跑通过；浏览器覆盖前 10 项）

| # | 检查项 | 结论 |
|---|---|---|
| 1 | goimIdb 桥接脚本注册 | ✅ |
| 2 | `init` 构建 Client（IdbStorage） | ✅ 9 个 store 自动建表 |
| 3 | `device_id` 写入 meta store | ✅ 真实 UUID 落库 |
| 4 | 再次 init 读回同一 device_id | ✅ 持久化读路径生效 |
| 5 | `status()` | ✅ = 0 |
| 6 | `syncCall(Status)` | ✅ `{"code":0,"data":"0"}` |
| 7 | `syncCall(SyncState)` | ✅ `{"code":0,"data":"0"}` |
| 8 | 未知方法归一化 | ✅ `{"code":90012,...}` |
| 9 | 未登录拦截 | ✅ `{"code":90011,"msg":"not logged in"}` |
| 10 | `subscribe(2001)` / `unsubscribe` | ✅ 返回订阅 id |
| 11 | `shutdown(onDone)` 回调 | ✅ onDone 已触发（仅 Node 脚本） |
| 12 | `go.run` resolve（运行时正常退出） | ✅ main 已返回（仅 Node 脚本） |

浏览器侧真实 IndexedDB 的 store 列表：`blacks, conversations, friends, groupMembers, groups, messages, meta, seqs, versions`（9 个）。

### 11.3 验证中发现并修复的 3 个缺陷

| # | 缺陷 | 现象 | 修复 |
|---|---|---|---|
| D1 | `cmd/wasm/main.go` 用 `select {}` 常驻 | Go 1.26 wasm runtime 判定 `all goroutines are asleep - deadlock!` 并退出，`js.FuncOf` 全部失效 | 改为 timer 保活 + `shutdownCh` 退出开关（`select` 内 `time.After(24h)` 循环重设；纯 channel 阻塞仍会判死锁） |
| D2 | `IdbStorage.op` 把 JS 返回的 **JSON 字符串**当对象用 | `result.Get("ok")` 恒为 undefined → 所有 IndexedDB 操作误报失败（`GetMeta`/`SetMeta` 吞错导致 init 表面成功） | 解析信封 `{"ok","data","error"}` 后再 `JSON.parse(data)` 取 `js.Value` |
| D3 | `goimSdk.init` 是 JS 同步调用，内部阻塞等 IndexedDB Promise | JS 主线程卡在 wasm 调用栈上无法跑微任务，双向等待死锁（表现为 `init` 返回 undefined） | `init` 改为回调式 `init(configJSON, onSuccess, onError)`，Go 侧在 goroutine 里执行 |

> D3 的通用约束：**wasm 侧任何会阻塞的导出函数都不能是同步的**。`syncCall` 因此只适用于纯本地方法（`Status` / `SyncState` / `Methods`），网络类调用必须走异步 `call`。已在 `cmd/wasm/main.go` 文档注释中标注。

### 11.4 剩余未验证项

| 项 | 说明 |
|---|---|
| 真实服务端登录链路（wasm） | 需要可达的 goim 服务端 + WS，验证 `login`/`sendMsg` 与消息同步落库 |
| 跨刷新持久化 | 关闭页面再打开，`device_id` 应保持不变（当前已验证同进程内二次 init 读回） |
| 大数据量事务 | 万级消息批量 `upsertMsgs` 的耗时与事务分片 |

---

## 12. 关键事实速记

| 问题 | 答案 |
|---|---|
| wasm 为什么不用 SQLite？ | glebarez/sqlite（pure-Go）依赖 modernc libc，没有 js/wasm 后端 |
| 同步接口怎么实现？ | `awaitPromise` 用 `js.FuncOf` + channel 阻塞 JS Promise |
| store 怎么对应 gorm 表？ | 1:1 同构，加一个 meta（无 owner） |
| 扁平 row 为什么不加嵌套？ | IndexedDB keyPath 不能深入嵌套对象 |
| 事务能 await 吗？ | 用 `Promise.resolve(asyncFn(...))` 包装，保证所有请求在事务内串行 |
| 竞态窗口？ | wasm 单线程串行执行 Go 方法，不存在竞态 |
| meta 为什么无 owner？ | device_id 等 SDK 级配置跨账号共享 |
| 页面 script 顺序？ | `idb_wrapper.js` → `wasm_exec.js` → wasm 实例化 |
| 为什么 `init` 是回调式？ | Go 内等待 IndexedDB Promise 时 JS 主线程必须能跑微任务（§11.3 D3） |
| wasm 常驻为什么用 timer 而不是 channel？ | checkdead 只认 timer/netpoll/可运行 goroutine；纯 channel 阻塞会 fatal deadlock |
| 单次 timer 上限？ | JS `setTimeout` 最多 2^31-1 ms（≈24.8 天），超限会被当 1ms 触发 → 用循环重设 |

// goim-sdk-core IndexedDB 桥接层 —— 扁平 row + dataJSON 模式（对齐 SQLiteStorage gorm 表）
//
// 用法：页面里先加载此脚本（在 wasm_exec.js / goim.wasm 之前）：
//
//   <script src="idb_wrapper.js"></script>
//   <script src="wasm_exec.js"></script>
//   <script>const go = new Go(); WebAssembly.instantiateStreaming(...)
//
// 全局暴露 goimIdb：
//   open() → Promise(null)  预热 db 连接（首次触发 onupgradeneeded）
//   op(operation, argsJSON) → Promise<string resultJSON>
//
// 返回 JSON：{"ok":true,"data":<结果>} 或 {"ok":false,"error":"..."}
//
// Store 设计（与 SQLiteStorage gorm 表模型对齐）：
//   seqs            keyPath=["owner"]                       {owner, maxSeqsJSON, minSeqsJSON}
//   versions        keyPath=["owner","entity"]              {owner, entity, versionID, version}
//   messages        keyPath=["owner","conversationID","seq"]  {owner, conversationID, seq, clientMsgID, dataJSON}
//   conversations   keyPath=["owner","conversationID"]       {owner, conversationID, latestMsgSendTime, dataJSON}
//   friends         keyPath=["owner","friendUserID"]        {owner, friendUserID, dataJSON}
//   blacks          keyPath=["owner","blackUserID"]         {owner, blackUserID, dataJSON}
//   groups          keyPath=["owner","groupID"]             {owner, groupID, dataJSON}
//   groupMembers    keyPath=["owner","groupID","userID"]    {owner, groupID, userID, dataJSON}
//   meta            keyPath=["key"]                         {key, value}

(function (global) {
  const DB_NAME = "goim_sdk";
  // v2：Local Data API P2 —— 补索引（messages 按 clientMsgID 直读、conversations 按时间排序），
  // 仅创建缺失的 store/index，不重建已有 store。
  const DB_VERSION = 2;

  const STORES = [
    { name: "seqs",          keyPath: ["owner"] },
    { name: "versions",      keyPath: ["owner", "entity"] },
    { name: "messages",      keyPath: ["owner", "conversationID", "seq"], indexes: [
        { name: "byClientMsgID", keyPath: ["owner", "conversationID", "clientMsgID"] },
    ] },
    { name: "conversations", keyPath: ["owner", "conversationID"], indexes: [
        { name: "byLatestMsgSendTime", keyPath: ["owner", "latestMsgSendTime"] },
    ] },
    { name: "friends",       keyPath: ["owner", "friendUserID"] },
    { name: "blacks",        keyPath: ["owner", "blackUserID"] },
    { name: "groups",        keyPath: ["owner", "groupID"] },
    { name: "groupMembers",  keyPath: ["owner", "groupID", "userID"] },
    { name: "meta",          keyPath: ["key"] },
  ];

  // MsgStatusRevoked 与 pkg/model/const.go 对齐
  const MsgStatusRevoked = 4;

  let _dbPromise = null;
  function openDB() {
    if (_dbPromise) return _dbPromise;
    _dbPromise = new Promise((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, DB_VERSION);
      req.onupgradeneeded = (ev) => {
        const db = ev.target.result;
        const tx = req.transaction;
        for (const s of STORES) {
          let store;
          if (!db.objectStoreNames.contains(s.name)) {
            store = db.createObjectStore(s.name, { keyPath: s.keyPath });
          } else {
            store = tx.objectStore(s.name);
          }
          for (const idx of s.indexes || []) {
            if (!store.indexNames.contains(idx.name)) {
              store.createIndex(idx.name, idx.keyPath, { unique: !!idx.unique });
            }
          }
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => { _dbPromise = null; reject(req.error); };
    });
    return _dbPromise;
  }

  async function open() { await openDB(); return null; }

  async function op(operation, argsJSON) {
    try {
      const args = JSON.parse(argsJSON || "{}");
      const db = await openDB();
      const handler = OPS[operation];
      if (!handler) {
        return JSON.stringify({ ok: false, error: "unknown operation: " + operation });
      }
      const data = await handler(db, args);
      return JSON.stringify({ ok: true, data: data == null ? null : data });
    } catch (err) {
      return JSON.stringify({ ok: false, error: String(err && err.message || err) });
    }
  }

  // —— 事务辅助：用 Promise.resolve 包裹 asyncFn，保证 await 在 tx 完成前返回 ——
  function runTx(db, stores, mode, asyncFn) {
    return new Promise((resolve, reject) => {
      if (!Array.isArray(stores)) stores = [stores];
      const tx = db.transaction(stores, mode);
      const refs = stores.map((n) => tx.objectStore(n));
      let result, failed = false;
      tx.oncomplete = () => { if (!failed) resolve(result); };
      tx.onerror = () => { failed = true; reject(tx.error); };
      Promise.resolve(asyncFn(tx, ...refs)).then((v) => { result = v; }, (e) => { failed = true; reject(e); });
    });
  }

  function reqPromise(req) {
    return new Promise((resolve, reject) => {
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  // 游标顺序收集至多 max 条（用于复合键分页，避免 getAll 全量）
  function collectCursor(req, max) {
    return new Promise((resolve, reject) => {
      const rows = [];
      req.onerror = () => reject(req.error);
      req.onsuccess = () => {
        const cur = req.result;
        if (!cur) { resolve(rows); return; }
        rows.push(cur.value);
        if (rows.length >= max) { resolve(rows); return; }
        cur.continue();
      };
    });
  }

  // owner 前缀范围（双/三主键 store 通用）
  function ownerRange(owner) {
    return IDBKeyRange.bound([owner, ""], [owner, "zzzzzzzz"]);
  }

  // 搜索：缓存 JSON 文本大小写不敏感子串匹配（与 SQLite LOWER(data_json) LIKE 一致）
  function rowsMatching(rows, keyword) {
    const kw = (keyword || "").toLowerCase();
    if (!kw) return [];
    return rows.filter((r) => (r.dataJSON || "").toLowerCase().includes(kw));
  }

  // —— 操作实现 ——

  const OPS = {
    // ===== seqs =====
    async getMaxSeqs(db, args) {
      return runTx(db, "seqs", "readonly", async (tx, s) => {
        const row = await reqPromise(s.get([args.owner]));
        if (!row) return null;
        return { maxSeqsJSON: row.maxSeqsJSON || "", minSeqsJSON: row.minSeqsJSON || "" };
      });
    },
    async setMaxSeqs(db, args) {
      return runTx(db, "seqs", "readwrite", async (tx, s) => {
        s.put({ owner: args.owner, maxSeqsJSON: args.maxSeqsJSON || "", minSeqsJSON: args.minSeqsJSON || "" });
        return null;
      });
    },

    // ===== versions =====
    async getVersion(db, args) {
      return runTx(db, "versions", "readonly", async (tx, s) => {
        const row = await reqPromise(s.get([args.owner, args.entity]));
        if (!row) return null;
        return { versionID: row.versionID || "", version: row.version || 0 };
      });
    },
    async setVersion(db, args) {
      return runTx(db, "versions", "readwrite", async (tx, s) => {
        s.put({ owner: args.owner, entity: args.entity, versionID: args.versionID || "", version: args.version || 0 });
        return null;
      });
    },
    async listVersions(db, args) {
      return runTx(db, "versions", "readonly", async (tx, s) => {
        return reqPromise(s.getAll(IDBKeyRange.bound([args.owner, ""], [args.owner, "zzzzzzzz"])));
      });
    },

    // ===== 消息 =====
    async upsertMsgs(db, args) {
      return runTx(db, "messages", "readwrite", async (tx, s) => {
        for (const row of args.rows || []) s.put(row);
        return null;
      });
    },
    async listMsgsByConversation(db, args) {
      return runTx(db, "messages", "readonly", async (tx, s) => {
        const rows = await reqPromise(s.getAll(IDBKeyRange.bound(
          [args.owner, args.conversationID, -Infinity],
          [args.owner, args.conversationID, Infinity]
        )));
        rows.sort((a, b) => a.seq - b.seq);
        return rows;
      });
    },
    async getLatestMsg(db, args) {
      return runTx(db, "messages", "readonly", async (tx, s) => {
        const rows = await reqPromise(s.getAll(IDBKeyRange.bound(
          [args.owner, args.conversationID, -Infinity],
          [args.owner, args.conversationID, Infinity]
        )));
        if (!rows.length) return null;
        let latest = rows[0];
        for (const row of rows) {
          if (row.seq > latest.seq) latest = row;
        }
        return latest;
      });
    },
    // 复合键分页：desc 取 seq<=seq（seq<=0 取最新），asc 取 seq>seq（seq<=0 从头）
    async listMsgsPage(db, args) {
      return runTx(db, "messages", "readonly", async (tx, s) => {
        const limit = args.limit > 0 ? args.limit : 30;
        let range, dir;
        if (args.desc) {
          range = IDBKeyRange.bound(
            [args.owner, args.conversationID, -Infinity],
            [args.owner, args.conversationID, args.seq > 0 ? args.seq : Infinity]
          );
          dir = "prev";
        } else {
          range = IDBKeyRange.bound(
            [args.owner, args.conversationID, args.seq > 0 ? args.seq : -Infinity],
            [args.owner, args.conversationID, Infinity],
            args.seq > 0,
            false
          );
          dir = "next";
        }
        return collectCursor(s.openCursor(range, dir), limit + 1);
      });
    },
    async getMsg(db, args) {
      return runTx(db, "messages", "readonly", async (tx, s) => {
        const row = await reqPromise(s.get([args.owner, args.conversationID, args.seq]));
        return row || null;
      });
    },
    async getMsgByClientID(db, args) {
      return runTx(db, "messages", "readonly", async (tx, s) => {
        const rows = await reqPromise(s.index("byClientMsgID").getAll(
          IDBKeyRange.only([args.owner, args.conversationID, args.clientMsgID])
        ));
        return rows.length ? rows[0] : null;
      });
    },
    async searchMsgs(db, args) {
      return runTx(db, "messages", "readonly", async (tx, s) => {
        const rows = await reqPromise(s.getAll(IDBKeyRange.bound(
          [args.owner, args.conversationID, -Infinity],
          [args.owner, args.conversationID, Infinity]
        )));
        const matched = rowsMatching(rows, args.keyword);
        matched.sort((a, b) => b.seq - a.seq);
        const limit = args.limit > 0 ? args.limit : 30;
        return matched.slice(0, limit + 1);
      });
    },
    async countMsgs(db, args) {
      return runTx(db, "messages", "readonly", async (tx, s) => {
        const range = args.conversationID
          ? IDBKeyRange.bound(
              [args.owner, args.conversationID, -Infinity],
              [args.owner, args.conversationID, Infinity]
            )
          : IDBKeyRange.bound(
              [args.owner, "", -Infinity],
              [args.owner, "zzzzzzzz", Infinity]
            );
        return reqPromise(s.count(range));
      });
    },
    async deleteMsgs(db, args) {
      return runTx(db, "messages", "readwrite", async (tx, s) => {
        for (const seq of args.seqs || []) s.delete([args.owner, args.conversationID, seq]);
        return null;
      });
    },
    // —— mutate 类操作：读 → 修改 dataJSON → 写回 ——
    async markMsgsRead(db, args) { return mutateMsgs(db, args, (m) => { m.isRead = true; }); },
    async markMsgsReadUpTo(db, args) {
      return mutateMsgs(db, args, (m) => { if (m.seq <= args.hasReadSeq) m.isRead = true; });
    },
    async markMsgsRevoked(db, args) {
      return mutateMsgs(db, args, (m) => { m.status = MsgStatusRevoked; });
    },

    // ===== 会话 =====
    async upsertConversations(db, args) {
      return runTx(db, "conversations", "readwrite", async (tx, s) => {
        for (const row of args.rows || []) s.put(row);
        return null;
      });
    },
    async deleteConversations(db, args) {
      return runTx(db, "conversations", "readwrite", async (tx, s) => {
        for (const id of args.conversationIDs || []) s.delete([args.owner, id]);
        return null;
      });
    },
    async getConversation(db, args) {
      return runTx(db, "conversations", "readonly", async (tx, s) => {
        const row = await reqPromise(s.get([args.owner, args.conversationID]));
        return row || null;
      });
    },
    async listConversations(db, args) {
      return runTx(db, "conversations", "readonly", async (tx, s) => {
        const rows = await reqPromise(s.getAll(IDBKeyRange.bound([args.owner, ""], [args.owner, "zzzzzzzz"])));
        rows.sort((a, b) => (b.latestMsgSendTime || 0) - (a.latestMsgSendTime || 0));
        return rows;
      });
    },
    async listConversationIDs(db, args) {
      return runTx(db, "conversations", "readonly", async (tx, s) => {
        return reqPromise(s.getAll(IDBKeyRange.bound([args.owner, ""], [args.owner, "zzzzzzzz"])));
      });
    },
    async searchConversations(db, args) {
      return runTx(db, "conversations", "readonly", async (tx, s) => {
        const rows = await reqPromise(s.getAll(ownerRange(args.owner)));
        return rowsMatching(rows, args.keyword);
      });
    },
    async countConversations(db, args) {
      return runTx(db, "conversations", "readonly", (tx, s) => reqPromise(s.count(ownerRange(args.owner))));
    },

    // ===== 好友 =====
    async upsertFriends(db, args) {
      return runTx(db, "friends", "readwrite", async (tx, s) => {
        for (const row of args.rows || []) s.put(row);
        return null;
      });
    },
    async deleteFriends(db, args) {
      return runTx(db, "friends", "readwrite", async (tx, s) => {
        for (const id of args.friendUserIDs || []) s.delete([args.owner, id]);
        return null;
      });
    },
    async listFriends(db, args) {
      return runTx(db, "friends", "readonly", async (tx, s) => {
        return reqPromise(s.getAll(IDBKeyRange.bound([args.owner, ""], [args.owner, "zzzzzzzz"])));
      });
    },
    async listFriendIDs(db, args) {
      return runTx(db, "friends", "readonly", async (tx, s) => {
        return reqPromise(s.getAll(IDBKeyRange.bound([args.owner, ""], [args.owner, "zzzzzzzz"])));
      });
    },
    async searchFriends(db, args) {
      return runTx(db, "friends", "readonly", async (tx, s) => {
        const rows = await reqPromise(s.getAll(ownerRange(args.owner)));
        return rowsMatching(rows, args.keyword);
      });
    },
    async countFriends(db, args) {
      return runTx(db, "friends", "readonly", (tx, s) => reqPromise(s.count(ownerRange(args.owner))));
    },

    // ===== 黑名单 =====
    async upsertBlacks(db, args) {
      return runTx(db, "blacks", "readwrite", async (tx, s) => {
        for (const row of args.rows || []) s.put(row);
        return null;
      });
    },
    async deleteBlacks(db, args) {
      return runTx(db, "blacks", "readwrite", async (tx, s) => {
        for (const id of args.blackUserIDs || []) s.delete([args.owner, id]);
        return null;
      });
    },
    async listBlacks(db, args) {
      return runTx(db, "blacks", "readonly", async (tx, s) => {
        return reqPromise(s.getAll(IDBKeyRange.bound([args.owner, ""], [args.owner, "zzzzzzzz"])));
      });
    },
    async listBlackIDs(db, args) {
      return runTx(db, "blacks", "readonly", async (tx, s) => {
        return reqPromise(s.getAll(IDBKeyRange.bound([args.owner, ""], [args.owner, "zzzzzzzz"])));
      });
    },
    async countBlacks(db, args) {
      return runTx(db, "blacks", "readonly", (tx, s) => reqPromise(s.count(ownerRange(args.owner))));
    },

    // ===== 群 =====
    async upsertGroups(db, args) {
      return runTx(db, "groups", "readwrite", async (tx, s) => {
        for (const row of args.rows || []) s.put(row);
        return null;
      });
    },
    async deleteGroups(db, args) {
      return runTx(db, ["groups", "groupMembers"], "readwrite", async (tx, gs, ms) => {
        for (const id of args.groupIDs || []) {
          gs.delete([args.owner, id]);
          ms.delete(IDBKeyRange.bound([args.owner, id, ""], [args.owner, id, "zzzzzzzz"]));
        }
        return null;
      });
    },
    async listGroups(db, args) {
      return runTx(db, "groups", "readonly", async (tx, s) => {
        return reqPromise(s.getAll(IDBKeyRange.bound([args.owner, ""], [args.owner, "zzzzzzzz"])));
      });
    },
    async listGroupIDs(db, args) {
      return runTx(db, "groups", "readonly", async (tx, s) => {
        return reqPromise(s.getAll(IDBKeyRange.bound([args.owner, ""], [args.owner, "zzzzzzzz"])));
      });
    },
    async searchGroups(db, args) {
      return runTx(db, "groups", "readonly", async (tx, s) => {
        const rows = await reqPromise(s.getAll(ownerRange(args.owner)));
        return rowsMatching(rows, args.keyword);
      });
    },
    async countGroups(db, args) {
      return runTx(db, "groups", "readonly", (tx, s) => reqPromise(s.count(ownerRange(args.owner))));
    },

    // ===== 群成员 =====
    async upsertGroupMembers(db, args) {
      return runTx(db, "groupMembers", "readwrite", async (tx, s) => {
        for (const row of args.rows || []) s.put(row);
        return null;
      });
    },
    async deleteGroupMembers(db, args) {
      return runTx(db, "groupMembers", "readwrite", async (tx, s) => {
        for (const uid of args.userIDs || []) s.delete([args.owner, args.groupID, uid]);
        return null;
      });
    },
    async listGroupMembers(db, args) {
      return runTx(db, "groupMembers", "readonly", async (tx, s) => {
        return reqPromise(s.getAll(IDBKeyRange.bound(
          [args.owner, args.groupID, ""],
          [args.owner, args.groupID, "zzzzzzzz"]
        )));
      });
    },
    async listGroupMemberIDs(db, args) {
      return runTx(db, "groupMembers", "readonly", async (tx, s) => {
        return reqPromise(s.getAll(IDBKeyRange.bound(
          [args.owner, args.groupID, ""],
          [args.owner, args.groupID, "zzzzzzzz"]
        )));
      });
    },
    async countGroupMembers(db, args) {
      return runTx(db, "groupMembers", "readonly", (tx, s) => {
        const range = args.groupID
          ? IDBKeyRange.bound(
              [args.owner, args.groupID, ""],
              [args.owner, args.groupID, "zzzzzzzz"]
            )
          : IDBKeyRange.bound(
              [args.owner, "", ""],
              [args.owner, "zzzzzzzz", "zzzzzzzz"]
            );
        return reqPromise(s.count(range));
      });
    },

    // ===== 生命周期 =====
    async clearOwner(db, args) {
      const stores = ["seqs", "versions", "messages", "conversations", "friends", "blacks", "groups", "groupMembers"];
      return runTx(db, stores, "readwrite", async (tx) => {
        for (const name of stores) {
          const s = tx.objectStore(name);
          if (name === "seqs") {
            s.delete([args.owner]);
          } else if (name === "versions") {
            s.delete(IDBKeyRange.bound([args.owner, ""], [args.owner, "zzzzzzzz"]));
          } else if (name === "messages") {
            const keys = await reqPromise(s.getAllKeys(IDBKeyRange.bound(
              [args.owner, "", -Infinity], [args.owner, "zzzzzzzz", Infinity]
            )));
            for (const k of keys) s.delete(k);
          } else if (name === "groupMembers") {
            const keys = await reqPromise(s.getAllKeys(IDBKeyRange.bound(
              [args.owner, "", ""], [args.owner, "zzzzzzzz", "zzzzzzzz"]
            )));
            for (const k of keys) s.delete(k);
          } else {
            // conversations / friends / blacks / groups → 双主键 range
            s.delete(IDBKeyRange.bound([args.owner, ""], [args.owner, "zzzzzzzz"]));
          }
        }
        return null;
      });
    },

    // ===== meta =====
    async getMeta(db, args) {
      return runTx(db, "meta", "readonly", async (tx, s) => {
        const row = await reqPromise(s.get([args.key]));
        if (!row) return null;
        return { value: row.value || "" };
      });
    },
    async setMeta(db, args) {
      return runTx(db, "meta", "readwrite", async (tx, s) => {
        s.put({ key: args.key, value: args.value || "" });
        return null;
      });
    },
  };

  // mutateMsgs: 拉全量消息 → 解析 dataJSON → mutator 修改 → 序列化写回
  // （Go 端 args.seqs 可能为空，表示条件过滤由 mutator 内部实现，如 markMsgsReadUpTo）
  async function mutateMsgs(db, args, mutator) {
    const rows = await runTx(db, "messages", "readonly", async (tx, s) => {
      return reqPromise(s.getAll(IDBKeyRange.bound(
        [args.owner, args.conversationID, -Infinity],
        [args.owner, args.conversationID, Infinity]
      )));
    });
    const seqSet = args.seqs ? new Set(args.seqs) : null;
    for (const row of rows) {
      if (seqSet && !seqSet.has(row.seq)) continue;
      const m = JSON.parse(row.dataJSON);
      mutator(m);
      row.dataJSON = JSON.stringify(m);
      row.status = m.status; // 让最新 status 进 store（冗余列，方便查询）
    }
    return runTx(db, "messages", "readwrite", async (tx, s) => {
      for (const row of rows) s.put(row);
      return null;
    });
  }

  global.goimIdb = { open, op };
})(typeof window !== "undefined" ? window : globalThis);

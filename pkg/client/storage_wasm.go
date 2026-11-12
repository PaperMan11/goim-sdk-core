//go:build js && wasm

package client

import (
	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/storage"
)

// newSQLiteStorage wasm 下 IndexedDB 实现（替代 SQLite）
//
// wasm 无 glebarez/sqlite 的后端，持久化统一走 IndexedDB。
// 参数 path 被忽略（IndexedDB 按 DB_NAME + owner 分片，无需路径）。
// 调用方传 SessionDataPath 仍可触发持久化（与非 wasm 行为对齐）。
func newSQLiteStorage(path string, l logger.Logger) (storage.Storage, error) {
	return storage.NewIdbStorage()
}

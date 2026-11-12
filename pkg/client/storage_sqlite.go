//go:build !(js && wasm)

package client

import (
	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/storage"
)

// newSQLiteStorage 打开 SQLite 存储（非 wasm 平台）
func newSQLiteStorage(path string, l logger.Logger) (storage.Storage, error) {
	return storage.NewSQLiteStorage(path, l)
}

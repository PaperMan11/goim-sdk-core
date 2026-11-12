//go:build js && wasm

package call

import "github.com/PaperMan11/goim-sdk-core/pkg/logger"

// defaultLogSink wasm 无 Node fs / process，zap 写 stdout 会报错，
// 浏览器侧统一通过 SetLogCallback（cmd/wasm 的 setLogCallback）注入输出。
func defaultLogSink(cfg *logger.Config) logger.Logger {
	return &logger.NopLogger{}
}

//go:build !(js && wasm)

package call

import "github.com/PaperMan11/goim-sdk-core/pkg/logger"

// defaultLogSink 非 wasm 平台的本地日志输出（zap，按 logging 配置写文件 / stdout）
func defaultLogSink(cfg *logger.Config) logger.Logger {
	return logger.New(cfg)
}

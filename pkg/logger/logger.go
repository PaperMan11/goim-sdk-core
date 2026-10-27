package logger

import "context"

// Logger 通用日志接口（printf 风格）
//
// 示例: logger.Infof("websocket connected, url=%s", wsURL)
type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})

	// With 返回带固定字段的子 Logger（如 userID / sessionID 等）
	// 实现上内部把 keyvals 转成适配层的结构化字段
	With(keyvals ...interface{}) Logger
}

// ContextAwareLogger 可选扩展：支持带 context 的日志
type ContextAwareLogger interface {
	Logger
	DebugContextf(ctx context.Context, format string, args ...interface{})
	InfoContextf(ctx context.Context, format string, args ...interface{})
	WarnContextf(ctx context.Context, format string, args ...interface{})
	ErrorContextf(ctx context.Context, format string, args ...interface{})
}

// —— 配置化构建 ——

// Config.Level 取值
const (
	LevelDebug = "debug"
	LevelInfo  = "info"
	LevelWarn  = "warn"
	LevelError = "error"
)

// Config.Format 取值
const (
	FormatConsole = "console" // 人类可读，默认
	FormatJSON    = "json"    // 结构化，生产/采集用
)

// Config Logger 构建配置（基于 zap 实现）
//
// 示例:
//
//	l := logger.New(&logger.Config{Level: logger.LevelDebug, Format: logger.FormatConsole})
type Config struct {
	// Disable 禁用日志（true 时 New 返回 NopLogger，默认 false）
	Disable bool

	// Level 日志级别：debug / info / warn / error；为空或非法值按 info 处理
	Level string

	// Format 输出格式：console / json；为空或非法值按 console 处理
	Format string

	// OutputPaths 输出目标：["stdout"] / ["stderr"] / 文件路径（可多个，如 ["sdk.log"]）
	// 为空时默认 ["stdout"]；路径不可用时降级 stdout
	OutputPaths []string

	// Development 开发模式：console 彩色级别 + Warn 及以上打印调用堆栈
	Development bool
}

// NopLogger 空实现 —— 测试/禁用日志时用
type NopLogger struct{}

func (n *NopLogger) Debugf(format string, args ...interface{})                             {}
func (n *NopLogger) Infof(format string, args ...interface{})                              {}
func (n *NopLogger) Warnf(format string, args ...interface{})                              {}
func (n *NopLogger) Errorf(format string, args ...interface{})                             {}
func (n *NopLogger) Fatalf(format string, args ...interface{})                             {}
func (n *NopLogger) With(keyvals ...interface{}) Logger                                    { return n }
func (n *NopLogger) DebugContextf(ctx context.Context, format string, args ...interface{}) {}
func (n *NopLogger) InfoContextf(ctx context.Context, format string, args ...interface{})  {}
func (n *NopLogger) WarnContextf(ctx context.Context, format string, args ...interface{})  {}
func (n *NopLogger) ErrorContextf(ctx context.Context, format string, args ...interface{}) {}

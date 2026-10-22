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

package logger

import (
	"context"
	"fmt"
)

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

// Namer 可选扩展：底层实现支持原生命名（如 zap 的 Named）时实现该接口，
// WithTag 会优先走原生命名——console 输出独立的名称列、JSON 输出 "logger" 字段，
// 不污染消息正文，也不影响 caller 定位
type Namer interface {
	Named(tag string) Logger
}

// WithTag 返回带标签的子 Logger：多客户端/多用户场景区分日志归属
//
// 示例: logger.WithTag(l, userID).Infof("websocket connected, url=%s", wsURL)
//
//   - l 实现了 Namer（内置 *ZapLogger）→ 走原生命名，标签是独立字段而非消息前缀
//   - 其他实现 → 退化为消息前缀 "[tag] <原文>"（先格式化原文再拼标签，
//     避免向调用方 format 注入占位符吞掉首参）
//   - With 派生的子 Logger 继承标签；tag 为空时原样返回；l 为 nil 时返回 NopLogger
func WithTag(l Logger, tag string) Logger {
	if l == nil {
		return &NopLogger{}
	}
	if tag == "" {
		return l
	}
	if n, ok := l.(Namer); ok {
		return n.Named(tag)
	}
	return &taggedLogger{Logger: l, tag: tag, prefix: "[" + tag + "] "}
}

// taggedLogger WithTag 对未实现 Namer 的 Logger 的通用回退包装（消息前缀方案）
type taggedLogger struct {
	Logger
	tag    string
	prefix string
}

// msg 先格式化原文再拼标签。顺序不能反：若把前缀直接拼进 format（尤其前缀含
// %s 之类占位符时）会吞掉调用方的第一个参数，导致整条日志字段错位
func (l *taggedLogger) msg(format string, args ...interface{}) string {
	return l.prefix + fmt.Sprintf(format, args...)
}

func (l *taggedLogger) Debugf(format string, args ...interface{}) {
	l.Logger.Debugf("%s", l.msg(format, args...))
}
func (l *taggedLogger) Infof(format string, args ...interface{}) {
	l.Logger.Infof("%s", l.msg(format, args...))
}
func (l *taggedLogger) Warnf(format string, args ...interface{}) {
	l.Logger.Warnf("%s", l.msg(format, args...))
}
func (l *taggedLogger) Errorf(format string, args ...interface{}) {
	l.Logger.Errorf("%s", l.msg(format, args...))
}
func (l *taggedLogger) Fatalf(format string, args ...interface{}) {
	l.Logger.Fatalf("%s", l.msg(format, args...))
}
func (l *taggedLogger) With(keyvals ...interface{}) Logger {
	return &taggedLogger{Logger: l.Logger.With(keyvals...), tag: l.tag, prefix: l.prefix}
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

package call

import (
	"fmt"
	"strings"

	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
)

// LogCallback 跨语言日志输出契约（gomobile 可绑定：单方法 + 基本类型参数）。
//
// Native 侧实现一次即可收到 sdk-core 的全部日志（级别已按 logging.level 过滤）：
//
//	type GoimLogCallback : LogCallback {
//	    override fun onLog(level: String, msg: String) { Log.d("goim-sdk", "[$level] $msg") }
//	}
//
// ⚠️ 回调在 SDK 内部 goroutine 上同步调用，Native 侧禁止阻塞（落盘/网络上报请自行异步化）。
type LogCallback interface {
	// OnLog level 取值为 debug / info / warn / error；msg 为已格式化文本
	OnLog(level string, msg string)
}

// SetLogCallback 注册日志回调（传 nil 注销）。
//
// 可在 Init 之前或之后调用：日志出口按写入时的注册状态动态分发——
// 已注册回调 → 转发回调；未注册 → 回落到 Init 的 logging 配置本地输出（zap）。
func (d *Dispatcher) SetLogCallback(cb LogCallback) {
	d.mu.Lock()
	d.logCB = cb
	d.mu.Unlock()
}

// buildLogger 构建跨语言日志出口（Init 专用，作为 ClientConfig.Logger 注入）。
//
// 开关：logging.disable=true → 全静默（含回调）；logging=nil 且未注册回调 → 静默；
// 注册回调即开启。级别取 logging.level（缺省 info）。
// 输出：回调优先（写入时动态判断），未注册回调时走 zap（logging.outputPaths，默认 stdout）。
//
// wasm 无 Node fs，zap 写 stdout 会失败，因此 wasm 的 fallback 恒为 NopLogger，
// 浏览器/Node 侧需通过 setLogCallback 注入输出。
func (d *Dispatcher) buildLogger(cfg *logger.Config) logger.Logger {
	if cfg != nil && cfg.Disable {
		return &logger.NopLogger{}
	}
	level := logger.LevelInfo
	if cfg != nil && cfg.Level != "" {
		level = cfg.Level
	}
	return &logHub{d: d, fallback: defaultLogSink(cfg), minLevel: logger.LevelValue(level)}
}

// logHub 动态日志出口 —— 实现 logger.Logger（printf 风格）。
//
// 每条日志按当前 LogCallback 注册状态分发：有回调走回调，无回调回落到本地 sink，
// 因此宿主先 Init 后注册回调也能拿到日志（跨语言宿主常见顺序）。
type logHub struct {
	d        *Dispatcher
	fallback logger.Logger
	minLevel int
	keyvals  []interface{}
}

// emit 按级别阈值过滤后分发；msg 先格式化再分发，避免把调用方 format 拼进下游 format
func (l *logHub) emit(level string, format string, args ...interface{}) {
	if logger.LevelValue(level) < l.minLevel {
		return
	}
	msg := fmt.Sprintf(format, args...)
	if len(l.keyvals) > 0 {
		msg = keyvalsString(l.keyvals) + msg
	}

	cb := l.d.currentLogCallback()
	if cb != nil {
		cb.OnLog(level, msg)
		return
	}
	if l.fallback == nil {
		return
	}
	switch level {
	case logger.LevelDebug:
		l.fallback.Debugf("%s", msg)
	case logger.LevelWarn:
		l.fallback.Warnf("%s", msg)
	case logger.LevelError:
		l.fallback.Errorf("%s", msg)
	default:
		l.fallback.Infof("%s", msg)
	}
}

func (l *logHub) Debugf(format string, args ...interface{}) {
	l.emit(logger.LevelDebug, format, args...)
}

func (l *logHub) Infof(format string, args ...interface{}) {
	l.emit(logger.LevelInfo, format, args...)
}

func (l *logHub) Warnf(format string, args ...interface{}) {
	l.emit(logger.LevelWarn, format, args...)
}

func (l *logHub) Errorf(format string, args ...interface{}) {
	l.emit(logger.LevelError, format, args...)
}

// Fatalf 库内不终止进程：按 error 级别输出（宿主进程不应被 SDK 日志强制退出）
func (l *logHub) Fatalf(format string, args ...interface{}) {
	l.emit(logger.LevelError, format, args...)
}

func (l *logHub) With(keyvals ...interface{}) logger.Logger {
	nh := &logHub{
		d:        l.d,
		minLevel: l.minLevel,
		keyvals:  append(append([]interface{}{}, l.keyvals...), keyvals...),
	}
	if l.fallback != nil {
		nh.fallback = l.fallback.With(keyvals...)
	}
	return nh
}

// Close 释放本地输出资源（文件句柄等）；纯回调模式无副作用
func (l *logHub) Close() error {
	if c, ok := l.fallback.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

// replaceLogSink 构建并记录日志出口（Init 每次重建）。
//
// 旧出口不在这里关闭：重复 Init 时旧 Client 可能仍在写，本地句柄统一在 Dispatcher.Close 释放。
func (d *Dispatcher) replaceLogSink(cfg *logger.Config) logger.Logger {
	l := d.buildLogger(cfg)
	d.mu.Lock()
	d.logSink = l
	d.mu.Unlock()
	return l
}

// closeLogSink 释放日志出口的本地资源（文件句柄等）；nil / 无 Close 方法时忽略
func closeLogSink(l logger.Logger) {
	if c, ok := l.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}

// currentLogCallback 读取当前注册的日志回调（写入时动态读取，兼容 Init 后注册）
func (d *Dispatcher) currentLogCallback() LogCallback {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.logCB
}

// keyvalsString 把 With 的结构化字段降级为消息前缀（回调只有 level/msg 两个参数）
func keyvalsString(kv []interface{}) string {
	var b strings.Builder
	b.WriteString("[")
	for i := 0; i+1 < len(kv); i += 2 {
		if i > 0 {
			b.WriteString(" ")
		}
		fmt.Fprintf(&b, "%v=%v", kv[i], kv[i+1])
	}
	b.WriteString("] ")
	return b.String()
}

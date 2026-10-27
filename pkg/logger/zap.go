package logger

import (
	"context"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// New 根据配置创建 Logger（zap 实现）
//
// 规则：
//   - cfg 为 nil 或 Disable=true → NopLogger（静默）
//   - Level 非法/为空 → info；Format 非法/为空 → console
//   - OutputPaths 为空 → stdout；路径不可用（如目录不存在）时降级 stdout
func New(cfg *Config) Logger {
	if cfg == nil || cfg.Disable {
		return &NopLogger{}
	}

	paths := cfg.OutputPaths
	if len(paths) == 0 {
		paths = []string{"stdout"}
	}

	zcfg := zap.Config{
		Level:            zap.NewAtomicLevelAt(parseLevel(cfg.Level)),
		Development:      cfg.Development,
		Encoding:         normalizeFormat(cfg.Format),
		EncoderConfig:    encoderConfig(cfg.Format, isTerminal(paths)),
		OutputPaths:      paths,
		ErrorOutputPaths: []string{"stderr"},
	}

	z, err := zcfg.Build()
	if err != nil {
		// 输出目标不可用 → 降级 stdout
		zcfg.OutputPaths = []string{"stdout"}
		if z, err = zcfg.Build(); err != nil {
			return &NopLogger{}
		}
	}
	return &ZapLogger{sugar: z.Sugar()}
}

// parseLevel 解析日志级别，非法/为空 → info
func parseLevel(s string) zapcore.Level {
	var level zapcore.Level
	if err := level.UnmarshalText([]byte(strings.TrimSpace(s))); err != nil {
		return zapcore.InfoLevel
	}
	return level
}

// normalizeFormat 归一化输出格式，非法/为空 → console
func normalizeFormat(s string) string {
	if strings.EqualFold(strings.TrimSpace(s), FormatJSON) {
		return FormatJSON
	}
	return FormatConsole
}

// isTerminal 是否输出到终端（console 格式且输出终端时级别用彩色）
func isTerminal(paths []string) bool {
	for _, p := range paths {
		if p == "stdout" || p == "stderr" {
			return true
		}
	}
	return false
}

func encoderConfig(format string, color bool) zapcore.EncoderConfig {
	encCfg := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
	if format == FormatJSON {
		return encCfg
	}
	if color {
		encCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}
	return encCfg
}

// ZapLogger Logger 接口的 zap 适配器（printf 风格，底层用 SugaredLogger）
type ZapLogger struct {
	sugar *zap.SugaredLogger
}

// NewZapLogger 从 *zap.Logger 构造适配器
func NewZapLogger(z *zap.Logger) *ZapLogger {
	return &ZapLogger{sugar: z.Sugar()}
}

// NewZapSugarLogger 从 *zap.SugaredLogger 构造
func NewZapSugarLogger(sugar *zap.SugaredLogger) *ZapLogger {
	return &ZapLogger{sugar: sugar}
}

func (l *ZapLogger) With(keyvals ...interface{}) Logger {
	return &ZapLogger{sugar: l.sugar.With(keyvals...)}
}

func (l *ZapLogger) Debugf(format string, args ...interface{}) {
	l.sugar.Debugf(format, args...)
}

func (l *ZapLogger) Infof(format string, args ...interface{}) {
	l.sugar.Infof(format, args...)
}

func (l *ZapLogger) Warnf(format string, args ...interface{}) {
	l.sugar.Warnf(format, args...)
}

func (l *ZapLogger) Errorf(format string, args ...interface{}) {
	l.sugar.Errorf(format, args...)
}

func (l *ZapLogger) Fatalf(format string, args ...interface{}) {
	l.sugar.Fatalf(format, args...)
}

// ContextAwareLogger 实现

func (l *ZapLogger) DebugContextf(ctx context.Context, format string, args ...interface{}) {
	l.sugar.Debugf(format, args...)
}

func (l *ZapLogger) InfoContextf(ctx context.Context, format string, args ...interface{}) {
	l.sugar.Infof(format, args...)
}

func (l *ZapLogger) WarnContextf(ctx context.Context, format string, args ...interface{}) {
	l.sugar.Warnf(format, args...)
}

func (l *ZapLogger) ErrorContextf(ctx context.Context, format string, args ...interface{}) {
	l.sugar.Errorf(format, args...)
}

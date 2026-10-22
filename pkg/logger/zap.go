package logger

import (
	"context"

	"go.uber.org/zap"
)

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

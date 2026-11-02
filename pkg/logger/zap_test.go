package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewWithFileOutput 写文件日志：JSON 格式 + With 字段 + 多级别
func TestNewWithFileOutput(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "sdk.log")

	l := New(&Config{
		Level:       LevelDebug,
		Format:      FormatJSON,
		OutputPaths: []string{logFile},
	})
	zl, ok := l.(*ZapLogger)
	require.True(t, ok)
	defer zl.Close()

	zl.With("userID", "u1").Infof("hello %s", "world")
	zl.Warnf("warn message")

	data, err := os.ReadFile(logFile)
	require.NoError(t, err)
	content := string(data)

	assert.Contains(t, content, `"level":"INFO"`)
	assert.Contains(t, content, "hello world")
	assert.Contains(t, content, `"userID":"u1"`)
	assert.Contains(t, content, `"level":"WARN"`)
	assert.Contains(t, content, "warn message")
}

// TestNewWithFileOutputLevelFilter 写文件日志：info 级别过滤 debug
func TestNewWithFileOutputLevelFilter(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "sdk.log")

	l := New(&Config{
		Level:       LevelInfo,
		OutputPaths: []string{logFile},
	})
	zl, ok := l.(*ZapLogger)
	require.True(t, ok)
	defer zl.Close()

	zl.Debugf("debug should be filtered")
	zl.Infof("info should be written")

	data, err := os.ReadFile(logFile)
	require.NoError(t, err)
	content := string(data)

	assert.NotContains(t, content, "debug should be filtered")
	assert.Contains(t, content, "info should be written")
}

// TestNewInvalidPathFallback 输出路径不可用（目录不存在）→ 降级 stdout，仍返回可用 Logger
func TestNewInvalidPathFallback(t *testing.T) {
	badPath := filepath.Join(t.TempDir(), "no-such-dir", "sdk.log")

	l := New(&Config{Level: LevelDebug, OutputPaths: []string{badPath}})
	assert.IsType(t, &ZapLogger{}, l)
	l.Infof("fallback to stdout")
}

// TestNewDefaults nil / Disable → NopLogger
func TestNewDefaults(t *testing.T) {
	assert.IsType(t, &NopLogger{}, New(nil))
	assert.IsType(t, &NopLogger{}, New(&Config{Disable: true}))
}

// TestWithTagZapNative WithTag 对 *ZapLogger 走 zap 原生 Named：
// 标签是独立 "logger" 字段，消息正文不被改写；With 派生子 Logger 后标签保留
func TestWithTagZapNative(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "sdk.log")

	l := New(&Config{
		Level:       LevelDebug,
		Format:      FormatJSON,
		OutputPaths: []string{logFile},
	})
	zl, ok := l.(*ZapLogger)
	require.True(t, ok)
	defer zl.Close()

	tagged := WithTag(l, "user-A")
	require.NotSame(t, l, tagged, "*ZapLogger should take the native Named path")
	tagged.Infof("hello %s", "world")
	tagged.With("k", "v").Warnf("warn message")

	data, err := os.ReadFile(logFile)
	require.NoError(t, err)
	content := string(data)

	assert.Contains(t, content, `"logger":"user-A"`)
	assert.Contains(t, content, "hello world")
	assert.Contains(t, content, `"k":"v"`)
	assert.Contains(t, content, "warn message")
}

// stubLogger 无原生命名能力的实现（宿主自建 Logger 的形态），验证 WithTag 回退路径
type stubLogger struct {
	msgs []string
}

func (s *stubLogger) Debugf(format string, args ...interface{}) {
	s.msgs = append(s.msgs, fmt.Sprintf(format, args...))
}
func (s *stubLogger) Infof(format string, args ...interface{}) {
	s.msgs = append(s.msgs, fmt.Sprintf(format, args...))
}
func (s *stubLogger) Warnf(format string, args ...interface{}) {
	s.msgs = append(s.msgs, fmt.Sprintf(format, args...))
}
func (s *stubLogger) Errorf(format string, args ...interface{}) {
	s.msgs = append(s.msgs, fmt.Sprintf(format, args...))
}
func (s *stubLogger) Fatalf(format string, args ...interface{}) {
	s.msgs = append(s.msgs, fmt.Sprintf(format, args...))
}
func (s *stubLogger) With(keyvals ...interface{}) Logger { return s }

// TestWithTagFallback WithTag 对非 Namer 实现退化为消息前缀；
// 原文含 % 字符时不得造成参数错位（前缀不能拼进调用方 format）
func TestWithTagFallback(t *testing.T) {
	stub := &stubLogger{}
	tagged := WithTag(stub, "user-B")

	tagged.Infof("hello %s", "world")
	tagged.Infof("100%% sure, no args")

	// With 派生保留标签
	tagged.With("k", "v").Errorf("err=%d", 7)

	// 边界：nil / 空 tag
	assert.Equal(t, []string{
		"[user-B] hello world",
		"[user-B] 100% sure, no args",
		"[user-B] err=7",
	}, stub.msgs)
	assert.IsType(t, &NopLogger{}, WithTag(nil, "user-C"))
	assert.Same(t, stub, WithTag(stub, ""))
}

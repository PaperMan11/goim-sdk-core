package logger

import (
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

package call

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
)

// recordingLogCB 记录回调收到的日志（level|msg）
type recordingLogCB struct {
	mu      sync.Mutex
	entries []string
}

func (r *recordingLogCB) OnLog(level string, msg string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, level+"|"+msg)
}

func (r *recordingLogCB) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string{}, r.entries...)
}

// TestLogCallbackRouting 回调转发 + Init 后注册仍生效 + With 字段前缀
func TestLogCallbackRouting(t *testing.T) {
	d := NewDispatcher()
	l := d.buildLogger(&logger.Config{Level: logger.LevelDebug})

	cb := &recordingLogCB{}
	d.SetLogCallback(cb)

	l.Infof("hello %s", "world")
	l.Debugf("debug %d", 1)
	l.With("userID", "u1").Warnf("warn")

	got := cb.all()
	want := []string{"info|hello world", "debug|debug 1", "warn|[userID=u1] warn"}
	if len(got) != len(want) {
		t.Fatalf("entries = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("entries[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	d.SetLogCallback(nil)
	l.Infof("after unregister")
	if len(cb.all()) != len(want) {
		t.Error("unregistered callback should not receive logs")
	}
}

// TestLogLevelFilter logging.level 作为开关阈值
func TestLogLevelFilter(t *testing.T) {
	d := NewDispatcher()
	cb := &recordingLogCB{}
	d.SetLogCallback(cb)

	l := d.buildLogger(&logger.Config{Level: logger.LevelWarn})
	l.Debugf("d")
	l.Infof("i")
	l.Warnf("w")
	l.Errorf("e")

	got := cb.all()
	if len(got) != 2 || got[0] != "warn|w" || got[1] != "error|e" {
		t.Fatalf("entries = %v, want [warn|w error|e]", got)
	}
}

// TestLogDisable logging.disable=true 整体静默（含已注册回调）
func TestLogDisable(t *testing.T) {
	d := NewDispatcher()
	cb := &recordingLogCB{}
	d.SetLogCallback(cb)

	l := d.buildLogger(&logger.Config{Disable: true, Level: logger.LevelDebug})
	if _, ok := l.(*logger.NopLogger); !ok {
		t.Fatalf("buildLogger = %T, want *logger.NopLogger", l)
	}
	l.Errorf("should be dropped")
	if len(cb.all()) != 0 {
		t.Errorf("disabled logging delivered %v", cb.all())
	}
}

// TestLogFileFallback 未注册回调时回落到 logging.outputPaths 文件输出
func TestLogFileFallback(t *testing.T) {
	d := NewDispatcher()
	path := filepath.Join(t.TempDir(), "sdk.log")

	l := d.buildLogger(&logger.Config{Level: logger.LevelDebug, Format: logger.FormatConsole, OutputPaths: []string{path}})
	l.Infof("file sink %s", "ok")
	closeLogSink(l)

	bs, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	if !strings.Contains(string(bs), "file sink ok") {
		t.Fatalf("log file = %q, want contains %q", string(bs), "file sink ok")
	}
}

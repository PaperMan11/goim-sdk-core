// 集成测试入口 —— 与 goim 服务端真实对接（docs/INTEGRATION_TEST_PLAN.md §2.2）
//
// 隔离机制：不使用 build tag（保证 gopls 可索引、IDE 跳转正常），
// 而是靠环境变量注入 + requireEnv 自动 Skip：
//
//	$env:GOIM_API_URL  = "http://127.0.0.1:18880/api/v1"
//	$env:GOIM_WS_URL   = "ws://127.0.0.1:50001/ws"
//	$env:GOIM_SECRET   = "<admin secret>"
//	$env:GOIM_LOG_LEVEL = "debug"
//	go test ./test/integration/... -v -count=1
//
// 环境变量缺失时所有用例自动 Skip，不影响 `go test ./...`。
package integration

import (
	"os"
	"testing"

	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
)

// 服务端地址与凭证（环境变量注入）
var (
	envAPIURL   = os.Getenv("GOIM_API_URL")
	envWSURL    = os.Getenv("GOIM_WS_URL")
	envSecret   = os.Getenv("GOIM_SECRET")
	envLogLevel = os.Getenv("GOIM_LOG_LEVEL")
	// envShortTTL 标记服务端已把 token 有效期调短（如 60s），E1.5 用例启用该开关
	// （服务端恢复常规有效期时保持未设置，E1.5 自动 Skip）
	envShortTTL = os.Getenv("GOIM_SHORT_TTL")
)

func TestMain(m *testing.M) {
	if envLogLevel == "" {
		envLogLevel = logger.LevelDebug
	}
	os.Exit(m.Run())
}

// requireEnv 校验环境变量齐备，缺失则 Skip 当前用例
func requireEnv(t *testing.T) {
	t.Helper()
	if envAPIURL == "" || envWSURL == "" || envSecret == "" {
		t.Skip("integration env missing: set GOIM_API_URL / GOIM_WS_URL / GOIM_SECRET (see docs/INTEGRATION_TEST_PLAN.md §2.2)")
	}
}

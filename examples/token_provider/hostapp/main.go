// 终端 App 示例：通过 TokenProvider 接入 goim-sdk-core，token 过期自动续期。
//
// 核心思路：
//   - App 首次登录宿主账号后，向宿主业务服务端要一个 IM user token，
//     用 LoginWithToken 建立会话
//   - 同时配置 TokenProvider 回调：SDK 内部发现 token 过期（HTTP 401 /
//     WS 重连鉴权失败）时自动回调，宿主再向业务服务端要新 token 返回，
//     SDK 自动续期并重试，业务层无感
//   - App 全程不接触 AdminSecret
//
// 运行：go run ./examples/token_provider/hostapp
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/PaperMan11/goim-sdk-core/pkg/client"
	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
)

const (
	hostTokenURL = "http://127.0.0.1:8081/im/token" // 宿主业务服务端的发 token 接口
	imAPIURL     = "http://192.168.241.128:10002/api/v1" // goim im-api 地址（含 /api/v1，与 SDK BaseURL 约定一致）
	imWSEndpoint = "ws://192.168.241.128:50001/ws"  // goim gateway 地址
	demoUserID   = "it_demo_user_1"
)

func main() {
	cfg := &client.ClientConfig{
		BaseURL:  imAPIURL,
		Endpoint: imWSEndpoint,
		Logging: &logger.Config{
			Level:  logger.LevelDebug,
			Format: logger.FormatConsole,
		},
		// ★ 关键：终端模式必须配置 TokenProvider
		TokenProvider: fetchTokenFromHost,
	}
	c := client.NewClient(cfg)
	defer func() {
		if err := c.Close(); err != nil {
			log.Printf("close: %v", err)
		}
	}()

	ctx := context.Background()

	// 首次登录：向宿主要 token，再用 LoginWithToken 建立会话。
	// token 过期后 SDK 走 TokenProvider 自动续期，无需宿主再次显式登录。
	token, expire, err := fetchTokenFromHost(ctx, demoUserID, 1)
	if err != nil {
		log.Fatalf("fetch first token: %v", err)
	}
	if err := c.LoginWithToken(ctx, demoUserID, 1, token, expire); err != nil {
		log.Fatalf("login: %v", err)
	}
	log.Printf("logged in as %s, token valid for %ds", demoUserID, expire)

	// 订阅全部事件（示例：仅打印。生产按需订阅具体 EventType）
	_, unsub := c.SubscribeAll()
	defer unsub()

	select {} // 示例：常驻。真实 App 里由 App 生命周期管理
}

// fetchTokenFromHost 就是配置给 SDK 的 TokenProvider：
// SDK 在 token 过期时调用它，宿主向自己的业务服务端请求新 token。
func fetchTokenFromHost(ctx context.Context, userID string, platformID int32) (string, int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s?userID=%s&platformID=%d", hostTokenURL, userID, platformID), nil)
	if err != nil {
		return "", 0, err
	}
	// 示例从简；生产环境带上宿主 App 的登录态，让业务服务端校验身份
	req.Header.Set("X-App-Session", "demo-session")

	httpCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req = req.WithContext(httpCtx)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("request host token server: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("host token server: http %d", resp.StatusCode)
	}

	var body struct {
		Token             string `json:"token"`
		ExpireTimeSeconds int64  `json:"expireTimeSeconds"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", 0, err
	}
	if body.Token == "" {
		return "", 0, fmt.Errorf("host token server returned empty token")
	}
	return body.Token, body.ExpireTimeSeconds, nil
}

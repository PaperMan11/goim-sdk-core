// 宿主业务服务端示例：为终端 App 签发 IM user token。
//
// 职责（全部发生在服务端，终端 App 永远接触不到这些凭证）：
//  1. 校验宿主自己的登录态（示例里用简单 header 模拟）
//  2. 用 AdminSecret 调 IM 的 get_admin_token 换取 admin token
//  3. 携 admin token 调 get_user_token 为目标用户签发 user token
//  4. 把 (token, expireTimeSeconds) 下发给 App
//
// 运行：go run ./examples/token_provider/hostserver
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

const (
	imAPIURL   = "http://192.168.241.128:10002" // goim im-api 地址
	adminUser  = "imAdmin"                      // 服务端管理员列表中的账号
	adminPass  = "your-admin-secret"            // ⚠️ 生产环境从配置中心/环境变量读取
	listenAddr = ":8081"
)

func main() {
	// 简化示例：admin token 缓存（生产环境要处理过期刷新与并发）
	var adminToken string

	// App 调用：GET /im/token?userID=xxx&platformID=1
	http.HandleFunc("/im/token", func(w http.ResponseWriter, r *http.Request) {
		userID := r.URL.Query().Get("userID")
		platformID := r.URL.Query().Get("platformID")
		if userID == "" {
			http.Error(w, "userID required", http.StatusBadRequest)
			return
		}
		// ① 校验宿主自身登录态（示例从简；生产用 session/JWT 校验）
		if r.Header.Get("X-App-Session") == "" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		// ② admin token 为空时先换取
		if adminToken == "" {
			at, err := fetchAdminToken()
			if err != nil {
				http.Error(w, "admin token: "+err.Error(), http.StatusInternalServerError)
				return
			}
			adminToken = at
		}

		// ③ 用 admin token 为目标用户签发 user token
		token, expire, err := fetchUserToken(adminToken, userID, platformID)
		if err != nil {
			// admin token 可能已过期：清缓存让下次重取
			adminToken = ""
			http.Error(w, "user token: "+err.Error(), http.StatusInternalServerError)
			return
		}

		// ④ 下发给 App
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"token":             token,
			"expireTimeSeconds": expire,
		})
	})

	log.Printf("host token server listening on %s", listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, nil))
}

func fetchAdminToken() (string, error) {
	body := fmt.Sprintf(`{"secret":%q,"userID":%q}`, adminPass, adminUser)
	return postIM("/auth/get_admin_token", body, "")
}

func fetchUserToken(adminToken, userID, platformID string) (string, int64, error) {
	body := fmt.Sprintf(`{"userID":%q,"platformID":%s}`, userID, platformID)
	raw, err := postIMRaw("/auth/get_user_token", body, adminToken)
	if err != nil {
		return "", 0, err
	}
	var resp struct {
		Data struct {
			Token             string `json:"token"`
			ExpireTimeSeconds int64  `json:"expireTimeSeconds"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return "", 0, err
	}
	return resp.Data.Token, resp.Data.ExpireTimeSeconds, nil
}

func postIM(path, body, token string) (string, error) {
	raw, err := postIMRaw(path, body, token)
	if err != nil {
		return "", err
	}
	var resp struct {
		Data struct {
			Token             string `json:"token"`
			ExpireTimeSeconds int64  `json:"expireTimeSeconds"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return "", err
	}
	return resp.Data.Token, nil
}

// postIMRaw 调 IM HTTP API。⚠️ 路由与响应字段以实际服务端为准（SDK 侧 /api/v1 前缀由
// goim-sdk-core 自动拼接；这里是宿主服务端直调，手动带全路径）。
func postIMRaw(path, body, token string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodPost, imAPIURL+"/api/v1"+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("operationID", fmt.Sprintf("host-%d", time.Now().UnixNano()))
	if token != "" {
		req.Header.Set("token", token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("im api %s: http %d", path, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

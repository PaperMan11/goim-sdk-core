package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"sync"

	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/convert"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/timex"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/uuidx"
)

const (
	defaultHTTPTimeout = 15 * timex.Second
	maxRetry           = 3
	initialBackoff     = 100 * timex.Millisecond
)

// HTTPClient HTTP 客户端接口
type HTTPClient interface {
	Post(ctx context.Context, path string, body, resp interface{}) error
	Get(ctx context.Context, path string, query url.Values, resp interface{}) error
}

// CurrentUserSetter 可选扩展：设置当前用户，用于 TokenStore 选路
// （TokenStore 按 (userID, platformID) 存储 token，HTTP 层需要知道当前用户才能取到）
type CurrentUserSetter interface {
	SetCurrentUser(userID string, platformID int32)
}

// DefaultHTTPClient 默认 HTTPClient 实现
type DefaultHTTPClient struct {
	baseURL    string
	httpClient *http.Client
	tokenStore TokenStore
	logger     logger.Logger

	mu        sync.Mutex
	curUser   string // 当前用户（Login 后设置），token 按 (curUser, curPlatform) 选取
	curPlatID int32
}

// NewHTTPClient 创建 DefaultHTTPClient
func NewHTTPClient(baseURL string, tokenStore TokenStore, l logger.Logger) *DefaultHTTPClient {
	return &DefaultHTTPClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: defaultHTTPTimeout,
		},
		tokenStore: tokenStore,
		logger:     l,
	}
}

// NewHTTPClientWithTimeout 允许自定义超时
func NewHTTPClientWithTimeout(baseURL string, tokenStore TokenStore, l logger.Logger, timeout timex.Duration) *DefaultHTTPClient {
	return &DefaultHTTPClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		tokenStore: tokenStore,
		logger:     l,
	}
}

// postResp 统一响应结构
type postResp struct {
	Code int32           `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

// SetCurrentUser 设置当前用户（TokenStore 选路用）；Logout 时传空清除
func (c *DefaultHTTPClient) SetCurrentUser(userID string, platformID int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.curUser = userID
	c.curPlatID = platformID
}

// currentUser 返回当前 (userID, platformID)
func (c *DefaultHTTPClient) currentUser() (string, int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.curUser, c.curPlatID
}

// Post 发送 POST 请求
func (c *DefaultHTTPClient) Post(ctx context.Context, path string, body, resp interface{}) error {
	tokenRefreshed := false
	for {
		err := c.doPost(ctx, path, body, resp)
		if err == nil {
			return nil
		}

		// 认证类错误（过期/无效/服务端记录不存在等）→ 真正刷新 token 后重试一次
		if model.IsTokenRefreshable(err) && !tokenRefreshed && c.tokenStore != nil {
			tokenRefreshed = true
			userID, platformID := c.currentUser()
			if userID == "" {
				return err // 未登录态无刷新主体（如 bootstrap 之前的白名单接口）
			}
			if c.logger != nil {
				c.logger.Warnf("token auth error, refreshing once, path=%s, err=%v", path, err)
			}
			oldTok, _ := c.tokenStore.Get(userID, platformID)
			if _, rerr := c.tokenStore.Refresh(ctx, userID, platformID); rerr != nil {
				if c.logger != nil {
					c.logger.Errorf("token refresh failed: %v", rerr)
				}
				return err // 刷新失败，返回原始认证错误
			}
			// 刷新后 token 未变化（宿主返回了同一个失效 token）→ 重试无意义，放弃
			if newTok, _ := c.tokenStore.Get(userID, platformID); newTok == oldTok {
				return err
			}
			continue
		}
		return err
	}
}

// Get 发送 GET 请求
func (c *DefaultHTTPClient) Get(ctx context.Context, path string, query url.Values, resp interface{}) error {
	tokenRefreshed := false
	for {
		err := c.doGet(ctx, path, query, resp)
		if err == nil {
			return nil
		}
		if model.IsTokenRefreshable(err) && !tokenRefreshed && c.tokenStore != nil {
			tokenRefreshed = true
			userID, platformID := c.currentUser()
			if userID == "" {
				return err
			}
			if c.logger != nil {
				c.logger.Warnf("token auth error, refreshing once, path=%s, err=%v", path, err)
			}
			oldTok, _ := c.tokenStore.Get(userID, platformID)
			if _, rerr := c.tokenStore.Refresh(ctx, userID, platformID); rerr != nil {
				if c.logger != nil {
					c.logger.Errorf("token refresh failed: %v", rerr)
				}
				return err
			}
			if newTok, _ := c.tokenStore.Get(userID, platformID); newTok == oldTok {
				return err
			}
			continue
		}
		return err
	}
}

func (c *DefaultHTTPClient) doPost(ctx context.Context, path string, body, resp interface{}) error {
	var bodyBytes []byte
	var err error
	if body != nil {
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal body: %w", err)
		}
	}

	// BaseURL 由调用方提供完整前缀（如 https://api.goim.io/api/v1），此处直接拼接业务路径
	fullURL := c.baseURL + path
	operationID := uuidx.NewOperationID(path)

	var lastErr error
	for attempt := 0; attempt < maxRetry; attempt++ {
		if attempt > 0 {
			backoff := initialBackoff * convert.ToDuration(1<<uint(attempt-1))
			// 加 jitter
			jitter := convert.ToDuration(rand.Int63n(convert.ToInt64(backoff / 2)))
			wait := backoff + jitter
			if c.logger != nil {
				c.logger.Debugf("retrying request, attempt=%d, wait=%v, operationID=%s",
					attempt+1, wait, operationID)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timex.After(wait):
			}
		}

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, fullURL, bytes.NewReader(bodyBytes))
		if err != nil {
			return fmt.Errorf("build request: %w", err)
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("operationID", operationID)

		if c.tokenStore != nil {
			// 按 SetCurrentUser 设置的当前用户选取 token（TokenStore 以 (userID, platformID) 为键）
			userID, platformID := c.currentUser()
			token, _ := c.tokenStore.Get(userID, platformID)
			if token != "" {
				httpReq.Header.Set("token", token)
			}
		}

		if c.logger != nil {
			c.logger.Debugf("http request, method=POST, url=%s, operationID=%s",
				fullURL, operationID)
		}

		httpResp, err := c.httpClient.Do(httpReq)
		if err != nil {
			lastErr = err
			if c.logger != nil {
				c.logger.Debugf("http request failed, will retry, operationID=%s, error=%v",
					operationID, err)
			}
			continue
		}

		data, err := io.ReadAll(httpResp.Body)
		httpResp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}

		if c.logger != nil {
			c.logger.Debugf("http response, operationID=%s, status=%d, body=%s",
				operationID, httpResp.StatusCode, data)
		}

		// 非 200 一律视为失败（如路由 404）：其响应体不含业务信封，
		// 若继续按 {code,msg,data} 解析会得到 code=0 + 空 data 的"假成功"
		if httpResp.StatusCode != http.StatusOK {
			return model.NewSDKError(model.ErrCodeInternalError,
				fmt.Sprintf("http status %d, url=%s, body=%s", httpResp.StatusCode, fullURL, truncateBody(data)))
		}

		// HTTP 层非 2xx 也解析统一响应
		var parsed postResp
		if err := json.Unmarshal(data, &parsed); err != nil {
			return fmt.Errorf("parse response: %w", err)
		}

		if parsed.Code == 0 {
			if resp != nil && len(parsed.Data) > 0 {
				if err := json.Unmarshal(parsed.Data, resp); err != nil {
					return fmt.Errorf("unmarshal data: %w", err)
				}
			}
			return nil
		}

		// 认证类错误（1501/1504/1507 等）直接返回，统一由外层 Post 刷新后重试。
		// 此处不再触发 Refresh：外层收到错误还会刷新一次，会变成双重刷新
		// （TokenProvider 回调被调两次 / AdminSecret 多签发一次 token）
		return model.NewSDKError(parsed.Code, parsed.Msg)
	}

	if lastErr != nil {
		return fmt.Errorf("http request failed after retries: %w", lastErr)
	}
	return fmt.Errorf("http request failed after retries")
}

func (c *DefaultHTTPClient) doGet(ctx context.Context, path string, query url.Values, resp interface{}) error {
	// BaseURL 由调用方提供完整前缀（含 /api/v1），此处直接拼接业务路径
	fullURL := c.baseURL + path
	if len(query) > 0 {
		fullURL += "?" + query.Encode()
	}
	operationID := uuidx.NewOperationID(path)

	var lastErr error
	for attempt := 0; attempt < maxRetry; attempt++ {
		if attempt > 0 {
			backoff := initialBackoff * convert.ToDuration(1<<uint(attempt-1))
			jitter := convert.ToDuration(rand.Int63n(convert.ToInt64(backoff / 2)))
			wait := backoff + jitter
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timex.After(wait):
			}
		}

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
		if err != nil {
			return fmt.Errorf("build request: %w", err)
		}
		httpReq.Header.Set("operationID", operationID)
		if c.tokenStore != nil {
			userID, platformID := c.currentUser()
			token, _ := c.tokenStore.Get(userID, platformID)
			if token != "" {
				httpReq.Header.Set("token", token)
			}
		}

		httpResp, err := c.httpClient.Do(httpReq)
		if err != nil {
			lastErr = err
			continue
		}
		data, err := io.ReadAll(httpResp.Body)
		httpResp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}

		if c.logger != nil {
			c.logger.Debugf("http response, operationID=%s, status=%d, body=%s",
				operationID, httpResp.StatusCode, data)
		}

		// 非 200 一律视为失败（如路由 404），避免空 data "假成功"（与 doPost 一致）
		if httpResp.StatusCode != http.StatusOK {
			return model.NewSDKError(model.ErrCodeInternalError,
				fmt.Sprintf("http status %d, url=%s, body=%s", httpResp.StatusCode, fullURL, truncateBody(data)))
		}

		var parsed postResp
		if err := json.Unmarshal(data, &parsed); err != nil {
			return fmt.Errorf("parse response: %w", err)
		}
		if parsed.Code == 0 {
			if resp != nil && len(parsed.Data) > 0 {
				if err := json.Unmarshal(parsed.Data, resp); err != nil {
					return fmt.Errorf("unmarshal data: %w", err)
				}
			}
			return nil
		}
		return model.NewSDKError(parsed.Code, parsed.Msg)
	}
	if lastErr != nil {
		return fmt.Errorf("http request failed after retries: %w", lastErr)
	}
	return fmt.Errorf("http request failed after retries")
}

// truncateBody 错误信息里附带的响应体片段（防超长刷屏）
func truncateBody(data []byte) string {
	const max = 256
	if len(data) > max {
		return string(data[:max]) + "..."
	}
	return string(data)
}

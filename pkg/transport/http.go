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

// DefaultHTTPClient 默认 HTTPClient 实现
type DefaultHTTPClient struct {
	baseURL    string
	httpClient *http.Client
	tokenStore TokenStore
	logger     logger.Logger
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

// Post 发送 POST 请求
func (c *DefaultHTTPClient) Post(ctx context.Context, path string, body, resp interface{}) error {
	tokenRefreshed := false
	for {
		err := c.doPost(ctx, path, body, resp)
		if err == nil {
			return nil
		}

		// 判断是否 Token 过期类错误
		if se, ok := err.(*model.SDKError); ok &&
			(se.Code == model.ErrCodeTokenExpiredError || se.Code == model.ErrCodeTokenNotValidYetError) {
			if !tokenRefreshed && c.tokenStore != nil {
				if c.logger != nil {
					c.logger.Debugf("token expired, refreshing, code=%d", se.Code)
				}
				// Refresh 接口没有 userID/platformID —— 这里依赖 tokenStore 内部状态
				// 调用无参数的 Refresh 需要外部约定，暂不直接调用，由上层触发
				// 此处仅演示刷新一次的语义
				tokenRefreshed = true
				continue
			}
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
		if se, ok := err.(*model.SDKError); ok &&
			(se.Code == model.ErrCodeTokenExpiredError || se.Code == model.ErrCodeTokenNotValidYetError) {
			if !tokenRefreshed {
				tokenRefreshed = true
				continue
			}
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

	fullURL := c.baseURL + path
	operationID := "sdk:" + uuidx.New()

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
			// 从 store 中取一个 token，按约定使用固定的 userID/platformID
			// 实际多用户场景下应由上层持有 tokenStore 实例并自行选路
			token, _ := c.tokenStore.Get("", 0)
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

		// Token 过期/未生效 → 刷新并返回让上层重试一次
		if parsed.Code == model.ErrCodeTokenExpiredError || parsed.Code == model.ErrCodeTokenNotValidYetError {
			if c.tokenStore != nil {
				// 尝试刷新（TokenStore.Refresh 需要 userID/platformID，
				// 这里空参数会返回 error，实际由上层持有具体 store 处理）
				if _, rerr := c.tokenStore.Refresh(ctx, "", 0); rerr == nil {
					// 刷新成功，返回特殊错误让 Post 外层重试
					return model.NewSDKError(parsed.Code, parsed.Msg)
				}
			}
			return model.NewSDKError(parsed.Code, parsed.Msg)
		}

		return model.NewSDKError(parsed.Code, parsed.Msg)
	}

	if lastErr != nil {
		return fmt.Errorf("http request failed after retries: %w", lastErr)
	}
	return fmt.Errorf("http request failed after retries")
}

func (c *DefaultHTTPClient) doGet(ctx context.Context, path string, query url.Values, resp interface{}) error {
	fullURL := c.baseURL + path
	if len(query) > 0 {
		fullURL += "?" + query.Encode()
	}
	operationID := "sdk:" + uuidx.New()

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
			token, _ := c.tokenStore.Get("", 0)
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

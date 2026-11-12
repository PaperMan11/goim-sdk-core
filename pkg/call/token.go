package call

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// TokenProviderBridge TokenProvider 的跨语言契约（Native 实现此 interface）
//
// gomobile 硬约束：Native 只能实现单方法 interface，且参数必须是基本类型，
// 所以 token + 过期时间打包成 JSON string 返回：
//
//	{"token":"...","expireTimeSeconds":3600}
//
// ⚠️ 线程约束：FetchToken 在 SDK 的刷新 goroutine 里被同步调用，Native 侧
// 禁止在 UI 主线程做网络请求（gomobile 生成的代理已在后台线程，Native 实现里
// 自行同步请求宿主业务服务端即可）。
//
// wasm 场景无同步跨语言回调：用「预取模式」——JS 刷新 token 后主动调
// UpdateToken 注入，等价于宿主主动注入而非回调。
type TokenProviderBridge interface {
	// FetchToken SDK token 过期时同步调用；返回 JSON {"token","expireTimeSeconds"}
	// error 非空 → 该次请求 / 重连失败并上报事件（宿主可监听后引导重新登录）
	FetchToken(userID string, platformID int32) (string, error)
}

// tokenFetchResult FetchToken 返回 JSON 的结构
type tokenFetchResult struct {
	Token             string `json:"token"`
	ExpireTimeSeconds int64  `json:"expireTimeSeconds"`
}

// SetTokenProvider 注册 token 刷新桥（终端模式推荐）。
//
// ⚠️ 必须在 Init 之前调用（ClientConfig.TokenProvider 在构建 Client 时定型）。
// SetClient 场景由宿主在 Go 侧直接配置 ClientConfig.TokenProvider，无需此桥。
func (d *Dispatcher) SetTokenProvider(p TokenProviderBridge) {
	d.mu.Lock()
	d.tokenProvider = p
	d.mu.Unlock()
}

// UpdateToken wasm 预取模式：JS / Native 侧刷新 token 后主动注入。
//
// 未设置 TokenProviderBridge 时，Init 会把 call 层内部缓存包装成
// ClientConfig.TokenProvider——SDK token 过期触发刷新时读取最近一次
// UpdateToken 注入的值。对已登录会话：当前请求失败触发刷新即取到新 token；
// 若需立即生效，宿主应走 LoginWithToken 重新登录。
func (d *Dispatcher) UpdateToken(userID string, platformID int32, token string, expireTimeSeconds int64) {
	d.tokenCache.set(userID, platformID, token, expireTimeSeconds)
}

// resolveTokenProvider Init 时决定 ClientConfig.TokenProvider。
//
// 终端模式（无 AdminSecret）返回一个动态 provider：每次 token 刷新时重新读取
// d.tokenProvider（bridge）或 d.tokenCache（预取），因此 SetTokenProvider /
// UpdateToken 在 Init 之后调用也能生效——这对 gomobile/cgo/wasm 三端都很重要，
// 因为宿主经常在 Init 之后才完成回调注册。
//
// AdminSecret 模式且未设 bridge 时返回 nil，交给 core 层 admin bootstrap。
func (d *Dispatcher) resolveTokenProvider(hasAdminSecret bool) func(ctx context.Context, userID string, platformID int32) (string, int64, error) {
	d.mu.RLock()
	tp := d.tokenProvider
	d.mu.RUnlock()
	if tp == nil && hasAdminSecret {
		return nil
	}
	return d.refreshTokenProvider
}

// refreshTokenProvider 动态 token 刷新入口（注入到 ClientConfig.TokenProvider）。
// 优先级：TokenProviderBridge > UpdateToken 预取缓存。
func (d *Dispatcher) refreshTokenProvider(ctx context.Context, userID string, platformID int32) (string, int64, error) {
	d.mu.RLock()
	tp := d.tokenProvider
	d.mu.RUnlock()
	if tp != nil {
		resultJSON, err := tp.FetchToken(userID, platformID)
		if err != nil {
			return "", 0, err
		}
		return parseTokenResult(resultJSON)
	}
	return d.tokenCache.get(userID, platformID)
}

// wrapTokenProvider TokenProviderBridge → client.ClientConfig.TokenProvider
func wrapTokenProvider(p TokenProviderBridge) func(ctx context.Context, userID string, platformID int32) (string, int64, error) {
	return func(ctx context.Context, userID string, platformID int32) (string, int64, error) {
		resultJSON, err := p.FetchToken(userID, platformID)
		if err != nil {
			return "", 0, err
		}
		return parseTokenResult(resultJSON)
	}
}

func parseTokenResult(resultJSON string) (string, int64, error) {
	var r tokenFetchResult
	if err := json.Unmarshal([]byte(resultJSON), &r); err != nil {
		return "", 0, fmt.Errorf("token provider: decode result: %w", err)
	}
	if r.Token == "" {
		return "", 0, fmt.Errorf("token provider returned empty token")
	}
	return r.Token, r.ExpireTimeSeconds, nil
}

// prefetchedTokenCache UpdateToken 预取缓存（wasm / 无回调平台用）
type prefetchedTokenCache struct {
	mu                sync.RWMutex
	userID            string
	platformID        int32
	token             string
	expireTimeSeconds int64
}

func (c *prefetchedTokenCache) set(userID string, platformID int32, token string, expireTimeSeconds int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.userID, c.platformID = userID, platformID
	c.token, c.expireTimeSeconds = token, expireTimeSeconds
}

// get 读取预取缓存；缓存为空或账号不匹配时返回错误
// （该次刷新失败并上报事件，宿主应随后 UpdateToken 或重新登录）
func (c *prefetchedTokenCache) get(userID string, platformID int32) (string, int64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.token == "" || c.userID != userID || c.platformID != platformID {
		return "", 0, fmt.Errorf("token provider: no prefetched token for %s/%d, call UpdateToken first", userID, platformID)
	}
	return c.token, c.expireTimeSeconds, nil
}

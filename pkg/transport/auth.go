package transport

import (
	"context"
	"fmt"
	"sync"

	"github.com/PaperMan11/goim-sdk-core/pkg/utils/convert"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/timex"
)

// TokenResult 获取 Token 的返回结构
type TokenResult struct {
	Token             string `json:"token"`
	ExpireTimeSeconds int64  `json:"expireTimeSeconds"`
}

// TokenInfo 解析 Token 的返回结构
type TokenInfo struct {
	UserID            string `json:"userID"`
	PlatformID        int32  `json:"platformID"`
	ExpireTimeSeconds int64  `json:"expireTimeSeconds"`
}

// TokenStore Token 存储接口 —— 按 (userID, platformID) 多维度存储
type TokenStore interface {
	// Set 存入 token，expireTimeSeconds 秒后过期
	Set(userID string, platformID int32, token string, expireTimeSeconds int64) error
	// Get 获取 token，返回空串表示未找到或已过期
	Get(userID string, platformID int32) (string, error)
	// Delete 删除 token（登出时调用）
	Delete(userID string, platformID int32) error
	// Refresh 刷新 token（Token 过期时由 HTTPClient 调用）
	// 具体刷新逻辑由外部注入（因为需要调用 AuthMgr.GetUserToken）
	Refresh(ctx context.Context, userID string, platformID int32) (string, error)
}

// tokenEntry 内部存储结构
type tokenEntry struct {
	token    string
	expireAt int64 // unix 秒时间戳
}

// RefreshFunc 外部注入的刷新逻辑
type RefreshFunc func(ctx context.Context, userID string, platformID int32) (string, int64, error)

// defaultTokenStore 默认 TokenStore 实现
type defaultTokenStore struct {
	mu      sync.RWMutex
	tokens  map[string]tokenEntry
	refresh RefreshFunc
}

// NewTokenStore 创建一个 TokenStore
// refresh 闭包由上层注入，返回 (token, expireTimeSeconds, error)
func NewTokenStore(refresh RefreshFunc) TokenStore {
	return &defaultTokenStore{
		tokens:  make(map[string]tokenEntry),
		refresh: refresh,
	}
}

func key(userID string, platformID int32) string {
	return userID + ":" + convert.ToString(platformID)
}

func (s *defaultTokenStore) Set(userID string, platformID int32, token string, expireTimeSeconds int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokens[key(userID, platformID)] = tokenEntry{
		token:    token,
		expireAt: timex.Unix() + expireTimeSeconds,
	}
	return nil
}

func (s *defaultTokenStore) Get(userID string, platformID int32) (string, error) {
	s.mu.RLock()
	k := key(userID, platformID)
	entry, ok := s.tokens[k]
	s.mu.RUnlock()
	if !ok {
		return "", nil
	}
	if timex.Unix() >= entry.expireAt {
		s.mu.Lock()
		// 写锁内复查：entry 可能已被其他 goroutine 删除，或被 Refresh 替换为未过期的新 token，
		// 无条件 delete 会误删新 token
		if cur, ok := s.tokens[k]; ok && timex.Unix() >= cur.expireAt {
			delete(s.tokens, k)
		}
		s.mu.Unlock()
		return "", nil
	}
	return entry.token, nil
}

func (s *defaultTokenStore) Delete(userID string, platformID int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tokens, key(userID, platformID))
	return nil
}

func (s *defaultTokenStore) Refresh(ctx context.Context, userID string, platformID int32) (string, error) {
	if s.refresh == nil {
		return "", fmt.Errorf("token refresh function not configured")
	}
	token, expire, err := s.refresh(ctx, userID, platformID)
	if err != nil {
		return "", err
	}
	if err := s.Set(userID, platformID, token, expire); err != nil {
		return "", err
	}
	return token, nil
}

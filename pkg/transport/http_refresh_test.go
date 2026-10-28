package transport

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// TestPostRefreshRetryOnAuthError 认证错误 → 真正刷新 token → 重试成功（R2 修复回归）。
// 服务端场景：token 记录先于 JWT exp 失效（Redis TTL），返回 1507 TokenNotExistError；
// SDK 应调用 TokenStore.Refresh 取新 token 后重试一次，而不是拿着旧 token 空转。
func TestPostRefreshRetryOnAuthError(t *testing.T) {
	var calls atomic.Int32
	var gotTokens []string
	refreshed := false

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		tok := r.Header.Get("token")
		gotTokens = append(gotTokens, tok)
		calls.Add(1)
		if !refreshed {
			// 第一次：token 记录不存在（服务端短 TTL 已清除）
			refreshed = true
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 1507, "msg": "token does not exist"})
			return
		}
		// 刷新后的请求：携带新 token，成功
		if tok != "new-tok" {
			t.Errorf("retry should carry refreshed token, got %q", tok)
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0, "msg": "", "data": map[string]string{"ok": "1"}})
	}))
	defer srv.Close()

	store := NewTokenStore(func(ctx context.Context, userID string, platformID int32) (string, int64, error) {
		return "new-tok", 3600, nil
	})
	_ = store.Set("u1", 1, "old-tok", 3600)

	c := NewHTTPClient(srv.URL, store, nil)
	c.SetCurrentUser("u1", 1)

	var out struct {
		OK string `json:"ok"`
	}
	if err := c.Post(context.Background(), "/user/get_users_info", nil, &out); err != nil {
		t.Fatalf("post after refresh should succeed, err=%v", err)
	}
	if calls.Load() != 2 {
		t.Errorf("expected 2 requests (fail + retry), got %d", calls.Load())
	}
	if len(gotTokens) != 2 || gotTokens[0] != "old-tok" || gotTokens[1] != "new-tok" {
		t.Errorf("token sequence = %v, want [old-tok new-tok]", gotTokens)
	}
	// 新 token 已写回 store
	if tok, _ := store.Get("u1", 1); tok != "new-tok" {
		t.Errorf("store token after refresh = %q, want new-tok", tok)
	}
}

// TestPostNoRefreshWithoutUser 未登录态（无当前用户）认证错误不触发刷新，直接返回错误
func TestPostNoRefreshWithoutUser(t *testing.T) {
	refreshCalled := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 1507, "msg": "token does not exist"})
	}))
	defer srv.Close()

	store := NewTokenStore(func(ctx context.Context, userID string, platformID int32) (string, int64, error) {
		refreshCalled = true
		return "x", 60, nil
	})
	c := NewHTTPClient(srv.URL, store, nil) // 未 SetCurrentUser

	err := c.Post(context.Background(), "/auth/get_admin_token", nil, nil)
	if err == nil {
		t.Fatal("auth error without current user should be returned as-is")
	}
	if refreshCalled {
		t.Error("refresh must not be called without a current user")
	}
}

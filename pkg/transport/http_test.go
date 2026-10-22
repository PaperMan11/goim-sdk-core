package transport

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
)

func TestHTTPClient_PostSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.Header.Get("operationID") == "" {
			t.Error("missing operationID header")
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Content-Type = %s", r.Header.Get("Content-Type"))
		}

		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["key"] != "value" {
			t.Errorf("body = %v", body)
		}

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"code": 0,
			"msg":  "",
			"data": map[string]string{"name": "goim"},
		})
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, nil, &logger.NopLogger{})

	var resp struct {
		Name string `json:"name"`
	}
	if err := c.Post(context.Background(), "/test", map[string]string{"key": "value"}, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Name != "goim" {
		t.Fatalf("resp.Name = %q, want goim", resp.Name)
	}
}

func TestHTTPClient_PostBizError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"code": 1001,
			"msg":  "user not found",
		})
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, nil, &logger.NopLogger{})

	err := c.Post(context.Background(), "/test", nil, nil)
	if err == nil {
		t.Fatal("expected error")
	}

	se, ok := err.(*model.SDKError)
	if !ok {
		t.Fatalf("error type = %T, want *model.SDKError", err)
	}
	if se.Code != 1001 {
		t.Fatalf("code = %d, want 1001", se.Code)
	}
}

func TestHTTPClient_TokenHeader(t *testing.T) {
	var gotToken string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken = r.Header.Get("token")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0})
	}))
	defer srv.Close()

	store := NewTokenStore(nil)
	// http 层当前用固定 key Get("", 0)，对齐此行为
	_ = store.Set("", 0, "tok-abc", 3600)

	c := NewHTTPClient(srv.URL, store, &logger.NopLogger{})
	if err := c.Post(context.Background(), "/test", nil, nil); err != nil {
		t.Fatal(err)
	}
	if gotToken != "tok-abc" {
		t.Fatalf("token header = %q, want tok-abc", gotToken)
	}
}

func TestHTTPClient_GetSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		if r.URL.Query().Get("page") != "2" {
			t.Errorf("query page = %q", r.URL.Query().Get("page"))
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"code": 0,
			"data": map[string]int{"total": 42},
		})
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, nil, &logger.NopLogger{})

	q := url.Values{}
	q.Set("page", "2")

	var resp struct {
		Total int `json:"total"`
	}
	if err := c.Get(context.Background(), "/list", q, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Total != 42 {
		t.Fatalf("resp.Total = %d, want 42", resp.Total)
	}
}

func TestHTTPClient_ContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second) // 慢响应
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0})
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, nil, &logger.NopLogger{})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := c.Post(ctx, "/slow", nil, nil)
	if err == nil {
		t.Fatal("expected context deadline error")
	}
}

func TestHTTPClient_TokenExpiredRetriesOnce(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"code": model.ErrCodeTokenExpiredError,
			"msg":  "token expired",
		})
	}))
	defer srv.Close()

	// refresh 未配置 → Refresh 返回 error → 外层仍会重试一次后放弃
	c := NewHTTPClient(srv.URL, NewTokenStore(nil), &logger.NopLogger{})

	err := c.Post(context.Background(), "/test", nil, nil)
	if err == nil {
		t.Fatal("expected SDKError")
	}
	se, ok := err.(*model.SDKError)
	if !ok || se.Code != model.ErrCodeTokenExpiredError {
		t.Fatalf("err = %v", err)
	}
	// tokenRefreshed 逻辑会让外层重试一次 → 共 2 次请求
	if calls != 2 {
		t.Fatalf("server called %d times, want 2 (initial + 1 retry)", calls)
	}
}

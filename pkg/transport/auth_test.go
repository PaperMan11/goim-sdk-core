package transport

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/PaperMan11/goim-sdk-core/pkg/utils/timex"
)

func TestTokenStore_SetGet(t *testing.T) {
	s := NewTokenStore(nil)

	if err := s.Set("u1", 1, "tok-1", 3600); err != nil {
		t.Fatal(err)
	}

	tok, err := s.Get("u1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if tok != "tok-1" {
		t.Fatalf("got %q, want tok-1", tok)
	}
}

func TestTokenStore_GetMiss(t *testing.T) {
	s := NewTokenStore(nil)

	tok, err := s.Get("no-such", 1)
	if err != nil {
		t.Fatal(err)
	}
	if tok != "" {
		t.Fatalf("expected empty token, got %q", tok)
	}
}

func TestTokenStore_GetExpired(t *testing.T) {
	s := NewTokenStore(nil)

	// expireTimeSeconds=0 → 立即过期
	if err := s.Set("u1", 1, "tok-1", 0); err != nil {
		t.Fatal(err)
	}

	tok, _ := s.Get("u1", 1)
	if tok != "" {
		t.Fatalf("expired token should be empty, got %q", tok)
	}

	// 过期后应从 map 中清除
	if err := s.Set("u1", 1, "tok-2", 3600); err != nil {
		t.Fatal(err)
	}
	tok, _ = s.Get("u1", 1)
	if tok != "tok-2" {
		t.Fatalf("got %q, want tok-2", tok)
	}
}

func TestTokenStore_MultiUserIsolation(t *testing.T) {
	s := NewTokenStore(nil)

	_ = s.Set("u1", 1, "tok-u1-p1", 3600)
	_ = s.Set("u1", 2, "tok-u1-p2", 3600)
	_ = s.Set("u2", 1, "tok-u2-p1", 3600)

	cases := []struct {
		userID     string
		platformID int32
		want       string
	}{
		{"u1", 1, "tok-u1-p1"},
		{"u1", 2, "tok-u1-p2"},
		{"u2", 1, "tok-u2-p1"},
	}
	for _, c := range cases {
		tok, _ := s.Get(c.userID, c.platformID)
		if tok != c.want {
			t.Fatalf("(%s,%d): got %q, want %q", c.userID, c.platformID, tok, c.want)
		}
	}
}

func TestTokenStore_Delete(t *testing.T) {
	s := NewTokenStore(nil)

	_ = s.Set("u1", 1, "tok-1", 3600)
	if err := s.Delete("u1", 1); err != nil {
		t.Fatal(err)
	}

	tok, _ := s.Get("u1", 1)
	if tok != "" {
		t.Fatalf("deleted token should be empty, got %q", tok)
	}
}

func TestTokenStore_Refresh(t *testing.T) {
	refreshCalled := 0
	s := NewTokenStore(func(ctx context.Context, userID string, platformID int32) (string, int64, error) {
		refreshCalled++
		if userID != "u1" || platformID != 1 {
			return "", 0, errors.New("unexpected args")
		}
		return "new-tok", 7200, nil
	})

	tok, err := s.Refresh(context.Background(), "u1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if tok != "new-tok" {
		t.Fatalf("got %q, want new-tok", tok)
	}
	if refreshCalled != 1 {
		t.Fatalf("refresh called %d times, want 1", refreshCalled)
	}

	// 刷新后应已写入 store
	stored, _ := s.Get("u1", 1)
	if stored != "new-tok" {
		t.Fatalf("stored = %q, want new-tok", stored)
	}
}

func TestTokenStore_RefreshNotConfigured(t *testing.T) {
	s := NewTokenStore(nil)

	_, err := s.Refresh(context.Background(), "u1", 1)
	if err == nil {
		t.Fatal("expected error when refresh func not configured")
	}
}

func TestTokenStore_RefreshErrorPropagates(t *testing.T) {
	want := errors.New("refresh failed")
	s := NewTokenStore(func(ctx context.Context, userID string, platformID int32) (string, int64, error) {
		return "", 0, want
	})

	_, err := s.Refresh(context.Background(), "u1", 1)
	if !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
}

// 并发交错复现 Get 过期删除的竞态：
// A 读到过期 entry（RUnlock）→ B 写入新 token → A 拿写锁无条件 delete → 新 token 被误删。
// 修复后 A 必须在写锁内复查"仍存在且仍过期"才删除。
func TestTokenStore_GetExpiredNoDeleteFresh_Concurrent(t *testing.T) {
	s := NewTokenStore(nil).(*defaultTokenStore)

	const iterations = 3000
	var freshLost int32

	for i := 0; i < iterations; i++ {
		// 预置已过期 entry（expireAt 在过去）
		s.mu.Lock()
		s.tokens[key("u1", 1)] = tokenEntry{token: "stale", expireAt: timex.Unix() - 1}
		s.mu.Unlock()

		start := make(chan struct{})
		done := make(chan struct{}, 2)

		// B: 模拟 Refresh 成功写入新 token（未来过期）
		go func() {
			defer func() { done <- struct{}{} }()
			<-start
			_ = s.Set("u1", 1, "fresh", 3600)
		}()

		// A: Get 走过期删除分支
		go func() {
			defer func() { done <- struct{}{} }()
			<-start
			_, _ = s.Get("u1", 1)
		}()

		close(start)
		<-done
		<-done

		// 两方都结束后，fresh 必须存活（未被 A 误删）
		if tok, _ := s.Get("u1", 1); tok != "fresh" {
			atomic.AddInt32(&freshLost, 1)
		}
	}

	if freshLost > 0 {
		t.Fatalf("fresh token wrongly deleted in %d/%d iterations", freshLost, iterations)
	}
}

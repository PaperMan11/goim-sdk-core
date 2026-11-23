package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
)

// TestTokenRefreshEvents 自动续期结果事件：成功 → EventTokenRefreshed；
// 失败 → EventTokenRefreshFailed（携带账号与原因，供预取模式宿主感知并补发 token）。
func TestTokenRefreshEvents(t *testing.T) {
	var providerErr error
	cli := NewClient(&ClientConfig{
		Logger: &logger.NopLogger{},
		TokenProvider: func(ctx context.Context, userID string, platformID int32) (string, int64, error) {
			if providerErr != nil {
				return "", 0, providerErr
			}
			return "tk-new", 3600, nil
		},
	}).(*DefaultClient)
	defer cli.Close()

	okCh, unsubOK := cli.Subscribe(events.EventTokenRefreshed)
	defer unsubOK()
	failCh, unsubFail := cli.Subscribe(events.EventTokenRefreshFailed)
	defer unsubFail()

	// 成功续期
	if _, err := cli.tokens.Refresh(context.Background(), "u1", 1); err != nil {
		t.Fatalf("refresh success: %v", err)
	}
	select {
	case ev := <-okCh:
		data, ok := ev.Payload.(*model.TokenRefreshedData)
		if !ok || data.UserID != "u1" || data.PlatformID != 1 {
			t.Fatalf("EventTokenRefreshed payload = %#v", ev.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no EventTokenRefreshed")
	}

	// 失败续期（预取缓存无可用 token / 回调报错）
	providerErr = errors.New("token service unavailable")
	if _, err := cli.tokens.Refresh(context.Background(), "u1", 1); err == nil {
		t.Fatal("refresh: want error, got nil")
	}
	select {
	case ev := <-failCh:
		data, ok := ev.Payload.(*model.TokenRefreshFailedData)
		if !ok || data.UserID != "u1" || data.PlatformID != 1 || data.Error != "token service unavailable" {
			t.Fatalf("EventTokenRefreshFailed payload = %#v", ev.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no EventTokenRefreshFailed")
	}
}

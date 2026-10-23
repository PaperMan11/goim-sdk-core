package manager

import (
	"context"
	"fmt"
	"time"

	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/eventbus"
)

// DefaultAuthMgr AuthMgr 默认实现（纯 HTTP）
type DefaultAuthMgr struct {
	http   transport.HTTPClient
	tokens transport.TokenStore
	bus    *eventbus.EventBus
	logger logger.Logger
}

// NewAuthMgr 构造 AuthMgr
func NewAuthMgr(http transport.HTTPClient, tokens transport.TokenStore, bus *eventbus.EventBus, l logger.Logger) *DefaultAuthMgr {
	return &DefaultAuthMgr{http: http, tokens: tokens, bus: bus, logger: l}
}

func (m *DefaultAuthMgr) GetAdminToken(ctx context.Context, secret, userID string) (*transport.TokenResult, error) {
	req := struct {
		Secret string `json:"secret"`
		UserID string `json:"userID"`
	}{Secret: secret, UserID: userID}

	var resp transport.TokenResult
	if err := m.http.Post(ctx, "/auth/get_admin_token", req, &resp); err != nil {
		return nil, fmt.Errorf("get admin token: %w", err)
	}
	return &resp, nil
}

func (m *DefaultAuthMgr) GetUserToken(ctx context.Context, userID string, platformID int32) (*transport.TokenResult, error) {
	req := struct {
		UserID     string `json:"userID"`
		PlatformID int32  `json:"platformID"`
	}{UserID: userID, PlatformID: platformID}

	var resp transport.TokenResult
	if err := m.http.Post(ctx, "/auth/get_user_token", req, &resp); err != nil {
		if m.bus != nil {
			m.bus.Publish(events.EventLoginFailed, events.Event{
				Type:      events.EventLoginFailed,
				Timestamp: time.Now().UnixMilli(),
			})
		}
		return nil, fmt.Errorf("get user token: %w", err)
	}

	// 缓存 token
	if m.tokens != nil {
		_ = m.tokens.Set(userID, platformID, resp.Token, resp.ExpireTimeSeconds)
	}

	// 派发登录成功事件
	if m.bus != nil {
		m.bus.Publish(events.EventLoginSuccess, events.Event{
			Type:      events.EventLoginSuccess,
			Timestamp: time.Now().UnixMilli(),
			Payload:   &resp,
		})
	}

	return &resp, nil
}

func (m *DefaultAuthMgr) ParseToken(ctx context.Context, token string) (*transport.TokenInfo, error) {
	req := struct {
		Token string `json:"token"`
	}{Token: token}

	var resp transport.TokenInfo
	if err := m.http.Post(ctx, "/auth/parse_token", req, &resp); err != nil {
		return nil, fmt.Errorf("parse token: %w", err)
	}
	return &resp, nil
}

func (m *DefaultAuthMgr) ForceLogout(ctx context.Context, userID string, platformID int32) error {
	req := struct {
		UserID     string `json:"userID"`
		PlatformID int32  `json:"platformID"`
	}{UserID: userID, PlatformID: platformID}

	if err := m.http.Post(ctx, "/auth/force_logout", req, nil); err != nil {
		return fmt.Errorf("force logout: %w", err)
	}

	// 清理本地 token
	if m.tokens != nil {
		_ = m.tokens.Delete(userID, platformID)
	}
	return nil
}

package manager

import (
	"context"

	"github.com/PaperMan11/goim-sdk-core/pkg/transport"
)

// AuthMgr 认证相关操作
//
// 实现要点:
//   - GetUserToken 成功后调用 TokenStore.Set 缓存 token
//   - ForceLogout 后 MessageTransport 应触发重连或 Disconnect
type AuthMgr interface {
	GetAdminToken(ctx context.Context, secret, userID string) (*transport.TokenResult, error)
	GetUserToken(ctx context.Context, userID string, platformID int32) (*transport.TokenResult, error)
	ParseToken(ctx context.Context, token string) (*transport.TokenInfo, error)
	ForceLogout(ctx context.Context, userID string, platformID int32) error
}

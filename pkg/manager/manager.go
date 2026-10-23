// Package manager 业务编排层接口骨架 —— 先接口后实现，锁定边界。
//
// 依赖方向：manager → transport + model + events
// 实现层（Phase 3 逐个填充）将组合 HTTPClient / MessageTransport / TokenStore。
package manager

import (
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"
)

// Manager 六个业务 Manager 的聚合接口（client 层持有）
type Manager interface {
	Auth() AuthMgr
	User() UserMgr
	Msg() MsgMgr
	Group() GroupMgr
	Rel() RelMgr
	Conv() ConvMgr
}

// ManagerDeps Manager 构造函数的依赖注入
type ManagerDeps struct {
	HTTP       transport.HTTPClient
	Transport  transport.MessageTransport
	TokenStore transport.TokenStore
}

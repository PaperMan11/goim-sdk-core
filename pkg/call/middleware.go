package call

import (
	"fmt"
	"runtime/debug"

	"github.com/PaperMan11/goim-sdk-core/pkg/model"
)

// 登录态校验白名单：未登录也允许调用的方法
//
//	登录 / 注册 / token 类 + Status / SyncState（只读状态查询，UI 启动即可能调）
var authBypassMethods = map[string]struct{}{
	"Login":                 {},
	"LoginWithToken":        {},
	"Status":                {},
	"SyncState":             {},
	"UserMgr.Register":      {},
	"AuthMgr.ParseToken":    {},
	"AuthMgr.GetAdminToken": {},
	"AuthMgr.GetUserToken":  {},
}

func isAuthBypass(method string) bool {
	_, ok := authBypassMethods[method]
	return ok
}

// withMiddleware 调用中间件：panic recover
//
// panic 不能穿透跨语言边界（C/ObjC/Swift 侧没有 Go panic 语义，直接崩溃），
// 统一捕获并归一化为 ErrCodeInternalError(90001)。
// 登录态校验在 invoke 里做（方法查找之后，保证未知方法优先报 UnknownMethod）。
func (d *Dispatcher) withMiddleware(method string, fn func() (string, error)) (result string, err error) {
	defer func() {
		if r := recover(); r != nil {
			result = ""
			err = model.NewSDKError(model.ErrCodeInternalError,
				fmt.Sprintf("panic in %s: %v\n%s", method, r, debug.Stack()))
		}
	}()

	return fn()
}

package call

import (
	"errors"

	"github.com/PaperMan11/goim-sdk-core/pkg/model"
)

// call 层专属错误码（90010+ 段，与服务端业务码 1001-1605、底层 90001-90007 错开）
const (
	// ErrCodeNotInitialized Dispatcher 未注入 / 未构建 Client
	ErrCodeNotInitialized int32 = 90010
	// ErrCodeNotLoggedIn 未登录（命中登录态白名单之外的方法）
	ErrCodeNotLoggedIn int32 = 90011
	// ErrCodeUnknownMethod method 字符串未注册
	ErrCodeUnknownMethod int32 = 90012
	// ErrCodeInvalidArgs argsJSON 反序列化失败（个数 / 类型不匹配）
	ErrCodeInvalidArgs int32 = 90013
)

// NormalizeError 错误归一化：任意 error → (code, msg)
//
//   - *model.SDKError → 保留服务端原始 code / msg
//   - 其他 error      → ErrCodeInternalError(90001) + err.Error()
func NormalizeError(err error) (int32, string) {
	if err == nil {
		return model.SuccessCode, ""
	}
	var se *model.SDKError
	if errors.As(err, &se) {
		return se.Code, se.Msg
	}
	return model.ErrCodeInternalError, err.Error()
}

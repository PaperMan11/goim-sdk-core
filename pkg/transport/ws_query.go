package transport

import (
	"net/url"

	"github.com/PaperMan11/goim-sdk-core/pkg/utils/convert"
)

// encodeQuery 把认证参数编码为 URL query（WS 特有，与服务端字段名逐一对齐）
//
// 原生与 js/wasm 两种实现共用（见 ws.go / ws_js.go）。
func encodeQuery(p ConnectParams) url.Values {
	q := url.Values{}
	q.Set("userID", p.UserID)
	q.Set("token", p.Token)
	q.Set("platformID", convert.ToString(p.PlatformID))
	q.Set("deviceID", p.DeviceID)
	q.Set("operationID", p.OperationID)
	q.Set("sendResponse", convert.ToString(p.SendResponse))
	q.Set("isBackground", convert.ToString(p.IsBackground))
	q.Set("compression", p.Compression)
	q.Set("sdkType", p.SDKType)
	q.Set("sdkVersion", p.SDKVersion)
	return q
}

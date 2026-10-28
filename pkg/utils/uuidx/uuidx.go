package uuidx

import (
	"strings"

	"github.com/google/uuid"
)

// New 返回一个新的随机 UUID v4 字符串。
func New() string {
	return uuid.NewString()
}

// NewOperationID 生成带操作语义前缀的 operationID：sdk_<op>_<uuid>。
//
// 服务端（im-api / gateway）全链路日志都会打印 operationID，SDK 侧带上操作名后，
// 两边日志用同一个串 grep 即可把一次操作从头跟到尾，如：
//
//	sdk_login_3f2a...          登录链路
//	sdk_group_get_joined_group_list_3f2a...   群同步
//	sdk_sendMsg_3f2a...        发消息（WS 1003）
//
// op 为空时退化为 sdk_<uuid>。op 内的 "/" 转为 "_"（HTTP path 直接当 op 用）。
func NewOperationID(op string) string {
	op = strings.ReplaceAll(strings.Trim(op, "/"), "/", "_")
	if op == "" {
		return "sdk_" + New()
	}
	return "sdk_" + op + "_" + New()
}

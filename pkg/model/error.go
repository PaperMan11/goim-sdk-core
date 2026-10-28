package model

// 错误码常量

const (
	// 0: Success
	SuccessCode int32 = 0

	// 500: 服务端内部错误
	ErrCodeServerInternalError int32 = 500

	// 1001-1050: 参数/权限/冲突
	ErrCodeArgsError             int32 = 1001
	ErrCodeNoPermissionError     int32 = 1002
	ErrCodeDuplicateKeyError     int32 = 1003
	ErrCodeRecordNotFoundError   int32 = 1004
	ErrCodeSecretNotChangedError int32 = 1050

	// 1101-1102: 用户
	ErrCodeUserIDNotFoundError int32 = 1101
	ErrCodeRegisteredAlreadyError int32 = 1102

	// 1201-1210: 群组
	ErrCodeGroupIDNotFoundError   int32 = 1201
	ErrCodeGroupIDExisted         int32 = 1202
	ErrCodeNotInGroupYetError     int32 = 1203
	ErrCodeDismissedAlreadyError  int32 = 1204
	ErrCodeGroupNotFoundError     int32 = 1207
	ErrCodeGroupMutedError        int32 = 1208
	ErrCodeGroupMemberMutedError  int32 = 1209
	ErrCodeGroupDismissedError    int32 = 1210

	// 1301-1305: 关系链
	ErrCodeCanNotAddYourselfError int32 = 1301
	ErrCodeBlockedByPeer          int32 = 1302
	ErrCodeNotPeersFriend         int32 = 1303
	ErrCodeRelationshipAlreadyError int32 = 1304
	ErrCodeFriendRequestHandled   int32 = 1305

	// 1401-1404: 消息
	ErrCodeMessageHasReadDisable int32 = 1401
	ErrCodeMutedInGroup          int32 = 1402
	ErrCodeMutedGroup            int32 = 1403
	ErrCodeMsgAlreadyRevoke      int32 = 1404

	// 1501-1507: Token
	ErrCodeTokenExpiredError      int32 = 1501
	ErrCodeTokenInvalidError      int32 = 1502
	ErrCodeTokenMalformedError    int32 = 1503
	ErrCodeTokenNotValidYetError  int32 = 1504
	ErrCodeTokenUnknownError      int32 = 1505
	ErrCodeTokenKickedError       int32 = 1506
	ErrCodeTokenNotExistError     int32 = 1507

	// 1601-1605: 连接
	ErrCodeConnOverMaxNumLimit int32 = 1601
	ErrCodeConnArgsError       int32 = 1602
	ErrCodePushMsgError        int32 = 1603
	ErrCodeConnResetError      int32 = 1605

	// 90001-90007: 底层
	ErrCodeInternalError int32 = 90001
	ErrCodeDatabaseError int32 = 90002
	ErrCodeNetworkError  int32 = 90004
	ErrCodeDataError     int32 = 90007
)

// SDKError SDK 统一错误
type SDKError struct {
	Code        int32  `json:"code"`
	Msg         string `json:"msg"`
	OperationID string `json:"operationID,omitempty"`
}

// Error 实现 error 接口，直接返回 Msg，不添加任何前缀
func (e *SDKError) Error() string { return e.Msg }

// GetCode 返回错误码
func (e *SDKError) GetCode() int32 { return e.Code }

// NewSDKError 构造一个 SDKError
func NewSDKError(code int32, msg string) *SDKError {
	return &SDKError{Code: code, Msg: msg}
}

// IsTokenExpired 判断是否 Token 过期
func IsTokenExpired(err error) bool {
	if err == nil {
		return false
	}
	if se, ok := err.(*SDKError); ok {
		return se.Code == ErrCodeTokenExpiredError
	}
	return false
}

// IsTokenKicked 判断是否被踢下线
func IsTokenKicked(err error) bool {
	if err == nil {
		return false
	}
	if se, ok := err.(*SDKError); ok {
		return se.Code == ErrCodeTokenKickedError
	}
	return false
}

// IsTokenRefreshable 判断是否可通过刷新 token 恢复的认证错误。
// 覆盖：过期(1501)/无效(1502)/格式错(1503)/未生效(1504)/服务端记录不存在(1507，如
// 服务端 Redis TTL 先于 JWT exp 到期)。1506 被踢(1506) 不可刷新——刷新会对抗踢出逻辑。
func IsTokenRefreshable(err error) bool {
	if err == nil {
		return false
	}
	se, ok := err.(*SDKError)
	if !ok {
		return false
	}
	switch se.Code {
	case ErrCodeTokenExpiredError,
		ErrCodeTokenInvalidError,
		ErrCodeTokenMalformedError,
		ErrCodeTokenNotValidYetError,
		ErrCodeTokenNotExistError:
		return true
	}
	return false
}

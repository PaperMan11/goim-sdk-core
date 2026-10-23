package httpclient

import (
	"context"
	"net/http"

	"github.com/PaperMan11/goim-sdk-core/pkg/utils/timex"
)

const (
	// DefaultRetryTimes 重试次数
	DefaultRetryTimes = 3
	// DefaultRetryDelay 重试前等待时延
	DefaultRetryDelay = timex.Millisecond * 100
)

type RetryVerify func(body []byte) (shouldRetry bool)

func shouldRetry(ctx context.Context, httpCode int) bool {
	select {
	case <-ctx.Done():
		return false
	default:
	}

	switch httpCode {
	case
		_StatusReadRespErr,
		_StatusDoReqErr,

		http.StatusRequestTimeout,
		http.StatusLocked,
		http.StatusTooEarly,
		http.StatusTooManyRequests,

		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

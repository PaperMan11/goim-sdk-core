package httpclient

import (
	"sync"
	"time"
)

var (
	cache = &sync.Pool{
		New: func() interface{} {
			return &option{
				header: make(map[string][]string),
			}
		},
	}
)

type Logger interface {
	Errorf(message string, args ...interface{})
	Warningf(message string, args ...interface{})
	Infof(message string, args ...interface{})
	Debugf(message string, args ...interface{})
	Tracef(message string, args ...interface{})
}

// Mock 定义接口 mock 数据
type Mock func() (body []byte)

type Option func(*option)

type option struct {
	ttl         time.Duration
	header      map[string][]string
	logger      Logger
	retryTimes  int
	retryDelay  time.Duration
	retryVerify RetryVerify
	mock        Mock
}

func (o *option) reset() {
	o.ttl = 0
	o.header = make(map[string][]string)
	o.retryVerify = nil
	o.retryTimes = 0
	o.retryDelay = 0
	o.mock = nil
}

func getOption() *option {
	return cache.Get().(*option)
}

func releaseOption(opt *option) {
	opt.reset()
	cache.Put(opt)
}

// WithTTL http请求最长执行时间
func WithTTL(ttl time.Duration) Option {
	return func(o *option) {
		o.ttl = ttl
	}
}

func WithHeader(key, value string) Option {
	return func(o *option) {
		o.header[key] = []string{value}
	}
}

func WithLogger(log Logger) Option {
	return func(o *option) {
		o.logger = log
	}
}

func WithMock(m Mock) Option {
	return func(o *option) {
		o.mock = m
	}
}

func WithOnFailedRetry(retryTime int, retryDelay time.Duration, retryVerify RetryVerify) Option {
	return func(o *option) {
		o.retryTimes = retryTime
		o.retryDelay = retryDelay
		o.retryVerify = retryVerify
	}
}

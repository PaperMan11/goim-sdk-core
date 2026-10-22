package httpclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"time"
)

const (
	DefaultTTL = time.Minute
)

func Get(url string, form url.Values, options ...Option) (body []byte, err error) {
	return withoutBody(http.MethodGet, url, form, options...)
}

func Post(url string, form url.Values, options ...Option) (body []byte, err error) {
	return withoutBody(http.MethodPost, url, form, options...)
}

func Delete(url string, form url.Values, options ...Option) (body []byte, err error) {
	return withoutBody(http.MethodDelete, url, form, options...)
}

func PostForm(url string, form url.Values, options ...Option) (body []byte, err error) {
	return withFormBody(http.MethodPost, url, form, options...)
}

func PostJSON(url string, raw json.RawMessage, options ...Option) (body []byte, err error) {
	return withJSONBody(http.MethodPost, url, raw, options...)
}

func PutJSON(url string, raw json.RawMessage, options ...Option) (body []byte, err error) {
	return withJSONBody(http.MethodPut, url, raw, options...)
}

func PatchForm(url string, form url.Values, options ...Option) (body []byte, err error) {
	return withFormBody(http.MethodPatch, url, form, options...)
}

func PatchJSON(url string, raw json.RawMessage, options ...Option) (body []byte, err error) {
	return withJSONBody(http.MethodPatch, url, raw, options...)
}

func DeleteJSON(url string, raw json.RawMessage, options ...Option) (body []byte, err error) {
	return withJSONBody(http.MethodDelete, url, raw, options...)
}

func withoutBody(method, url string, form url.Values, options ...Option) (body []byte, err error) {
	if url == "" {
		return nil, errors.New("url required")
	}
	if len(form) > 0 {
		if url, err = addFormValuesIntoURL(url, form); err != nil {
			return nil, err
		}
	}

	opt := getOption()
	defer releaseOption(opt)

	for _, f := range options {
		f(opt)
	}
	opt.header["Content-Type"] = []string{"application/x-www-form-urlencoded; charset=utf-8"}

	ttl := opt.ttl
	if ttl <= 0 {
		ttl = DefaultTTL
	}

	retryTimes := opt.retryTimes
	if retryTimes <= 0 {
		retryTimes = DefaultRetryTimes
	}

	retryDelay := opt.retryDelay
	if retryDelay <= 0 {
		retryDelay = DefaultRetryDelay
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), ttl)
	defer cancelFunc()

	var httpCode int
	for i := 0; i < retryTimes; i++ {
		body, httpCode, err = doHTTP(ctx, method, url, nil, opt)
		if shouldRetry(ctx, httpCode) || (opt.retryVerify != nil && opt.retryVerify(body)) {
			time.Sleep(retryDelay)
			continue
		}
		return
	}
	return
}

func withFormBody(method, url string, form url.Values, options ...Option) (body []byte, err error) {
	if url == "" {
		return nil, errors.New("url required")
	}
	if len(form) == 0 {
		return nil, errors.New("form required")
	}

	opt := getOption()
	defer releaseOption(opt)

	for _, f := range options {
		f(opt)
	}
	opt.header["Content-Type"] = []string{"application/x-www-form-urlencoded; charset=utf-8"}
	formValue := form.Encode()

	ttl := opt.ttl
	if ttl <= 0 {
		ttl = DefaultTTL
	}

	retryTimes := opt.retryTimes
	if retryTimes <= 0 {
		retryTimes = DefaultRetryTimes
	}

	retryDelay := opt.retryDelay
	if retryDelay <= 0 {
		retryDelay = DefaultRetryDelay
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), ttl)
	defer cancelFunc()

	var httpCode int
	for i := 0; i < retryTimes; i++ {
		body, httpCode, err = doHTTP(ctx, method, url, []byte(formValue), opt)
		if shouldRetry(ctx, httpCode) || (opt.retryVerify != nil && opt.retryVerify(body)) {
			time.Sleep(retryDelay)
			continue
		}
		return
	}
	return
}

func withJSONBody(method, url string, raw json.RawMessage, options ...Option) (body []byte, err error) {
	if url == "" {
		return nil, errors.New("url required")
	}
	if len(raw) == 0 {
		return nil, errors.New("json raw required")
	}

	opt := getOption()
	defer releaseOption(opt)

	for _, f := range options {
		f(opt)
	}
	//opt.header["Content-Type"] = []string{"application/json; charset=utf-8"}
	opt.header["Content-Type"] = []string{"application/json"}

	ttl := opt.ttl
	if ttl <= 0 {
		ttl = DefaultTTL
	}

	retryTimes := opt.retryTimes
	if retryTimes <= 0 {
		retryTimes = DefaultRetryTimes
	}

	retryDelay := opt.retryDelay
	if retryDelay <= 0 {
		retryDelay = DefaultRetryDelay
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), ttl)
	defer cancelFunc()

	var httpCode int
	for i := 0; i < retryTimes; i++ {
		body, httpCode, err = doHTTP(ctx, method, url, raw, opt)
		if shouldRetry(ctx, httpCode) || (opt.retryVerify != nil && opt.retryVerify(body)) {
			time.Sleep(retryDelay)
			continue
		}
		return
	}
	return
}

package httpclient

// ReplyErr 错误响应，当 resp.StatusCode != http.StatusOK 时用来包装 httpcode 和 body
type ReplyErr interface {
	error
	StatusCode() int
	Body() []byte
}

var _ ReplyErr = (*replyErr)(nil)

type replyErr struct {
	err        error
	statusCode int
	body       []byte
}

func (r *replyErr) Error() string {
	return r.err.Error()
}

func (r *replyErr) StatusCode() int {
	return r.statusCode
}

func (r *replyErr) Body() []byte {
	return r.body
}

func newReplyErr(statusCode int, body []byte, err error) ReplyErr {
	return &replyErr{
		err:        err,
		statusCode: statusCode,
		body:       body,
	}
}

func ToReplyErr(err error) (ReplyErr, bool) {
	if err == nil {
		return nil, false
	}

	e, ok := err.(ReplyErr)
	return e, ok
}

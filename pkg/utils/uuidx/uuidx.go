package uuidx

import "github.com/google/uuid"

// New 返回一个新的随机 UUID v4 字符串。
func New() string {
	return uuid.NewString()
}

package bcrypt

import (
	"golang.org/x/crypto/bcrypt"
)

const (
	PasswordMinLength = 6
	PasswordMaxLength = 32
)

// HashPassword 使用 bcrypt 对密码进行加密
func HashPassword(password string) string {
	bytes, _ := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	return string(bytes)
}

// CheckPassword 对比明文密码和数据库的哈希值
func CheckPassword(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}

// 验证密码长度
func ValidatePasswordLength(password string) bool {
	return len(password) >= PasswordMinLength && len(password) <= PasswordMaxLength
}

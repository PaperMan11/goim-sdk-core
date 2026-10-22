// Package randx 提供项目统一的随机数工具封装，
// 包含「密码学安全随机」与「高性能伪随机」两套并发安全 API，
// 业务代码禁止直接 import math/rand 或 crypto/rand，应统一通过本包获取随机数。
//
// 两套 API 的选用原则：
//   - Secure* 系列（底层 crypto/rand）：token、锁 value、session、nonce、签名盐等
//     需要密码学不可预测性的场景。
//   - 其余函数（底层 math/rand + sync.Mutex）：TTL 抖动、洗牌、限流 jitter、
//     随机字符串、随机权重等性能敏感且不要求不可预测的场景。
package randx

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	mrand "math/rand"
	"sync"
	"time"

	"github.com/PaperMan11/goim/pkg/utils/timex"
)

const (
	CharsLower    = "abcdefghijklmnopqrstuvwxyz"
	CharsUpper    = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	CharsAlpha    = CharsLower + CharsUpper
	CharsNum      = "0123456789"
	CharsAlphaNum = CharsAlpha + CharsNum
	CharsHex      = "0123456789abcdef"
)

var (
	fastRand *mrand.Rand
	fastMu   sync.Mutex
)

func init() {
	fastRand = mrand.New(mrand.NewSource(timex.UnixNano()))
}

// SecureBytes 返回 n 字节的密码学安全随机字节切片。
// 底层基于 crypto/rand.Read，失败时返回 nil 与错误（如系统熵池不可用）。
// n <= 0 时返回长度为 0 的空切片且无错误。
// 并发安全。
func SecureBytes(n int) ([]byte, error) {
	if n <= 0 {
		return []byte{}, nil
	}
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// SecureString 返回长度为 n 的密码学安全随机字符串，每个字符从 chars 中均匀抽取。
// chars 长度 <= 256 时走一次 SecureBytes(n) 的快速路径；
// chars 长度 > 256 时每个位置单独读 4 字节以保证分布均匀。
// n <= 0 或 chars 为空时返回空字符串且无错误；crypto/rand 读失败时返回错误。
// 并发安全。
func SecureString(n int, chars string) (string, error) {
	if n <= 0 {
		return "", nil
	}
	alphabetLen := len(chars)
	if alphabetLen == 0 {
		return "", nil
	}

	result := make([]byte, n)
	if alphabetLen <= 0xFF {
		b, err := SecureBytes(n)
		if err != nil {
			return "", err
		}
		for i := 0; i < n; i++ {
			result[i] = chars[int(b[i])%alphabetLen]
		}
		return string(result), nil
	}

	buf := make([]byte, 4)
	for i := 0; i < n; i++ {
		if _, err := rand.Read(buf); err != nil {
			return "", err
		}
		idx := int(binary.BigEndian.Uint32(buf))
		if idx < 0 {
			idx = -idx
		}
		result[i] = chars[idx%alphabetLen]
	}
	return string(result), nil
}

// SecureHex 返回长度为 n 的密码学安全十六进制字符串（小写）。
// 内部读取 ceil(n/2) 字节再 hex.Encode 后截断前 n 位。
// n <= 0 时返回空字符串且无错误；随机源读取失败时返回错误。
// 并发安全。
func SecureHex(n int) (string, error) {
	if n <= 0 {
		return "", nil
	}
	raw := (n + 1) / 2
	b, err := SecureBytes(raw)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(b)[:n], nil
}

// Intn 返回 [0, n) 范围内均匀分布的伪随机整数。
// n <= 0 时直接返回 0；通过互斥锁保护底层 PRNG，并发安全。
func Intn(n int) int {
	if n <= 0 {
		return 0
	}
	fastMu.Lock()
	v := fastRand.Intn(n)
	fastMu.Unlock()
	return v
}

// Int31n 返回 [0, n) 范围内均匀分布的伪随机 int32。
// n <= 0 时直接返回 0；并发安全。
func Int31n(n int32) int32 {
	if n <= 0 {
		return 0
	}
	fastMu.Lock()
	v := fastRand.Int31n(n)
	fastMu.Unlock()
	return v
}

// Int63n 返回 [0, n) 范围内均匀分布的伪随机 int64。
// n <= 0 时直接返回 0；并发安全。
func Int63n(n int64) int64 {
	if n <= 0 {
		return 0
	}
	fastMu.Lock()
	v := fastRand.Int63n(n)
	fastMu.Unlock()
	return v
}

// IntnRange 返回 [min, max) 范围内均匀分布的伪随机整数。
// 当 min >= max 时直接返回 min（不做交换与报错）；并发安全。
func IntnRange(min, max int) int {
	if min >= max {
		return min
	}
	return min + Intn(max-min)
}

// IntRangeInclusive 返回 [min, max] 范围内（含两端）均匀分布的伪随机整数。
// 当 min >= max 时直接返回 min；并发安全。
func IntRangeInclusive(min, max int) int {
	if min >= max {
		return min
	}
	return IntnRange(min, max+1)
}

// Float64 返回 [0.0, 1.0) 范围内的伪随机 float64。
// 并发安全。
func Float64() float64 {
	fastMu.Lock()
	v := fastRand.Float64()
	fastMu.Unlock()
	return v
}

// JitterInt 对整数 base 添加比例抖动，返回值范围 base ± ratioPct%。
//   - base <= 0：直接返回 0（通常表示无 TTL / 立即执行）。
//   - ratioPct <= 0：直接返回 base（不抖动）。
//   - ratioPct > 100：自动截断为 100，避免抖动范围超出 ±base。
//   - 计算得到的 delta <= 0 时强制 delta = 1，保证至少 ±1 的抖动（base 足够大时）。
//
// 典型用法：
//
//	JitterInt(300, 10)   // 返回 270~330 之间的整数，用于 TTL 防雪崩。
//
// 并发安全。
func JitterInt(base int, ratioPct int) int {
	if base <= 0 {
		return 0
	}
	if ratioPct <= 0 {
		return base
	}
	if ratioPct > 100 {
		ratioPct = 100
	}
	delta := base * ratioPct / 100
	if delta <= 0 {
		delta = 1
	}
	return base + IntnRange(-delta, delta+1)
}

// JitterDuration 对 time.Duration 添加比例抖动，返回范围 base ± ratioPct%。
// 语义与 JitterInt 一致（ratioPct 被截断至 100，delta 至少 1ns）。
// 典型用法：
//
//	JitterDuration(5*time.Minute, 10)  // 用于重试退避、定时任务打散。
//
// 并发安全。
func JitterDuration(base time.Duration, ratioPct int) time.Duration {
	if base <= 0 {
		return 0
	}
	if ratioPct <= 0 {
		return base
	}
	if ratioPct > 100 {
		ratioPct = 100
	}
	delta := int64(base) * int64(ratioPct) / 100
	if delta <= 0 {
		delta = 1
	}
	jitter := Int63n(2*delta+1) - delta
	return base + time.Duration(jitter)
}

// String 返回长度为 n 的伪随机字符串，每个字符从 chars 中均匀抽取。
// n <= 0 或 chars 为空时返回空字符串；并发安全。
// 若需要密码学安全字符串请使用 SecureString。
func String(n int, chars string) string {
	if n <= 0 || len(chars) == 0 {
		return ""
	}
	alphabetLen := len(chars)
	b := make([]byte, n)
	fastMu.Lock()
	for i := range b {
		b[i] = chars[fastRand.Intn(alphabetLen)]
	}
	fastMu.Unlock()
	return string(b)
}

// AlphaString 返回长度为 n 的伪随机「大小写字母」字符串。
// 等价于 String(n, CharsAlpha)；并发安全。
func AlphaString(n int) string {
	return String(n, CharsAlpha)
}

// AlphaNumString 返回长度为 n 的伪随机「字母 + 数字」字符串。
// 等价于 String(n, CharsAlphaNum)；并发安全。
func AlphaNumString(n int) string {
	return String(n, CharsAlphaNum)
}

// HexString 返回长度为 n 的伪随机十六进制字符串（小写）。
// 内部读取 ceil(n/2) 字节再 hex.Encode 后截断前 n 位；并发安全。
// 若需要密码学安全十六进制请使用 SecureHex。
func HexString(n int) string {
	if n <= 0 {
		return ""
	}
	raw := (n + 1) / 2
	b := make([]byte, raw)
	fastMu.Lock()
	for i := range b {
		b[i] = byte(fastRand.Intn(0x100))
	}
	fastMu.Unlock()
	return hex.EncodeToString(b)[:n]
}

// Bool 返回伪随机布尔值（true / false 概率约各 50%）。
// 并发安全。
func Bool() bool {
	return Intn(2) == 1
}

// Duration 返回 [min, max) 范围内的伪随机 time.Duration。
// min >= max 时直接返回 min；并发安全。
func Duration(min, max time.Duration) time.Duration {
	if min >= max {
		return min
	}
	return min + time.Duration(Int63n(int64(max-min)))
}

// Perm 返回 0..n-1 的伪随机排列（洗牌后的切片）。
// n <= 0 时返回 nil；并发安全。
func Perm(n int) []int {
	if n <= 0 {
		return nil
	}
	fastMu.Lock()
	v := fastRand.Perm(n)
	fastMu.Unlock()
	return v
}

// Shuffle 使用伪随机数原地打乱长度为 n 的序列，swap 为用户提供的交换函数。
// n <= 1 或 swap == nil 时直接返回，不做任何操作；并发安全。
func Shuffle(n int, swap func(i, j int)) {
	if n <= 1 || swap == nil {
		return
	}
	fastMu.Lock()
	fastRand.Shuffle(n, swap)
	fastMu.Unlock()
}

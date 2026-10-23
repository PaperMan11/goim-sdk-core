// alias.go —— time 包 re-export 层（类型别名 + 常量 + 少量原生函数）
//
// 项目约定：除 timex 自身外，所有代码禁止直接 import "time"，
// 统一通过 timex 使用时间能力（now.go 的时钟可注入，便于单测）。
//
// 类型全部使用 Go 类型别名（type X = time.X），与 time 原生类型完全等价，
// 函数签名、接口实现、第三方库（gorilla/websocket 等）调用均无需任何适配。
//
// 注意：Unix / UTC / Since / Until / Parse 五个名字已被 timex 占用（语义见 now.go /
// const.go / compare.go / format.go），对应映射：
//
//	time.Now().Unix() → timex.Unix()          （可注入时钟）
//	time.UTC          → timex.UTCLoc
//	time.Since(t)     → timex.Since(t)        （可注入时钟）
//	time.Until(t)     → timex.Until(t)        （可注入时钟）
//	time.Unix(s, 0)   → timex.ParseUnix(s)
//	time.Unix(0, ns)  → timex.ParseUnixNano(ns)
//	time.Parse(...)   → timex.Parse(...)
package timex

import "time"

// === 类型别名 ===

type (
	// Time 等价 time.Time
	Time = time.Time
	// Duration 等价 time.Duration
	Duration = time.Duration
	// Location 等价 time.Location
	Location = time.Location
	// Month 等价 time.Month
	Month = time.Month
	// Weekday 等价 time.Weekday
	Weekday = time.Weekday
	// Timer 等价 time.Timer
	Timer = time.Timer
	// Ticker 等价 time.Ticker
	Ticker = time.Ticker
)

// === 时长常量 ===

const (
	Nanosecond  = time.Nanosecond
	Microsecond = time.Microsecond
	Millisecond = time.Millisecond
	Second      = time.Second
	Minute      = time.Minute
	Hour        = time.Hour
)

// UTCLoc time.UTC 的 re-export（UTC 名字已被时区名字符串常量占用，见 const.go）
var UTCLoc = time.UTC

// === 标准库 Layout（cast/convert 等格式化场景使用） ===

const (
	RFC3339     = time.RFC3339
	RFC3339Nano = time.RFC3339Nano
	RFC1123     = time.RFC1123
	RFC1123Z    = time.RFC1123Z
	RFC822      = time.RFC822
	RFC822Z     = time.RFC822Z
	RFC850      = time.RFC850
	ANSIC       = time.ANSIC
	UnixDate    = time.UnixDate
	RubyDate    = time.RubyDate
	Kitchen     = time.Kitchen
	Stamp       = time.Stamp
	StampMilli  = time.StampMilli
	StampMicro  = time.StampMicro
	StampNano   = time.StampNano
)

// === 原生函数 re-export ===

// After 等价 time.After（select 超时分支可直接替换）
func After(d Duration) <-chan Time { return time.After(d) }

// Date 等价 time.Date
func Date(year int, month Month, day, hour, min, sec, nsec int, loc *Location) Time {
	return time.Date(year, month, day, hour, min, sec, nsec, loc)
}

// ParseDuration 等价 time.ParseDuration
func ParseDuration(s string) (Duration, error) { return time.ParseDuration(s) }

package timex

import "time"

func Format(layout string) string {
	return timeNow().Format(layout)
}

func FormatIn(layout string, loc *time.Location) string {
	return timeNow().In(loc).Format(layout)
}

func Parse(layout, value string) (time.Time, error) {
	return time.Parse(layout, value)
}

func ParseIn(layout, value string, loc *time.Location) (time.Time, error) {
	return time.ParseInLocation(layout, value, loc)
}

func ParseUnix(seconds int64) time.Time {
	return time.Unix(seconds, 0)
}

func ParseUnixMilli(milliseconds int64) time.Time {
	return time.UnixMilli(milliseconds)
}

func ParseUnixMicro(microseconds int64) time.Time {
	return time.UnixMicro(microseconds)
}

func ParseUnixNano(nanoseconds int64) time.Time {
	return time.Unix(0, nanoseconds)
}
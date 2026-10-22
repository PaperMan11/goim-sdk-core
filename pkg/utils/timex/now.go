package timex

import "time"

var timeNow = time.Now

func SetTimeNow(now time.Time) {
	timeNow = func() time.Time {
		return now
	}
}

func ResetTimeNow() {
	timeNow = time.Now
}

func Now() time.Time {
	return timeNow()
}

func Unix() int64 {
	return timeNow().Unix()
}

func UnixMilli() int64 {
	return timeNow().UnixMilli()
}

func UnixNano() int64 {
	return timeNow().UnixNano()
}

func UnixMicro() int64 {
	return timeNow().UnixMicro()
}
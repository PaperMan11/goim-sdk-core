package timex

import "time"

func Sleep(d time.Duration) {
	time.Sleep(d)
}

func NewTimer(d time.Duration) *time.Timer {
	return time.NewTimer(d)
}

func NewTicker(d time.Duration) *time.Ticker {
	return time.NewTicker(d)
}

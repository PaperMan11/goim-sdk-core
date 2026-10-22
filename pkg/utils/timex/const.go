package timex

import "time"

const (
	UTC   = "UTC"
	UTC8P = "UTC+8"
	UTC3P = "UTC+3"
	UTC3M = "UTC-3"
	UTC5M = "UTC-5"
)

const (
	LayoutDate          = "2006-01-02"
	LayoutTime          = "15:04:05"
	LayoutDateTime      = "2006-01-02 15:04:05"
	LayoutDateTimeMilli = "2006-01-02 15:04:05.000"
	LayoutDateTimeMicro = "2006-01-02 15:04:05.000000"
	LayoutDateTimeNano  = "2006-01-02 15:04:05.000000000"
	LayoutCompactDate   = "20060102"
	LayoutCompactTime   = "150405"
	LayoutCompact       = "20060102150405"
	LayoutRFC3339       = time.RFC3339
	LayoutRFC3339Nano   = time.RFC3339Nano
)
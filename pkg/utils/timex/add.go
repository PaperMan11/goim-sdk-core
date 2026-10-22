package timex

import "time"

func AddDays(t time.Time, days int) time.Time {
	return t.AddDate(0, 0, days)
}

func AddHours(t time.Time, hours int) time.Time {
	return t.Add(time.Duration(hours) * time.Hour)
}

func AddMinutes(t time.Time, minutes int) time.Time {
	return t.Add(time.Duration(minutes) * time.Minute)
}

func AddSeconds(t time.Time, seconds int) time.Time {
	return t.Add(time.Duration(seconds) * time.Second)
}

func AddMilliseconds(t time.Time, milliseconds int64) time.Time {
	return t.Add(time.Duration(milliseconds) * time.Millisecond)
}
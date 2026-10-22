package timex

import "time"

func IsSameDay(t1, t2 time.Time) bool {
	y1, m1, d1 := t1.Date()
	y2, m2, d2 := t2.Date()
	return y1 == y2 && m1 == m2 && d1 == d2
}

func IsSameWeek(t1, t2 time.Time) bool {
	return BeginOfWeek(t1).Equal(BeginOfWeek(t2))
}

func IsSameMonth(t1, t2 time.Time) bool {
	return t1.Year() == t2.Year() && t1.Month() == t2.Month()
}

func IsSameYear(t1, t2 time.Time) bool {
	return t1.Year() == t2.Year()
}

func DaysBetween(start, end time.Time) int {
	start = BeginOfDay(start)
	end = BeginOfDay(end)
	diff := end.Sub(start)
	return int(diff.Hours() / 24)
}

func HoursBetween(start, end time.Time) int {
	diff := end.Sub(start)
	return int(diff.Hours())
}

func MinutesBetween(start, end time.Time) int {
	diff := end.Sub(start)
	return int(diff.Minutes())
}

func SecondsBetween(start, end time.Time) int {
	diff := end.Sub(start)
	return int(diff.Seconds())
}

func IsZero(t time.Time) bool {
	return t.IsZero()
}

func IsFuture(t time.Time) bool {
	return t.After(timeNow())
}

func IsPast(t time.Time) bool {
	return t.Before(timeNow())
}

func IsExpired(t time.Time) bool {
	return !t.IsZero() && t.Before(timeNow())
}

func Until(t time.Time) time.Duration {
	return t.Sub(timeNow())
}

func Since(t time.Time) time.Duration {
	return timeNow().Sub(t)
}

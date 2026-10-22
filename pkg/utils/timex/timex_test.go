package timex

import (
	"testing"
	"time"
)

func TestNow(t *testing.T) {
	now := Now()
	if now.IsZero() {
		t.Error("Expected non-zero time")
	}
}

func TestSetTimeNow(t *testing.T) {
	fixedTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	SetTimeNow(fixedTime)
	defer ResetTimeNow()

	if Now() != fixedTime {
		t.Errorf("Expected %v, got %v", fixedTime, Now())
	}

	if Unix() != fixedTime.Unix() {
		t.Errorf("Expected Unix %d, got %d", fixedTime.Unix(), Unix())
	}
}

func TestUnix(t *testing.T) {
	expected := time.Now().Unix()
	actual := Unix()
	if actual < expected-1 || actual > expected+1 {
		t.Errorf("Expected Unix %d, got %d", expected, actual)
	}
}

func TestUnixMilli(t *testing.T) {
	expected := time.Now().UnixMilli()
	actual := UnixMilli()
	if actual < expected-1 || actual > expected+1 {
		t.Errorf("Expected UnixMilli %d, got %d", expected, actual)
	}
}

func TestUnixNano(t *testing.T) {
	expected := time.Now().UnixNano()
	actual := UnixNano()
	if actual < expected-1000 || actual > expected+1000 {
		t.Errorf("Expected UnixNano %d, got %d", expected, actual)
	}
}

func TestUnixMicro(t *testing.T) {
	expected := time.Now().UnixMicro()
	actual := UnixMicro()
	if actual < expected-1000 || actual > expected+1000 {
		t.Errorf("Expected UnixMicro %d, got %d", expected, actual)
	}
}

func TestNowIn(t *testing.T) {
	tz := "Asia/Shanghai"
	now, err := NowIn(tz)
	if err != nil {
		t.Fatalf("NowIn failed: %v", err)
	}
	if now.IsZero() {
		t.Error("Expected non-zero time")
	}
	if now.Location().String() != tz {
		t.Errorf("Expected timezone %s, got %s", tz, now.Location())
	}
}

func TestFixedZone(t *testing.T) {
	testCases := []struct {
		name     string
		zone     string
		expected int
	}{
		{"UTC", UTC, 0},
		{"UTC+8", UTC8P, 8 * 3600},
		{"UTC+3", UTC3P, 3 * 3600},
		{"UTC-3", UTC3M, -3 * 3600},
		{"UTC-5", UTC5M, -5 * 3600},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			loc := FixedZone(tc.zone)
			_, offset := time.Now().In(loc).Zone()
			if offset != tc.expected {
				t.Errorf("Expected offset %d, got %d", tc.expected, offset)
			}
		})
	}
}

func TestFixedZoneIANA(t *testing.T) {
	loc := FixedZone("UTC")
	if loc != time.UTC {
		t.Error("Expected UTC location")
	}
}

func TestFormat(t *testing.T) {
	SetTimeNow(time.Date(2024, 6, 15, 10, 30, 45, 0, time.UTC))
	defer ResetTimeNow()

	result := Format(LayoutDateTime)
	expected := "2024-06-15 10:30:45"
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func TestParseUnix(t *testing.T) {
	seconds := int64(1623758400)
	tm := ParseUnix(seconds)
	if tm.Unix() != seconds {
		t.Errorf("Expected Unix %d, got %d", seconds, tm.Unix())
	}
}

func TestParseUnixMilli(t *testing.T) {
	milli := int64(1623758400000)
	tm := ParseUnixMilli(milli)
	if tm.UnixMilli() != milli {
		t.Errorf("Expected UnixMilli %d, got %d", milli, tm.UnixMilli())
	}
}

func TestBeginOfDay(t *testing.T) {
	tm := time.Date(2024, 6, 15, 10, 30, 45, 123456789, time.UTC)
	result := BeginOfDay(tm)
	expected := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	if !result.Equal(expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestEndOfDay(t *testing.T) {
	tm := time.Date(2024, 6, 15, 10, 30, 45, 123456789, time.UTC)
	result := EndOfDay(tm)
	expected := time.Date(2024, 6, 15, 23, 59, 59, 999999999, time.UTC)
	if !result.Equal(expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestBeginOfMonth(t *testing.T) {
	tm := time.Date(2024, 6, 15, 10, 30, 45, 123456789, time.UTC)
	result := BeginOfMonth(tm)
	expected := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	if !result.Equal(expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestEndOfMonth(t *testing.T) {
	tm := time.Date(2024, 2, 15, 10, 30, 45, 123456789, time.UTC)
	result := EndOfMonth(tm)
	expected := time.Date(2024, 2, 29, 23, 59, 59, 999999999, time.UTC)
	if !result.Equal(expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestBeginOfYear(t *testing.T) {
	tm := time.Date(2024, 6, 15, 10, 30, 45, 123456789, time.UTC)
	result := BeginOfYear(tm)
	expected := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if !result.Equal(expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestEndOfYear(t *testing.T) {
	tm := time.Date(2024, 6, 15, 10, 30, 45, 123456789, time.UTC)
	result := EndOfYear(tm)
	expected := time.Date(2024, 12, 31, 23, 59, 59, 999999999, time.UTC)
	if !result.Equal(expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestIsSameDay(t *testing.T) {
	t1 := time.Date(2024, 6, 15, 10, 30, 45, 0, time.UTC)
	t2 := time.Date(2024, 6, 15, 23, 59, 59, 999999999, time.UTC)
	t3 := time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC)

	if !IsSameDay(t1, t2) {
		t.Error("Expected same day")
	}
	if IsSameDay(t1, t3) {
		t.Error("Expected different days")
	}
}

func TestIsSameMonth(t *testing.T) {
	t1 := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 6, 30, 23, 59, 59, 999999999, time.UTC)
	t3 := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)

	if !IsSameMonth(t1, t2) {
		t.Error("Expected same month")
	}
	if IsSameMonth(t1, t3) {
		t.Error("Expected different months")
	}
}

func TestDaysBetween(t *testing.T) {
	start := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 5, 0, 0, 0, 0, time.UTC)
	result := DaysBetween(start, end)
	if result != 4 {
		t.Errorf("Expected 4 days, got %d", result)
	}
}

func TestAddDays(t *testing.T) {
	tm := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	result := AddDays(tm, 5)
	expected := time.Date(2024, 6, 6, 0, 0, 0, 0, time.UTC)
	if !result.Equal(expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestIsZero(t *testing.T) {
	if !IsZero(time.Time{}) {
		t.Error("Expected zero time")
	}
	if IsZero(time.Now()) {
		t.Error("Expected non-zero time")
	}
}

func TestIsFuture(t *testing.T) {
	future := time.Now().Add(time.Hour)
	if !IsFuture(future) {
		t.Error("Expected future time")
	}

	past := time.Now().Add(-time.Hour)
	if IsFuture(past) {
		t.Error("Expected past time")
	}
}

func TestIsPast(t *testing.T) {
	past := time.Now().Add(-time.Hour)
	if !IsPast(past) {
		t.Error("Expected past time")
	}

	future := time.Now().Add(time.Hour)
	if IsPast(future) {
		t.Error("Expected future time")
	}
}

func TestIsExpired(t *testing.T) {
	past := time.Now().Add(-time.Hour)
	if !IsExpired(past) {
		t.Error("Expected expired time")
	}

	future := time.Now().Add(time.Hour)
	if IsExpired(future) {
		t.Error("Expected not expired time")
	}

	if IsExpired(time.Time{}) {
		t.Error("Expected zero time to not be expired")
	}
}

func TestSince(t *testing.T) {
	start := time.Now()
	time.Sleep(10 * time.Millisecond)
	dur := Since(start)
	if dur < 10*time.Millisecond {
		t.Errorf("Expected duration >= 10ms, got %v", dur)
	}
}

func TestUntil(t *testing.T) {
	future := time.Now().Add(10 * time.Millisecond)
	dur := Until(future)
	if dur < 5*time.Millisecond {
		t.Errorf("Expected duration >= 5ms, got %v", dur)
	}
}

func TestParse(t *testing.T) {
	tm, err := Parse(LayoutDateTime, "2024-06-15 10:30:45")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	expected := time.Date(2024, 6, 15, 10, 30, 45, 0, time.UTC)
	if !tm.Equal(expected) {
		t.Errorf("Expected %v, got %v", expected, tm)
	}
}

func TestBeginOfWeek(t *testing.T) {
	tm := time.Date(2024, 6, 15, 10, 30, 45, 0, time.UTC)
	result := BeginOfWeek(tm)
	expected := time.Date(2024, 6, 10, 0, 0, 0, 0, time.UTC)
	if !result.Equal(expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestEndOfWeek(t *testing.T) {
	tm := time.Date(2024, 6, 15, 10, 30, 45, 0, time.UTC)
	result := EndOfWeek(tm)
	expected := time.Date(2024, 6, 16, 23, 59, 59, 999999999, time.UTC)
	if !result.Equal(expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

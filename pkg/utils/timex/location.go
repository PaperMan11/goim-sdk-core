package timex

import "time"

func NowIn(tzName string) (time.Time, error) {
	loc, err := time.LoadLocation(tzName)
	if err != nil {
		return time.Time{}, err
	}
	return timeNow().In(loc), nil
}

func NowInLocation(loc *time.Location) time.Time {
	return timeNow().In(loc)
}

func FixedZone(zone string) *time.Location {
	switch zone {
	case UTC:
		return time.UTC
	case UTC8P:
		return time.FixedZone(UTC8P, 8*3600)
	case UTC3P:
		return time.FixedZone(UTC3P, 3*3600)
	case UTC3M:
		return time.FixedZone(UTC3M, -3*3600)
	case UTC5M:
		return time.FixedZone(UTC5M, -5*3600)
	default:
		loc, err := time.LoadLocation(zone)
		if err == nil {
			return loc
		}
		return time.UTC
	}
}

func ToUTC(t time.Time) time.Time {
	return t.UTC()
}

func ToLocal(t time.Time) time.Time {
	return t.Local()
}

func ToLocation(t time.Time, loc *time.Location) time.Time {
	return t.In(loc)
}
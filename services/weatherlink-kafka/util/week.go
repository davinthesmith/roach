package util

import "time"

// StartOfWeekUnix returns the Unix timestamp (seconds) for the start of the week (Monday 00:00:00) in the given time's location.
func StartOfWeekUnix(t time.Time) int64 {
	weekday := int(t.Weekday())
	daysSinceMonday := (weekday + 6) % 7
	start := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).AddDate(0, 0, -daysSinceMonday)
	return start.Unix()
}

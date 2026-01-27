package util

import (
	"strconv"
	"time"

	"weatherlink-kafka/models"
)

// StartOfWeekUnix returns the Unix timestamp (seconds) for the start of the week (Monday 00:00:00) in the given time's location.
func StartOfWeekUnix(t time.Time) int64 {
	weekday := int(t.Weekday())
	daysSinceMonday := (weekday + 6) % 7
	start := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).AddDate(0, 0, -daysSinceMonday)
	return start.Unix()
}

// ParseTimestamp parses a timestamp string that can be either:
// - Unix timestamp (e.g., "1768780863")
// - Datetime string (e.g., "2026-01-11 18:20:47")
func ParseTimestamp(s string) (int64, error) {
	// Try parsing as Unix timestamp first
	if ts, err := strconv.ParseInt(s, 10, 64); err == nil {
		return ts, nil
	}

	// Try parsing as datetime string
	layouts := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04",
		"2006-01-02",
	}

	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.Unix(), nil
		}
	}

	return 0, nil
}

// splitInto24HourWindows splits a time range into 24-hour windows
func SplitInto24HourWindows(startTs, endTs int64) []models.TimeWindow {
	const windowSize = 86400 // 24 hours in seconds

	windows := []models.TimeWindow{}
	currentStart := startTs

	for currentStart < endTs {
		currentEnd := currentStart + windowSize
		if currentEnd > endTs {
			currentEnd = endTs
		}

		windows = append(windows, models.TimeWindow{
			Start: currentStart,
			End:   currentEnd,
		})

		currentStart = currentEnd
	}

	return windows
}

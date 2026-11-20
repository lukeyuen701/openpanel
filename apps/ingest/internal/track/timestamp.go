package track

import (
	"math"
	"regexp"
	"strconv"
	"time"

	"openpanel/ingest/internal/jsjson"
)

const (
	oneMinuteMs      = 60 * 1000
	fifteenMinutesMs = 15 * oneMinuteMs
	// maxTimeValue is ECMAScript's TimeClip bound.
	maxTimeValue = 8.64e15
)

// earliestAcceptedMs is 2000-01-01: a smaller value cannot be a real event
// time (epoch seconds read as milliseconds land in 1970).
var earliestAcceptedMs = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()

// Timestamp is the event time and whether it is back-dated past 15 minutes.
type Timestamp struct {
	Ms       int64
	FromPast bool
}

// ResolveTimestamp is getTimestamp(arrival, userDefined). ok is false when the
// value is a string this port does not parse the way JavaScriptCore would
// (local-time forms, RFC 2822, …): the caller hands such a request to the Bun
// API rather than guess.
func ResolveTimestamp(arrivalMs int64, userDefined *jsjson.Value) (Timestamp, bool) {
	fallback := Timestamp{Ms: arrivalMs}
	if userDefined == nil {
		return fallback, true
	}
	var clientMs float64
	switch userDefined.Kind {
	case jsjson.Number:
		if userDefined.Num == 0 {
			return fallback, true
		}
		if math.Abs(userDefined.Num) > maxTimeValue {
			return fallback, true
		}
		clientMs = math.Trunc(userDefined.Num)
	case jsjson.String:
		if userDefined.Str == "" {
			return fallback, true
		}
		parsed, ok := parseISODate(userDefined.Str)
		if !ok {
			return Timestamp{}, false
		}
		clientMs = float64(parsed)
	default:
		return fallback, true
	}
	if clientMs < float64(earliestAcceptedMs) || clientMs > float64(arrivalMs+oneMinuteMs) {
		return fallback, true
	}
	ms := int64(clientMs)
	return Timestamp{Ms: ms, FromPast: ms < arrivalMs-fifteenMinutesMs}, true
}

// isoDate is the slice of the ECMAScript date-time format whose meaning does
// not depend on the server's time zone: a date alone (UTC), or a date and
// time with an explicit Z or ±HH:MM offset. JavaScriptCore also takes the
// designators in lower case and any number of fraction digits.
var isoDate = regexp.MustCompile(`^(\d{4})(?:-(\d{2})(?:-(\d{2}))?)?$|^(\d{4})-(\d{2})-(\d{2})[Tt](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?(?:([Zz])|([+-])(\d{2}):(\d{2}))$`)

func parseISODate(s string) (int64, bool) {
	m := isoDate.FindStringSubmatch(s)
	if m == nil {
		return 0, false
	}
	atoi := func(v string, fallback int) int {
		if v == "" {
			return fallback
		}
		n, _ := strconv.Atoi(v)
		return n
	}
	if m[1] != "" {
		year, month, day := atoi(m[1], 0), atoi(m[2], 1), atoi(m[3], 1)
		if !validDate(year, month, day) {
			return 0, false
		}
		return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC).UnixMilli(), true
	}
	year, month, day := atoi(m[4], 0), atoi(m[5], 0), atoi(m[6], 0)
	hour, minute, second := atoi(m[7], 0), atoi(m[8], 0), atoi(m[9], 0)
	if !validDate(year, month, day) || hour > 23 || minute > 59 || second > 59 {
		return 0, false
	}
	millis := 0
	if fraction := m[10]; fraction != "" {
		for len(fraction) < 3 {
			fraction += "0"
		}
		millis, _ = strconv.Atoi(fraction[:3])
	}
	t := time.Date(year, time.Month(month), day, hour, minute, second, millis*int(time.Millisecond), time.UTC)
	if m[11] == "" {
		offsetHours, offsetMinutes := atoi(m[13], 0), atoi(m[14], 0)
		if offsetHours > 23 || offsetMinutes > 59 {
			return 0, false
		}
		offset := time.Duration(offsetHours)*time.Hour + time.Duration(offsetMinutes)*time.Minute
		if m[12] == "+" {
			t = t.Add(-offset)
		} else {
			t = t.Add(offset)
		}
	}
	return t.UnixMilli(), true
}

// validDate rejects days a month does not have: JavaScriptCore rolls
// 2026-02-30 over into March, which this port does not reproduce.
func validDate(year, month, day int) bool {
	if month < 1 || month > 12 || day < 1 {
		return false
	}
	return day <= time.Date(year, time.Month(month)+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

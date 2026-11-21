package jsport

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"openpanel/ingest/internal/jsjson"
)

const (
	msPerSecond = 1000
	msPerMinute = 60 * msPerSecond
	msPerHour   = 60 * msPerMinute
	msPerDay    = 24 * msPerHour
	// maxTimeMs is the largest magnitude a Date holds (TimeClip).
	maxTimeMs = 8.64e15

	invalidDateTime = "Invalid DateTime"
	// ClickhouseDateTimeFormat is the Luxon format the event row's created_at uses.
	ClickhouseDateTimeFormat = "yyyy-MM-dd HH:mm:ss.SSS"
)

var (
	// ErrInvalidDate is the RangeError toISOString throws for an invalid date.
	ErrInvalidDate = errors.New("jsport: invalid time value")
	// ErrUnsupportedDate marks a date string outside the ISO 8601 forms this
	// package reads; JavaScriptCore's legacy parser accepts many more (month
	// names, slashes, RFC 2822) and is not reproduced.
	ErrUnsupportedDate = errors.New("jsport: date string outside the supported forms")
)

// Clock is the wall clock; the worker passes a fake one in tests.
type Clock interface{ Now() time.Time }

// SystemClock reads the real time.
type SystemClock struct{}

// Now implements Clock.
func (SystemClock) Now() time.Time { return time.Now() }

// Date is a JavaScript Date: milliseconds since the epoch, or invalid.
type Date struct {
	ms    int64
	valid bool
}

// DateFromMillis is new Date(ms) for a number: NaN, infinities and values
// beyond +-8.64e15 are invalid, the rest truncate toward zero.
func DateFromMillis(ms float64) Date {
	if math.IsNaN(ms) || math.Abs(ms) > maxTimeMs {
		return Date{}
	}
	return Date{ms: int64(math.Trunc(ms)), valid: true}
}

// DateFromTime is new Date(t.getTime()) for a time.Time.
func DateFromTime(t time.Time) Date { return Date{ms: t.UnixMilli(), valid: true} }

// ParseDate is new Date(v) for the timestamp forms an event carries: a number
// or a string. A string outside the ISO 8601 forms yields ErrUnsupportedDate;
// the result for any other kind of value is ErrUnsupportedDate too.
func ParseDate(v *jsjson.Value) (Date, error) {
	if v == nil {
		return Date{}, ErrUnsupportedDate
	}
	switch v.Kind {
	case jsjson.Number:
		return DateFromMillis(v.Num), nil
	case jsjson.String:
		return ParseDateString(v.Str)
	}
	return Date{}, ErrUnsupportedDate
}

// Valid reports whether the date is not Invalid Date.
func (d Date) Valid() bool { return d.valid }

// Millis is getTime(); ok is false for an invalid date (NaN).
func (d Date) Millis() (ms int64, ok bool) { return d.ms, d.valid }

// ISOString is toISOString(): always milliseconds and Z, years outside
// 0000-9999 as +-YYYYYY, and ErrInvalidDate (a RangeError) for an invalid date.
func (d Date) ISOString() (string, error) {
	if !d.valid {
		return "", ErrInvalidDate
	}
	c := civilFromMillis(d.ms)
	buf := make([]byte, 0, 32)
	switch {
	case c.year >= 0 && c.year <= 9999:
		buf = appendPadded(buf, int64(c.year), 4)
	case c.year < 0:
		buf = append(buf, '-')
		buf = appendPadded(buf, -int64(c.year), 6)
	default:
		buf = append(buf, '+')
		buf = appendPadded(buf, int64(c.year), 6)
	}
	buf = append(buf, '-')
	buf = appendPadded(buf, int64(c.month), 2)
	buf = append(buf, '-')
	buf = appendPadded(buf, int64(c.day), 2)
	buf = append(buf, 'T')
	buf = appendClock(buf, c)
	buf = append(buf, '.')
	buf = appendPadded(buf, int64(c.millisecond), 3)
	return string(append(buf, 'Z')), nil
}

// JSON is JSON.stringify(date): the quoted ISO string, or null when invalid.
func (d Date) JSON() string {
	iso, err := d.ISOString()
	if err != nil {
		return "null"
	}
	return `"` + iso + `"`
}

// LuxonFormat is DateTime.fromJSDate(date).setZone('UTC').toFormat('yyyy-MM-dd HH:mm:ss.SSS');
// an invalid date gives Luxon's "Invalid DateTime".
func (d Date) LuxonFormat() string {
	if !d.valid {
		return invalidDateTime
	}
	c := civilFromMillis(d.ms)
	buf := make([]byte, 0, 32)
	if c.year < 0 {
		buf = append(buf, '-')
		buf = appendPadded(buf, -int64(c.year), 4)
	} else {
		buf = appendPadded(buf, int64(c.year), 4)
	}
	buf = append(buf, '-')
	buf = appendPadded(buf, int64(c.month), 2)
	buf = append(buf, '-')
	buf = appendPadded(buf, int64(c.day), 2)
	buf = append(buf, ' ')
	buf = appendClock(buf, c)
	buf = append(buf, '.')
	buf = appendPadded(buf, int64(c.millisecond), 3)
	return string(buf)
}

// ClickhouseDate is formatClickhouseDate(date): toISOString with the T
// replaced by a space and the milliseconds and Z removed.
func (d Date) ClickhouseDate() (string, error) {
	iso, err := d.ISOString()
	if err != nil {
		return "", err
	}
	const millisecondsAndZoneLength = len(".000Z")
	withoutZone := iso[:len(iso)-millisecondsAndZoneLength]
	dateEnd := strings.IndexByte(withoutZone, 'T')
	return withoutZone[:dateEnd] + " " + withoutZone[dateEnd+1:], nil
}

// FormatNow is DateTime.utc().toFormat('yyyy-MM-dd HH:mm:ss.SSS') at the clock's time.
func FormatNow(clock Clock) string { return DateFromTime(clock.Now()).LuxonFormat() }

func appendClock(buf []byte, c civil) []byte {
	buf = appendPadded(buf, int64(c.hour), 2)
	buf = append(buf, ':')
	buf = appendPadded(buf, int64(c.minute), 2)
	buf = append(buf, ':')
	return appendPadded(buf, int64(c.second), 2)
}

func appendPadded(buf []byte, n int64, width int) []byte {
	digits := strconv.FormatInt(n, 10)
	for i := len(digits); i < width; i++ {
		buf = append(buf, '0')
	}
	return append(buf, digits...)
}

type civil struct {
	year, month, day, hour, minute, second, millisecond int
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

// civilFromMillis converts epoch milliseconds to a proleptic Gregorian date
// (days-from-civil inverse, valid for the whole Date range).
func civilFromMillis(ms int64) civil {
	days := floorDiv(ms, msPerDay)
	rest := ms - days*msPerDay
	z := days + 719468
	era := floorDiv(z, 146097)
	dayOfEra := z - era*146097
	yearOfEra := (dayOfEra - dayOfEra/1460 + dayOfEra/36524 - dayOfEra/146096) / 365
	year := yearOfEra + era*400
	dayOfYear := dayOfEra - (365*yearOfEra + yearOfEra/4 - yearOfEra/100)
	mp := (5*dayOfYear + 2) / 153
	day := dayOfYear - (153*mp+2)/5 + 1
	month := mp + 3
	if mp >= 10 {
		month = mp - 9
	}
	if month <= 2 {
		year++
	}
	return civil{
		year: int(year), month: int(month), day: int(day),
		hour: int(rest / msPerHour), minute: int(rest % msPerHour / msPerMinute),
		second: int(rest % msPerMinute / msPerSecond), millisecond: int(rest % msPerSecond),
	}
}

// daysFromCivil is the inverse; month 1-12, day may exceed the month's length
// (it rolls into the next month, as JavaScriptCore does for "2024-02-30").
func daysFromCivil(year int64, month, day int64) int64 {
	if month <= 2 {
		year--
	}
	era := floorDiv(year, 400)
	yearOfEra := year - era*400
	shifted := month - 3
	if month <= 2 {
		shifted = month + 9
	}
	dayOfYear := (153*shifted+2)/5 + day - 1
	dayOfEra := yearOfEra*365 + yearOfEra/4 - yearOfEra/100 + dayOfYear
	return era*146097 + dayOfEra - 719468
}

const (
	clickhouseDateTimeLength = len("2024-01-01 00:00:00.000")
	february                 = 2
	monthsPerYear            = 12
)

// ParseClickhouseDateTime is DateTime.fromFormat(s, 'yyyy-MM-dd HH:mm:ss.SSS',
// {zone: 'UTC'}).toJSDate(): exactly that shape, a real calendar day, and
// hour 24 only as 24:00:00.000; anything else is an Invalid Date.
func ParseClickhouseDateTime(s string) Date {
	if len(s) != clickhouseDateTimeLength || s[4] != '-' || s[7] != '-' || s[10] != ' ' || s[13] != ':' || s[16] != ':' || s[19] != '.' {
		return Date{}
	}
	p := dateScanner{s: s}
	year, okYear := p.digits(4)
	p.pos++
	month, okMonth := p.digits(2)
	p.pos++
	day, okDay := p.digits(2)
	p.pos++
	hour, okHour := p.digits(2)
	p.pos++
	minute, okMinute := p.digits(2)
	p.pos++
	second, okSecond := p.digits(2)
	p.pos++
	millisecond, okMillisecond := p.digits(3)
	if !(okYear && okMonth && okDay && okHour && okMinute && okSecond && okMillisecond) {
		return Date{}
	}
	endOfDay := hour == hoursEndOfDay && minute == 0 && second == 0 && millisecond == 0
	if month < 1 || month > monthsPerYear || day < 1 || day > daysInMonth(year, month) ||
		(hour >= hoursEndOfDay && !endOfDay) || minute > maxMinuteOrSecond || second > maxMinuteOrSecond {
		return Date{}
	}
	ms := daysFromCivil(year, month, day)*msPerDay + hour*msPerHour + minute*msPerMinute + second*msPerSecond + millisecond
	return DateFromMillis(float64(ms))
}

func daysInMonth(year, month int64) int64 {
	if month == february && (year%4 == 0 && (year%100 != 0 || year%400 == 0)) {
		return 29
	}
	return [...]int64{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}[month-1]
}

package jsport

const (
	isoYearDigits           = 4
	isoExpandedDigits       = 6
	maxMonth                = 12
	maxDay                  = 31
	maxHour                 = 24
	maxMinuteOrSecond       = 59
	maxOffsetHours          = 23
	msFractionDigits        = 3
	pow10ForMsFraction      = 100
	hoursEndOfDay           = 24
	legacyTwoDigitYearLimit = 100
	offsetColonlessSize     = 4
)

// ParseDateString is new Date(s) for the ISO 8601 forms JavaScript reads:
// YYYY, YYYY-MM, YYYY-MM-DD, +-YYYYYY, optionally followed by T (or t, or a
// space) HH:mm[:ss[.fff...]] and Z, +-HH:mm or +-HHmm. Without an offset a
// time is local time, taken as UTC here. As in JavaScriptCore a day up to 31
// rolls into the next month ("2024-02-30" is March 1) and hour 24 is allowed
// only for 24:00:00. A string outside these forms gives ErrUnsupportedDate;
// a string inside them that is not a valid date gives an invalid Date.
func ParseDateString(s string) (Date, error) {
	p := dateScanner{s: s}
	year, ok := p.year()
	if !ok {
		return Date{}, ErrUnsupportedDate
	}
	month, day := int64(1), int64(1)
	if p.consume('-') {
		var okMonth bool
		if month, okMonth = p.twoDigits(); !okMonth {
			return Date{}, ErrUnsupportedDate
		}
		if p.consume('-') {
			var okDay bool
			if day, okDay = p.twoDigits(); !okDay {
				return Date{}, ErrUnsupportedDate
			}
		}
	}
	var hour, minute, second, millisecond, offsetMinutes int64
	if p.consumeAny("Tt ") {
		// The legacy parser reads a space-separated string, and it maps years 0-99 to 1900-2049.
		if p.s[p.pos-1] == ' ' && (p.expandedYear || year < legacyTwoDigitYearLimit) {
			return Date{}, ErrUnsupportedDate
		}
		var okTime bool
		if hour, minute, second, millisecond, okTime = p.timeOfDay(); !okTime {
			return Date{}, ErrUnsupportedDate
		}
		var okZone bool
		if offsetMinutes, okZone = p.zone(); !okZone {
			return Date{}, ErrUnsupportedDate
		}
	} else {
		p.consumeAny("Zz")
	}
	if !p.done() {
		return Date{}, ErrUnsupportedDate
	}
	inRange := month >= 1 && month <= maxMonth && day >= 1 && day <= maxDay &&
		hour <= maxHour && minute <= maxMinuteOrSecond && second <= maxMinuteOrSecond
	endOfDay := hour == hoursEndOfDay && (minute != 0 || second != 0 || millisecond != 0)
	// JavaScriptCore retries a string its ISO reader refuses with the legacy
	// parser, which may accept it as something else, so only a string that is
	// a valid ISO date is answered here.
	if !inRange || endOfDay || p.invalidOffset || (year == 0 && p.negativeZeroYear) {
		return Date{}, ErrUnsupportedDate
	}
	days := daysFromCivil(year, month, day)
	ms := days*msPerDay + hour*msPerHour + minute*msPerMinute + second*msPerSecond + millisecond - offsetMinutes*msPerMinute
	return DateFromMillis(float64(ms)), nil
}

type dateScanner struct {
	s                string
	pos              int
	invalidOffset    bool
	negativeZeroYear bool
	expandedYear     bool
}

func (p *dateScanner) done() bool { return p.pos == len(p.s) }

func (p *dateScanner) consume(c byte) bool {
	if p.pos < len(p.s) && p.s[p.pos] == c {
		p.pos++
		return true
	}
	return false
}

func (p *dateScanner) consumeAny(set string) bool {
	if p.pos < len(p.s) && indexByte(set, p.s[p.pos]) >= 0 {
		p.pos++
		return true
	}
	return false
}

func indexByte(set string, c byte) int {
	for i := 0; i < len(set); i++ {
		if set[i] == c {
			return i
		}
	}
	return -1
}

func (p *dateScanner) digits(n int) (int64, bool) {
	if p.pos+n > len(p.s) {
		return 0, false
	}
	var value int64
	for i := 0; i < n; i++ {
		c := p.s[p.pos+i]
		if !isDigit(c) {
			return 0, false
		}
		value = value*10 + int64(c-'0')
	}
	p.pos += n
	return value, true
}

func (p *dateScanner) twoDigits() (int64, bool) { return p.digits(2) }

func (p *dateScanner) year() (int64, bool) {
	if p.pos < len(p.s) && (p.s[p.pos] == '+' || p.s[p.pos] == '-') {
		negative := p.s[p.pos] == '-'
		p.expandedYear = true
		p.pos++
		value, ok := p.digits(isoExpandedDigits)
		if negative {
			p.negativeZeroYear = value == 0
			return -value, ok
		}
		return value, ok
	}
	return p.digits(isoYearDigits)
}

func (p *dateScanner) timeOfDay() (hour, minute, second, millisecond int64, ok bool) {
	var okHour, okMinute bool
	if hour, okHour = p.twoDigits(); !okHour || !p.consume(':') {
		return 0, 0, 0, 0, false
	}
	if minute, okMinute = p.twoDigits(); !okMinute {
		return 0, 0, 0, 0, false
	}
	if !p.consume(':') {
		return hour, minute, 0, 0, true
	}
	var okSecond bool
	if second, okSecond = p.twoDigits(); !okSecond {
		return 0, 0, 0, 0, false
	}
	if !p.consume('.') {
		return hour, minute, second, 0, true
	}
	start := p.pos
	for p.pos < len(p.s) && isDigit(p.s[p.pos]) {
		p.pos++
	}
	if p.pos == start {
		return 0, 0, 0, 0, false
	}
	scale := int64(pow10ForMsFraction)
	for i := start; i < p.pos && i < start+msFractionDigits; i++ {
		millisecond += int64(p.s[i]-'0') * scale
		scale /= 10
	}
	return hour, minute, second, millisecond, true
}

// zone reads the optional offset and returns it in minutes east of UTC.
func (p *dateScanner) zone() (int64, bool) {
	if p.pos == len(p.s) {
		return 0, true
	}
	if p.consumeAny("Zz") {
		return 0, true
	}
	if p.pos >= len(p.s) || (p.s[p.pos] != '+' && p.s[p.pos] != '-') {
		return 0, false
	}
	sign := int64(1)
	if p.s[p.pos] == '-' {
		sign = -1
	}
	p.pos++
	hours, ok := p.twoDigits()
	if !ok {
		return 0, false
	}
	p.consume(':')
	minutes, ok := p.twoDigits()
	if !ok {
		return 0, false
	}
	if hours > maxOffsetHours || minutes > maxMinuteOrSecond {
		p.invalidOffset = true
	}
	return sign * (hours*60 + minutes), true
}

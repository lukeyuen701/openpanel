package events

import (
	"encoding/json"
	"fmt"
	"math"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/jsport"
)

// eventPayload is an IServiceCreateEventPayload: the object the handler builds
// and createEvent and the notification check share. createdAt is kept as a Date
// next to its ISO text in the object, so time arithmetic stays exact.
type eventPayload struct {
	obj       *jsjson.Obj
	createdAt jsport.Date
}

func (p *eventPayload) millis() float64 {
	ms, ok := p.createdAt.Millis()
	if !ok {
		return math.NaN()
	}
	return float64(ms)
}

// copyOf is {...payload}: a new top-level object over the same members.
func (p *eventPayload) copyOf() *eventPayload {
	return &eventPayload{obj: shallowCopy(p.obj), createdAt: p.createdAt}
}

func (p *eventPayload) setCreatedAt(date jsport.Date) {
	p.createdAt = date
	p.obj.Set("createdAt", dateValue(date))
}

func (p *eventPayload) value() *jsjson.Value { return jsjson.NewObject(p.obj) }

// dateValue is how a Date sits in a JSON tree: its ISO string, null when invalid.
func dateValue(date jsport.Date) *jsjson.Value {
	text, err := date.ISOString()
	if err != nil {
		return jsjson.NewNull()
	}
	return str(text)
}

// parseRaw parses a raw JSON member; absent is nil (undefined).
func parseRaw(raw json.RawMessage) (*jsjson.Value, error) {
	if raw == nil {
		return nil, nil
	}
	value, err := jsport.ParseJSON(string(raw))
	if err != nil {
		return nil, fmt.Errorf("event member: %w", err)
	}
	return value, nil
}

// dateFromTimestamp is new Date(body.timestamp).
func dateFromTimestamp(raw json.RawMessage) (jsport.Date, error) {
	value, err := parseRaw(raw)
	if err != nil {
		return jsport.Date{}, err
	}
	switch {
	case value == nil:
		return jsport.DateFromMillis(math.NaN()), nil
	case value.Kind == jsjson.Null:
		return jsport.DateFromMillis(0), nil
	case value.Kind == jsjson.Bool:
		if value.Bool {
			return jsport.DateFromMillis(1), nil
		}
		return jsport.DateFromMillis(0), nil
	case value.Kind == jsjson.Number || value.Kind == jsjson.String:
		return jsport.ParseDate(value)
	default:
		return jsport.DateFromMillis(math.NaN()), nil
	}
}

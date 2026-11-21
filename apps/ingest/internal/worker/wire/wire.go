// Package wire is the Go form of the record the events topic carries
// (IncomingEventPayload, packages/core/src/modules/ingest/src/incoming-event.ts).
//
// Decoding follows what JSON.parse followed by property reads does in the
// TypeScript consumer: object keys are matched case-sensitively, a key that
// repeats keeps its last value, and the values the handler passes on
// untouched (properties, profile id, groups, timestamp, headers, ai) stay raw
// JSON bytes so a later re-serialisation sees what the producer wrote. Raw
// fields are nil when the key is absent and hold the literal null when it was
// null, which JavaScript tells apart too.
//
// Field order on the wire is the producer's: id, projectId, event, uaInfo,
// geo, headers, deviceId, sessionId, ai. Decoding does not depend on it, and
// anything that copies the payload into another JSON document (a BullMQ job,
// a dead-letter value) must keep the bytes it was given instead of
// re-encoding this struct.
package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// IncomingEventPayload is one record's value.
type IncomingEventPayload struct {
	// ID is nil on records written before the producer minted ids.
	ID        *string
	ProjectID string
	Event     Event
	UAInfo    UAInfo
	Geo       Geo
	// Headers is the raw object; read it with HeaderMap. Records without one
	// make the TypeScript handler throw, so the handler must too.
	Headers   json.RawMessage
	DeviceID  string
	SessionID string
	// AI is the raw AiEventRowDraft; its presence alone routes the message
	// down the server branch.
	AI json.RawMessage
}

// Event is the tracked event.
type Event struct {
	Name string
	// Properties, ProfileID (string or number), Groups (array or null) and
	// Timestamp (string or number) are raw JSON.
	Properties             json.RawMessage
	ProfileID              json.RawMessage
	Groups                 json.RawMessage
	Timestamp              json.RawMessage
	IsTimestampFromThePast bool
}

// UAInfo is the parsed user agent; absent fields are nil.
type UAInfo struct {
	IsServer       bool
	OS             *string
	OSVersion      *string
	Browser        *string
	BrowserVersion *string
	Device         *string
	Brand          *string
	Model          *string
}

// Geo is the resolved location; absent fields are nil.
type Geo struct {
	City      *string
	Country   *string
	Region    *string
	Longitude *float64
	Latitude  *float64
}

// Decoded is a record's value after JSON parsing.
type Decoded struct {
	Payload IncomingEventPayload
	// ShapeErr is set when the value is valid JSON the typed fields cannot
	// hold (not an object, a field of another type). TypeScript has no
	// decode step to fail there; its handler throws somewhere inside, so the
	// consumer treats ShapeErr as a failing handler attempt, not a parse error.
	ShapeErr error
}

// ErrNotObject is the ShapeErr of a value that is not a JSON object.
var ErrNotObject = errors.New("incoming event payload is not an object")

// Decode parses text (already UTF-8 decoded). The error is a JSON syntax
// error, the consumer's parse_error.
func Decode(text []byte) (Decoded, error) {
	var any json.RawMessage
	if err := json.Unmarshal(text, &any); err != nil {
		return Decoded{}, err
	}
	trimmed := bytes.TrimSpace(text)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return Decoded{ShapeErr: ErrNotObject}, nil
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &top); err != nil {
		return Decoded{ShapeErr: err}, nil
	}
	var d decoder
	p := IncomingEventPayload{
		ID:        d.optString(top, "id"),
		ProjectID: d.string(top, "projectId"),
		Headers:   top["headers"],
		DeviceID:  d.string(top, "deviceId"),
		SessionID: d.string(top, "sessionId"),
		AI:        top["ai"],
	}
	if raw, ok := top["event"]; ok {
		p.Event = d.event(raw)
	}
	if raw, ok := top["uaInfo"]; ok {
		p.UAInfo = d.uaInfo(raw)
	}
	if raw, ok := top["geo"]; ok {
		p.Geo = d.geo(raw)
	}
	return Decoded{Payload: p, ShapeErr: d.err}, nil
}

// HeaderMap returns the headers object. ok is false when it is absent, null
// or not an object.
func (p IncomingEventPayload) HeaderMap() (map[string]json.RawMessage, bool) {
	trimmed := bytes.TrimSpace(p.Headers)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, false
	}
	var headers map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &headers); err != nil {
		return nil, false
	}
	return headers, true
}

// decoder keeps the first shape error and reads fields leniently: a field of
// the wrong type is a zero value plus that error.
type decoder struct{ err error }

func (d *decoder) fail(name string, raw json.RawMessage, want string) {
	if d.err == nil {
		d.err = fmt.Errorf("%s is not %s: %.40s", name, want, raw)
	}
}

func isNull(raw json.RawMessage) bool {
	return len(raw) == 0 || string(bytes.TrimSpace(raw)) == "null"
}

func (d *decoder) optString(m map[string]json.RawMessage, name string) *string {
	raw := m[name]
	if isNull(raw) {
		return nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		d.fail(name, raw, "a string")
		return nil
	}
	return &s
}

func (d *decoder) string(m map[string]json.RawMessage, name string) string {
	if s := d.optString(m, name); s != nil {
		return *s
	}
	return ""
}

func (d *decoder) optFloat(m map[string]json.RawMessage, name string) *float64 {
	raw := m[name]
	if isNull(raw) {
		return nil
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err != nil {
		d.fail(name, raw, "a number")
		return nil
	}
	return &f
}

func (d *decoder) bool(m map[string]json.RawMessage, name string) bool {
	raw := m[name]
	if isNull(raw) {
		return false
	}
	var b bool
	if err := json.Unmarshal(raw, &b); err != nil {
		d.fail(name, raw, "a boolean")
	}
	return b
}

func (d *decoder) object(raw json.RawMessage, name string) map[string]json.RawMessage {
	if isNull(raw) {
		return nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		d.fail(name, raw, "an object")
		return nil
	}
	return m
}

func (d *decoder) event(raw json.RawMessage) Event {
	m := d.object(raw, "event")
	return Event{
		Name:                   d.string(m, "name"),
		Properties:             m["properties"],
		ProfileID:              m["profileId"],
		Groups:                 m["groups"],
		Timestamp:              m["timestamp"],
		IsTimestampFromThePast: d.bool(m, "isTimestampFromThePast"),
	}
}

func (d *decoder) uaInfo(raw json.RawMessage) UAInfo {
	m := d.object(raw, "uaInfo")
	return UAInfo{
		IsServer:       d.bool(m, "isServer"),
		OS:             d.optString(m, "os"),
		OSVersion:      d.optString(m, "osVersion"),
		Browser:        d.optString(m, "browser"),
		BrowserVersion: d.optString(m, "browserVersion"),
		Device:         d.optString(m, "device"),
		Brand:          d.optString(m, "brand"),
		Model:          d.optString(m, "model"),
	}
}

func (d *decoder) geo(raw json.RawMessage) Geo {
	m := d.object(raw, "geo")
	return Geo{
		City:      d.optString(m, "city"),
		Country:   d.optString(m, "country"),
		Region:    d.optString(m, "region"),
		Longitude: d.optFloat(m, "longitude"),
		Latitude:  d.optFloat(m, "latitude"),
	}
}

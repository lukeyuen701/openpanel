package sessions

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/chclient"
	"openpanel/ingest/internal/worker/jsport"
	"openpanel/ingest/internal/worker/notify"
)

const (
	// maxSessionEvents: funnel rules are not evaluated for longer sessions.
	maxSessionEvents = 500
	// eventsWindowPaddingMs pads the session's bounds on each side of the events query.
	eventsWindowPaddingMs = 1000

	sessionEventsStatement = `SELECT *
FROM events
WHERE session_id = {p1:String}
  AND project_id = {p2:String}
  AND created_at BETWEEN {p3:String} AND {p4:String}
ORDER BY created_at DESC
LIMIT {p5:UInt64}`

	clickhouseDateLayout = "2006-01-02 15:04:05"
)

// Querier runs a SELECT: *chclient.Client.
type Querier interface {
	Query(ctx context.Context, statement string, params []chclient.Param, settings chclient.Settings) (*chclient.Result, error)
}

// formatClickhouseDate is formatClickhouseDate: UTC, whole seconds.
func formatClickhouseDate(ms float64) string {
	text, err := jsport.DateFromMillis(ms).ISOString()
	if err != nil {
		// An invalid date has no ISO form; toISOString throws RangeError.
		return ""
	}
	// 2006-01-02T15:04:05.000Z
	return strings.Replace(text[:len(clickhouseDateLayout)], "T", " ", 1)
}

// sessionEvents is getSessionEvents: the session's events in ClickHouse plus
// its own exit page, newest first.
func sessionEvents(ctx context.Context, ch Querier, session *jsjson.Value) ([]notify.SessionEvent, error) {
	startAt := dateMillis(clickhouseDate(stringOr(session.Get("created_at"))))
	endAt := dateMillis(clickhouseDate(stringOr(session.Get("ended_at"))))
	if math.IsNaN(startAt) || math.IsNaN(endAt) {
		return nil, fmt.Errorf("%w: Invalid time value", jsport.ErrTypeError)
	}
	params := []chclient.Param{
		{Name: "p1", Value: chclient.FormatString(stringOr(session.Get("id")))},
		{Name: "p2", Value: chclient.FormatString(stringOr(session.Get("project_id")))},
		{Name: "p3", Value: chclient.FormatString(formatClickhouseDate(startAt - eventsWindowPaddingMs))},
		{Name: "p4", Value: chclient.FormatString(formatClickhouseDate(endAt + eventsWindowPaddingMs))},
		{Name: "p5", Value: fmt.Sprint(maxSessionEvents)},
	}
	result, err := ch.Query(ctx, sessionEventsStatement, params, nil)
	if err != nil {
		return nil, err
	}
	events := []notify.SessionEvent{transformSessionToEvent(session)}
	rows, err := coerceRows(result)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		events = append(events, transformEvent(row))
	}
	sort.SliceStable(events, func(i, j int) bool { return events[i].CreatedAtMs > events[j].CreatedAtMs })
	return events, nil
}

// coerceRows is chQueryWithMeta's reshaping: a truthy value of an Int column
// becomes a number, and the keys are those of the first row.
func coerceRows(result *chclient.Result) ([]*jsjson.Value, error) {
	types := map[string]string{}
	for _, column := range result.Meta {
		types[column.Name] = column.Type
	}
	var rows []*jsjson.Value
	var keys []string
	for i, raw := range result.Data {
		row, err := jsport.ParseJSONBytes(raw)
		if err != nil {
			return nil, fmt.Errorf("sessions: events row: %w", err)
		}
		if i == 0 {
			keys = row.Obj.Keys()
		}
		coerced := jsjson.NewObj()
		for _, key := range keys {
			value := row.Obj.Get(key)
			if isTruthy(value) && strings.Contains(types[key], "Int") && value.Kind == jsjson.String {
				value = num(jsport.ParseFloat(value.Str))
			}
			coerced.Set(key, value)
		}
		rows = append(rows, jsjson.NewObject(coerced))
	}
	return rows, nil
}

// stripFixedStringPadding removes the NUL padding of a FixedString.
func stripFixedStringPadding(v *jsjson.Value) *jsjson.Value {
	if !isString(v) {
		return v
	}
	return str(strings.TrimRight(v.Str, "\x00"))
}

// transformEvent is transformEvent of event.service.ts, as the JSON the
// notification payload stores.
func transformEvent(row *jsjson.Value) notify.SessionEvent {
	createdAt := clickhouseDate(stringOr(row.Get("created_at")))
	var importedAt *jsjson.Value
	if isTruthy(row.Get("imported_at")) {
		if date, err := jsport.ParseDate(row.Get("imported_at")); err == nil {
			importedAt = dateValue(date)
		} else {
			importedAt = jsjson.NewNull()
		}
	}
	event := jsjson.NewObj()
	for _, field := range [...]struct{ to, from string }{
		{"id", "id"}, {"name", "name"}, {"deviceId", "device_id"}, {"profileId", "profile_id"},
		{"projectId", "project_id"}, {"sessionId", "session_id"}, {"properties", "properties"},
	} {
		event.Set(field.to, row.Get(field.from))
	}
	event.Set("createdAt", dateValue(createdAt))
	event.Set("country", stripFixedStringPadding(row.Get("country")))
	for _, field := range [...]struct{ to, from string }{
		{"city", "city"}, {"region", "region"}, {"longitude", "longitude"}, {"latitude", "latitude"},
		{"os", "os"}, {"osVersion", "os_version"}, {"browser", "browser"}, {"browserVersion", "browser_version"},
		{"device", "device"}, {"brand", "brand"}, {"model", "model"}, {"path", "path"}, {"origin", "origin"},
		{"referrer", "referrer"}, {"referrerName", "referrer_name"}, {"referrerType", "referrer_type"},
		{"meta", "meta"},
	} {
		event.Set(field.to, row.Get(field.from))
	}
	event.Set("importedAt", importedAt)
	event.Set("sdkName", row.Get("sdk_name"))
	event.Set("sdkVersion", row.Get("sdk_version"))
	event.Set("profile", row.Get("profile"))
	event.Set("revenue", row.Get("revenue"))
	event.Set("groups", orDefault(row.Get("groups"), &jsjson.Value{Kind: jsjson.Array}))
	return notify.SessionEvent{Value: jsjson.NewObject(event), CreatedAtMs: dateMillis(createdAt)}
}

// transformSessionToEvent is transformSessionToEvent: the session's exit page
// as a screen_view.
func transformSessionToEvent(session *jsjson.Value) notify.SessionEvent {
	createdAt := clickhouseDate(stringOr(session.Get("ended_at")))
	event := jsjson.NewObj()
	event.Set("id", str(""))
	event.Set("name", str("screen_view"))
	event.Set("sessionId", session.Get("id"))
	event.Set("profileId", session.Get("profile_id"))
	event.Set("path", session.Get("exit_path"))
	event.Set("origin", session.Get("exit_origin"))
	event.Set("createdAt", dateValue(createdAt))
	for _, field := range [...]struct{ to, from string }{
		{"referrer", "referrer"}, {"referrerName", "referrer_name"}, {"referrerType", "referrer_type"},
		{"os", "os"}, {"osVersion", "os_version"}, {"browser", "browser"}, {"browserVersion", "browser_version"},
		{"device", "device"}, {"brand", "brand"}, {"model", "model"}, {"country", "country"},
		{"region", "region"}, {"city", "city"}, {"longitude", "longitude"}, {"latitude", "latitude"},
		{"projectId", "project_id"}, {"deviceId", "device_id"},
	} {
		event.Set(field.to, session.Get(field.from))
	}
	event.Set("duration", num(0))
	event.Set("revenue", session.Get("revenue"))
	query := jsjson.NewObj()
	for _, key := range [...]string{"utm_medium", "utm_source", "utm_campaign", "utm_content", "utm_term"} {
		query.Set(key, session.Get(key))
	}
	properties := jsjson.NewObj()
	properties.Set("is_bounce", session.Get("is_bounce"))
	properties.Set("__query", jsjson.NewObject(query))
	event.Set("properties", jsjson.NewObject(properties))
	event.Set("profile", nil)
	event.Set("meta", nil)
	event.Set("importedAt", nil)
	event.Set("sdkName", nil)
	event.Set("sdkVersion", nil)
	event.Set("groups", &jsjson.Value{Kind: jsjson.Array})
	return notify.SessionEvent{Value: jsjson.NewObject(event), CreatedAtMs: dateMillis(createdAt)}
}

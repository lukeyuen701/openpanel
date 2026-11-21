package events

import (
	"context"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/jsport"
)

// preparedEvent is what createEvent's synchronous prefix produced: the row it
// handed to the buffer and the profile it has still to upsert.
type preparedEvent struct {
	row     *jsjson.Obj
	profile *jsjson.Obj
}

// prepareEvent is the synchronous prefix of createEvent: the profile id falls
// back to the device id (in the caller's payload, which the notification check
// then sees), the row is built and handed to the event buffer.
func (h *Handler) prepareEvent(payload *eventPayload, redeliverable bool) (*preparedEvent, error) {
	o := payload.obj
	if !isTruthy(get(o, "profileId")) && isTruthy(get(o, "deviceId")) {
		o.Set("profileId", get(o, "deviceId"))
	}

	dots, err := jsport.ToDots(asObject(get(o, "properties")))
	if err != nil {
		return nil, err
	}
	empty := str("")
	field := func(key string) *jsjson.Value { return orDefault(get(o, key), empty) }
	id := get(o, "id")
	if isNullish(id) {
		id = str(h.IDs.NewUUID())
	}
	profileID := get(o, "profileId")
	profileText := empty
	if isTruthy(profileID) {
		profileText = str(stringOf(profileID))
	}

	row := jsjson.NewObj()
	row.Set("id", id)
	row.Set("name", get(o, "name"))
	row.Set("device_id", get(o, "deviceId"))
	row.Set("profile_id", profileText)
	row.Set("project_id", get(o, "projectId"))
	row.Set("session_id", get(o, "sessionId"))
	row.Set("properties", jsjson.NewObject(dots))
	row.Set("path", field("path"))
	row.Set("origin", field("origin"))
	row.Set("created_at", str(payload.createdAt.LuxonFormat()))
	row.Set("country", field("country"))
	row.Set("city", field("city"))
	row.Set("region", field("region"))
	row.Set("longitude", orDefault(get(o, "longitude"), jsjson.NewNull()))
	row.Set("latitude", orDefault(get(o, "latitude"), jsjson.NewNull()))
	row.Set("os", field("os"))
	row.Set("os_version", field("osVersion"))
	row.Set("browser", field("browser"))
	row.Set("browser_version", field("browserVersion"))
	row.Set("device", field("device"))
	row.Set("brand", field("brand"))
	row.Set("model", field("model"))
	row.Set("duration", orDefault(get(o, "duration"), num(0)))
	row.Set("referrer", field("referrer"))
	row.Set("referrer_name", field("referrerName"))
	row.Set("referrer_type", field("referrerType"))
	row.Set("imported_at", jsjson.NewNull())
	// Stamped here, not by the column default, so a backdated event still gets
	// the time it was ingested.
	row.Set("inserted_at", str(jsport.FormatNow(h.Clock)))
	row.Set("sdk_name", field("sdkName"))
	row.Set("sdk_version", field("sdkVersion"))
	row.Set("revenue", get(o, "revenue"))
	row.Set("groups", orDefault(get(o, "groups"), &jsjson.Value{Kind: jsjson.Array}))

	line, err := stringify(jsjson.NewObject(row))
	if err != nil {
		return nil, err
	}
	// Redeliverable: a failed write drops the row and relies on the Kafka batch
	// being redelivered, so a retry never leaves a second copy behind.
	h.Events.Add(line, redeliverable)

	prepared := &preparedEvent{row: row}
	name := stringOf(get(o, "name"))
	if isTruthy(profileID) && (name == nameSessionStart || name == nameSessionEnd) {
		prepared.profile = profileRow(o, h.Clock)
	}
	return prepared, nil
}

// writeProfile is the awaited tail of createEvent: the profile upsert of a
// session boundary.
func (h *Handler) writeProfile(ctx context.Context, prepared *preparedEvent) {
	if prepared.profile != nil {
		h.Profiles.Add(ctx, prepared.profile, true)
	}
}

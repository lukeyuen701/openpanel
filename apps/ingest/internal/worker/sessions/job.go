package sessions

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/bullmq"
	"openpanel/ingest/internal/worker/jsport"
	"openpanel/ingest/internal/worker/wire"
)

const (
	// QueueName and JobName address the sessions queue's only job.
	QueueName = "sessions"
	JobName   = "session"

	legacyJobType = "createSessionEnd"
)

// DecodeJob is resolveJob and the job's payload schema: an envelope, or the
// legacy {type: 'createSessionEnd', payload, snapshot} data still replayable
// from a failed set. The schema is a shape check only.
func DecodeJob(name string, data []byte, ids wire.IDSource) (Job, error) {
	root, err := jsport.ParseJSONBytes(data)
	if err != nil {
		return Job{}, fmt.Errorf("sessions: job data: %w", err)
	}
	var payload *jsjson.Value
	requestID := ""
	if root.IsObject() && root.Obj.Has("payload") && root.Obj.Has("meta") {
		if name != JobName {
			return Job{}, fmt.Errorf("Queue '%s' has no job named '%s'", QueueName, name)
		}
		payload = root.Get("payload")
		meta := root.Get("meta")
		if meta == nil || meta.Kind == jsjson.Null {
			return Job{}, errors.New("Cannot read properties of null (reading 'requestId')")
		}
		if isString(meta.Get("requestId")) {
			requestID = meta.Get("requestId").Str
		}
	} else if root.IsObject() && isString(root.Get("type")) && root.Get("type").Str == legacyJobType {
		legacy := jsjson.NewObj()
		legacy.Set("event", root.Get("payload"))
		legacy.Set("snapshot", root.Get("snapshot"))
		payload = jsjson.NewObject(legacy)
	} else {
		return Job{}, fmt.Errorf("Queue '%s' cannot resolve job '%s': not an envelope, and no compat hook matched its data", QueueName, name)
	}
	// A scheduled or legacy job has no originating request.
	if requestID == "" {
		requestID = ids.NewID(wire.RequestIDLength)
	}
	return checkShape(payload, requestID)
}

func checkShape(payload *jsjson.Value, requestID string) (Job, error) {
	event, snapshot := payload.Get("event"), payload.Get("snapshot")
	if !event.IsObject() || !snapshot.IsObject() {
		return Job{}, errors.New("session job payload does not match the schema")
	}
	for _, key := range [...]string{"projectId", "deviceId"} {
		if !isString(event.Get(key)) {
			return Job{}, fmt.Errorf("session job payload does not match the schema: event.%s", key)
		}
	}
	for _, key := range [...]string{"id", "project_id", "device_id", "ended_at"} {
		if !isString(snapshot.Get(key)) {
			return Job{}, fmt.Errorf("session job payload does not match the schema: snapshot.%s", key)
		}
	}
	return Job{Event: event.Obj, Snapshot: snapshot.Obj, RequestID: requestID}, nil
}

// UsageUpdater is updateEventsCount.
type UsageUpdater interface {
	UpdateEventsCount(ctx context.Context, projectID string) error
}

// Handler is the sessions queue's job handler: createSessionEnd, then
// updateEventsCount whether or not an event was emitted. A failure of
// createSessionEnd fails the attempt; one of updateEventsCount is only logged.
func Handler(ender *Ender, usage UsageUpdater, ids wire.IDSource, logger *slog.Logger) bullmq.Handler {
	return func(ctx context.Context, job bullmq.Job) error {
		decoded, err := DecodeJob(job.Name, job.Data, ids)
		if err != nil {
			return err
		}
		if _, err := ender.Create(ctx, decoded); err != nil {
			return err
		}
		projectID := stringOr(decoded.Event.Get("projectId"))
		if err := usage.UpdateEventsCount(ctx, projectID); err != nil {
			logger.Error("Failed to update events count", slog.Any("err", err), slog.String("projectId", projectID), slog.String("sessionId", stringOr(decoded.Snapshot.Get("id"))))
		}
		return nil
	}
}

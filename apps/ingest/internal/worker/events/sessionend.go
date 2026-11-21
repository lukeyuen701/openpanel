package events

import (
	"context"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/jsport"
)

// CreateEvent is createEvent for a caller that has no Kafka batch to redeliver
// (the sessions job): the row joins the event buffer, which keeps it and
// retries when a write fails, and the profile upsert of a session boundary
// follows. The payload must be a fresh object; createdAt replaces its
// createdAt member. It returns the row, the document of createEvent.
func (h *Handler) CreateEvent(ctx context.Context, payload *jsjson.Obj, createdAt jsport.Date) (*jsjson.Obj, error) {
	event := &eventPayload{obj: payload, createdAt: createdAt}
	event.setCreatedAt(createdAt)
	prepared, err := h.prepareEvent(event, false)
	if err != nil {
		return nil, err
	}
	h.writeProfile(ctx, prepared)
	return prepared.row, nil
}

// EnqueueSessionEnd adds the session-end job of closed. event is the payload
// of the event that caused it (the first event of the next session at a
// boundary, the reaper's rebuilt payload otherwise).
func (h *Handler) EnqueueSessionEnd(ctx context.Context, requestID string, event, closed *jsjson.Obj) error {
	return h.enqueueSessionEnd(ctx, scope{requestID: requestID, logger: h.Logger}, &eventPayload{obj: event}, closed)
}

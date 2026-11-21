package events

import (
	"context"

	"openpanel/ingest/internal/worker/bullmq"
	"openpanel/ingest/internal/worker/notify"
	"openpanel/ingest/internal/worker/wire"
)

// NotificationQueue adapts a BullMQ producer to notify.Enqueuer: the data is
// already the {payload, meta} envelope and the queue's defaults apply.
type NotificationQueue struct{ Producer SessionQueue }

// Add implements notify.Enqueuer.
func (q NotificationQueue) Add(ctx context.Context, queue, name string, data []byte, opts notify.JobOptions) (string, error) {
	return q.Producer.Add(ctx, queue, name, data, bullmq.AddOptions{
		JobID:       opts.JobID,
		Attempts:    opts.Attempts,
		AttemptsSet: opts.Attempts != 0,
	})
}

// RandomIDs is the production IDs.
type RandomIDs struct{ wire.RandomIDs }

// NewUUID implements IDs.
func (RandomIDs) NewUUID() string { return newUUID() }

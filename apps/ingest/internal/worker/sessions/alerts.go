package sessions

import (
	"context"
	"fmt"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/bullmq"
	"openpanel/ingest/internal/worker/jsport"
	"openpanel/ingest/internal/worker/wire"
)

// JobAdder adds a BullMQ job: *bullmq.Producer.
type JobAdder interface {
	Add(ctx context.Context, queueName, jobName string, data []byte, opts bullmq.AddOptions) (string, error)
}

// BullAlertQueue enqueues the sendUsageAlert job on the notification queue,
// which the Bun worker keeps consuming: the e-mails are React Email templates.
type BullAlertQueue struct {
	Producer JobAdder
	IDs      wire.IDSource
}

// EnqueueUsageAlert implements AlertQueue.
func (q BullAlertQueue) EnqueueUsageAlert(ctx context.Context, organizationID string, count int64) error {
	payload := jsjson.NewObj()
	payload.Set("organizationId", jsjson.NewString(organizationID))
	payload.Set("count", jsjson.NewNumber(float64(count)))
	body, err := jsport.Stringify(jsjson.NewObject(payload))
	if err != nil {
		return err
	}
	data := bullmq.Envelope([]byte(body), q.IDs.NewID(wire.RequestIDLength))
	if _, err := q.Producer.Add(ctx, UsageAlertQueue, UsageAlertJob, data, bullmq.AddOptions{}); err != nil {
		return fmt.Errorf("sessions: enqueue usage alert: %w", err)
	}
	return nil
}

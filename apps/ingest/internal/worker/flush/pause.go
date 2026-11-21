package flush

import (
	"context"

	"openpanel/ingest/internal/worker/bullmq"
)

const (
	cronQueueName   = "cron"
	bullKeyPrefix   = "bull:"
	pausedMetaField = "paused"
)

// CronPauseCheck is Queue.isPaused() on the BullMQ cron queue: HEXISTS of the
// "paused" field of its meta hash. Pausing cron in bull-board halts every
// flush, the Go ones too. queueNamespace and queueCluster are QUEUE_NAMESPACE
// and QUEUE_CLUSTER, which decide the queue's key.
func CronPauseCheck(redis Redis, queueNamespace string, queueCluster bool) PauseCheck {
	metaKey := bullKeyPrefix + bullmq.QueueKey(cronQueueName, queueNamespace, queueCluster) + ":meta"
	return func(ctx context.Context) (bool, error) {
		return redis.HExists(ctx, metaKey, pausedMetaField)
	}
}

// NeverPaused is for tests and for a process with no cron queue.
func NeverPaused(context.Context) (bool, error) { return false, nil }

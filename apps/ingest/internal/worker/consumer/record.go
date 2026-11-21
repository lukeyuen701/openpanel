package consumer

import (
	"context"

	"openpanel/ingest/internal/worker/wire"
)

// Header is one record header, in the order the broker returned it.
type Header struct {
	Key   string
	Value []byte
}

// Record is one message of the events topic. A nil Key or Value is a Kafka
// null; an empty non-nil one is a zero-length value.
type Record struct {
	Partition   int32
	Offset      int64
	LeaderEpoch int32
	Key         []byte
	Value       []byte
	Headers     []Header
}

// Delivery says where a payload came from.
type Delivery struct {
	Partition int32
	Offset    int64
}

// Handler processes one event. An error is a failed attempt; the consumer
// retries it and, when attempts run out, dead-letters the record. A handler
// must be safe to call again for the same record.
type Handler func(ctx context.Context, payload wire.IncomingEventPayload, delivery Delivery) error

// Durability is the gate between handling a batch and committing it.
type Durability interface {
	// OpenWindow is called before the first handler of a batch runs. The
	// returned func is called once every message has been handled and before
	// any offset is resolved; an error means the events this batch buffered
	// are not in Redis, so the whole batch is redelivered.
	OpenWindow() (closeWindow func(ctx context.Context) error)
}

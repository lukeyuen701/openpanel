package consumer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/rueidis"

	"openpanel/ingest/internal/jsjson"
)

// DeadLetterKey is the Redis list the dead letters go to
// (packages/redis/dead-letter.ts).
const DeadLetterKey = "dead_letter:events"

// BufferDeadLetterKey is the list of buffer rows a flush could not read, capped
// like DeadLetterKey by INGEST_DEAD_LETTER_MAX_ENTRIES.
const BufferDeadLetterKey = "dead_letter:buffer"

// DeadLetterMessage is a record that is dropped, with why.
type DeadLetterMessage struct {
	Key       []byte
	Value     []byte
	Headers   []Header
	Topic     string
	Partition int32
	Offset    int64
	Reason    string
	Err       string
}

// DeadLetterSink parks a dropped message. It may fail; the caller drops the
// message and resolves its offset regardless.
type DeadLetterSink interface {
	Record(ctx context.Context, message DeadLetterMessage) error
}

// ListStore pushes onto a capped Redis list in one atomic step.
type ListStore interface {
	// PushCapped is MULTI, LPUSH key value, LTRIM key 0 maxEntries-1, EXEC. It
	// fails when Redis discarded the transaction or any command in it errored.
	PushCapped(ctx context.Context, key, value string, maxEntries int) error
}

// RedisDeadLetter writes the capped list the Bun worker writes, newest first.
type RedisDeadLetter struct {
	store      ListStore
	clock      Clock
	maxEntries int
}

// NewRedisDeadLetter keeps the last maxEntries records.
func NewRedisDeadLetter(store ListStore, clock Clock, maxEntries int) *RedisDeadLetter {
	return &RedisDeadLetter{store: store, clock: clock, maxEntries: maxEntries}
}

// Record implements DeadLetterSink.
func (d *RedisDeadLetter) Record(ctx context.Context, message DeadLetterMessage) error {
	return d.store.PushCapped(ctx, DeadLetterKey, EncodeDeadLetter(message, d.clock.Now()), d.maxEntries)
}

const isoMillisLayout = "2006-01-02T15:04:05.000Z"

// EncodeDeadLetter is JSON.stringify(toDeadLetterRecord(...)): recordedAt,
// topic, partition, offset (a string), reason, error, key, value, headers.
func EncodeDeadLetter(message DeadLetterMessage, recordedAt time.Time) string {
	record := jsjson.NewObj()
	record.Set("recordedAt", jsjson.NewString(recordedAt.UTC().Format(isoMillisLayout)))
	record.Set("topic", jsjson.NewString(message.Topic))
	record.Set("partition", jsjson.NewNumber(float64(message.Partition)))
	record.Set("offset", jsjson.NewString(strconv.FormatInt(message.Offset, 10)))
	record.Set("reason", jsjson.NewString(message.Reason))
	record.Set("error", jsjson.NewString(message.Err))
	record.Set("key", textOrNull(message.Key))
	record.Set("value", textOrNull(message.Value))
	headers := jsjson.NewObj()
	for _, header := range message.Headers {
		headers.Set(header.Key, jsjson.NewString(decodeText(header.Value)))
	}
	record.Set("headers", jsjson.NewObject(headers))
	return string(jsjson.Stringify(jsjson.NewObject(record)))
}

// EncodeBufferDeadLetter is the record of a buffer row a flush set aside:
// recordedAt, buffer, reason and the raw row, as the events record writes them.
func EncodeBufferDeadLetter(buffer, reason, row string, recordedAt time.Time) string {
	record := jsjson.NewObj()
	record.Set("recordedAt", jsjson.NewString(recordedAt.UTC().Format(isoMillisLayout)))
	record.Set("buffer", jsjson.NewString(buffer))
	record.Set("reason", jsjson.NewString(reason))
	record.Set("row", jsjson.NewString(decodeText([]byte(row))))
	return string(jsjson.Stringify(jsjson.NewObject(record)))
}

func textOrNull(b []byte) *jsjson.Value {
	if b == nil {
		return jsjson.NewNull()
	}
	return jsjson.NewString(decodeText(b))
}

// RueidisListStore is the ListStore over a rueidis client.
type RueidisListStore struct {
	client  rueidis.Client
	timeout time.Duration
}

// NewRueidisListStore bounds every write by timeout (REDIS_CACHE_COMMAND_TIMEOUT_MS):
// the dead letter must fail fast when Redis is down.
func NewRueidisListStore(client rueidis.Client, timeout time.Duration) *RueidisListStore {
	return &RueidisListStore{client: client, timeout: timeout}
}

// PushCapped implements ListStore.
func (s *RueidisListStore) PushCapped(ctx context.Context, key, value string, maxEntries int) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	b := s.client.B()
	results := s.client.DoMulti(ctx,
		b.Multi().Build(),
		b.Lpush().Key(key).Element(value).Build(),
		b.Ltrim().Key(key).Start(0).Stop(int64(maxEntries-1)).Build(),
		b.Exec().Build(),
	)
	for _, result := range results {
		if err := result.Error(); err != nil && !rueidis.IsRedisNil(err) {
			return fmt.Errorf("dead-letter MULTI on %s: %w", key, err)
		}
	}
	executed, err := results[len(results)-1].ToArray()
	if err != nil {
		return fmt.Errorf("dead-letter MULTI on %s was discarded by Redis: %w", key, err)
	}
	for _, command := range executed {
		if err := command.Error(); err != nil {
			return fmt.Errorf("dead-letter command on %s: %w", key, err)
		}
	}
	return nil
}

var errNoValue = errors.New("kafka message has no value")

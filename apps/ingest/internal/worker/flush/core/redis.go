package core

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/rueidis"

	"openpanel/ingest/internal/worker/flush"
)

// Redis is what the four flushes send beyond the commands of flush.Redis.
type Redis interface {
	flush.Redis
	// MGet is MGET; a missing key is the empty string, which the flush treats
	// the same as a miss.
	MGet(ctx context.Context, keys []string) ([]string, error)
	// CommitProfiles runs, in one MULTI/EXEC, a SET key value EX ttl for every
	// write and then LTRIM listKey consumed -1. It fails if EXEC was aborted or
	// any command in it failed.
	CommitProfiles(ctx context.Context, writes []CacheWrite, ttl time.Duration, listKey string, consumed int) error
	// Publish is PUBLISH channel message.
	Publish(ctx context.Context, channel, message string) error
}

// CacheWrite is one profile-cache entry.
type CacheWrite struct{ Key, Value string }

// RueidisRedis is Redis on a rueidis client; the commands of flush.Redis come
// from the embedded adapter.
type RueidisRedis struct {
	*flush.RueidisRedis
	client  rueidis.Client
	timeout time.Duration
}

// NewRueidis wraps client; the caller keeps ownership and closes it. Every
// command is bounded by timeout, REDIS_CACHE_COMMAND_TIMEOUT_MS.
func NewRueidis(client rueidis.Client, timeout time.Duration) *RueidisRedis {
	return &RueidisRedis{RueidisRedis: flush.NewRueidis(client, timeout), client: client, timeout: timeout}
}

func wrap(err error) error {
	if err == nil {
		return nil
	}
	if redisErr, ok := rueidis.IsRedisErr(err); ok {
		return &flush.ReplyError{Message: redisErr.Error()}
	}
	return fmt.Errorf("redis: %w", err)
}

// MGet implements Redis.
func (r *RueidisRedis) MGet(ctx context.Context, keys []string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	replies, err := r.client.Do(ctx, r.client.B().Mget().Key(keys...).Build()).ToArray()
	if err != nil {
		return nil, wrap(err)
	}
	values := make([]string, len(replies))
	for i, reply := range replies {
		if reply.IsNil() {
			continue
		}
		text, err := reply.ToString()
		if err != nil {
			return nil, wrap(err)
		}
		values[i] = text
	}
	return values, nil
}

// CommitProfiles implements Redis.
func (r *RueidisRedis) CommitProfiles(ctx context.Context, writes []CacheWrite, ttl time.Duration, listKey string, consumed int) error {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	seconds := int64(ttl / time.Second)
	commands := make(rueidis.Commands, 0, len(writes)+3)
	commands = append(commands, r.client.B().Multi().Build())
	for _, write := range writes {
		commands = append(commands, r.client.B().Set().Key(write.Key).Value(write.Value).ExSeconds(seconds).Build())
	}
	commands = append(commands, r.client.B().Ltrim().Key(listKey).Start(int64(consumed)).Stop(-1).Build())
	commands = append(commands, r.client.B().Exec().Build())

	replies := r.client.DoMulti(ctx, commands...)
	for _, reply := range replies[:len(replies)-1] {
		if err := reply.Error(); err != nil {
			return wrap(err)
		}
	}
	executed, err := replies[len(replies)-1].ToArray()
	if err != nil {
		return wrap(err)
	}
	var failures []error
	for _, reply := range executed {
		if err := reply.Error(); err != nil {
			failures = append(failures, wrap(err))
		}
	}
	return errors.Join(failures...)
}

// Publish implements Redis.
func (r *RueidisRedis) Publish(ctx context.Context, channel, message string) error {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	return wrap(r.client.Do(ctx, r.client.B().Publish().Channel(channel).Message(message).Build()).Error())
}

package events

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/rueidis"
)

// RueidisRedis is the Redis surface over a rueidis client. Every call is bounded
// by timeout (REDIS_CACHE_COMMAND_TIMEOUT_MS), so a Redis that is down fails
// the call instead of holding the message.
type RueidisRedis struct {
	client  rueidis.Client
	timeout time.Duration
}

// NewRueidisRedis wraps client; the caller keeps ownership and closes it.
func NewRueidisRedis(client rueidis.Client, timeout time.Duration) *RueidisRedis {
	return &RueidisRedis{client: client, timeout: timeout}
}

func (r *RueidisRedis) bounded(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, r.timeout)
}

// Get implements Redis.
func (r *RueidisRedis) Get(ctx context.Context, key string) (string, bool, error) {
	ctx, cancel := r.bounded(ctx)
	defer cancel()
	value, err := r.client.Do(ctx, r.client.B().Get().Key(key).Build()).ToString()
	if rueidis.IsRedisNil(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("redis get: %w", err)
	}
	return value, true, nil
}

// Del implements Redis.
func (r *RueidisRedis) Del(ctx context.Context, key string) error {
	ctx, cancel := r.bounded(ctx)
	defer cancel()
	if err := r.client.Do(ctx, r.client.B().Del().Key(key).Build()).Error(); err != nil {
		return fmt.Errorf("redis del: %w", err)
	}
	return nil
}

// Exists implements Redis.
func (r *RueidisRedis) Exists(ctx context.Context, key string) (bool, error) {
	ctx, cancel := r.bounded(ctx)
	defer cancel()
	count, err := r.client.Do(ctx, r.client.B().Exists().Key(key).Build()).AsInt64()
	if err != nil {
		return false, fmt.Errorf("redis exists: %w", err)
	}
	return count == 1, nil
}

// RPush implements Redis.
func (r *RueidisRedis) RPush(ctx context.Context, key string, values ...string) error {
	ctx, cancel := r.bounded(ctx)
	defer cancel()
	if err := r.client.Do(ctx, r.client.B().Rpush().Key(key).Element(values...).Build()).Error(); err != nil {
		return fmt.Errorf("redis rpush %s: %w", key, err)
	}
	return nil
}

// Transaction implements Redis.
func (r *RueidisRedis) Transaction(ctx context.Context, commands [][]string) error {
	ctx, cancel := r.bounded(ctx)
	defer cancel()
	built := make(rueidis.Commands, 0, len(commands)+2)
	built = append(built, r.client.B().Multi().Build())
	for _, command := range commands {
		built = append(built, r.client.B().Arbitrary(command[0]).Args(command[1:]...).Build())
	}
	built = append(built, r.client.B().Exec().Build())
	results := r.client.DoMulti(ctx, built...)
	for _, result := range results {
		if err := result.Error(); err != nil && !rueidis.IsRedisNil(err) {
			return fmt.Errorf("redis transaction: %w", err)
		}
	}
	executed, err := results[len(results)-1].ToArray()
	if err != nil {
		return fmt.Errorf("session write-back transaction was aborted: %w", err)
	}
	for _, reply := range executed {
		if err := reply.Error(); err != nil {
			return fmt.Errorf("redis transaction command: %w", err)
		}
	}
	return nil
}

// LLen is the length of a list; a missing key is 0.
func (r *RueidisRedis) LLen(ctx context.Context, key string) (int64, error) {
	ctx, cancel := r.bounded(ctx)
	defer cancel()
	return r.client.Do(ctx, r.client.B().Llen().Key(key).Build()).AsInt64()
}

// ZCard is the size of a sorted set; a missing key is 0.
func (r *RueidisRedis) ZCard(ctx context.Context, key string) (int64, error) {
	ctx, cancel := r.bounded(ctx)
	defer cancel()
	return r.client.Do(ctx, r.client.B().Zcard().Key(key).Build()).AsInt64()
}

// SMembers is the members of a set.
func (r *RueidisRedis) SMembers(ctx context.Context, key string) ([]string, error) {
	ctx, cancel := r.bounded(ctx)
	defer cancel()
	return r.client.Do(ctx, r.client.B().Smembers().Key(key).Build()).AsStrSlice()
}

// Ping checks the connection.
func (r *RueidisRedis) Ping(ctx context.Context) error {
	ctx, cancel := r.bounded(ctx)
	defer cancel()
	return r.client.Do(ctx, r.client.B().Ping().Build()).Error()
}

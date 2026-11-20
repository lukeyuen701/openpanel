// Package kv is the handful of Redis commands the receiver sends, over
// rueidis. Its auto-pipelining puts concurrent requests' commands on a few
// connections in shared writes, as ioredis does for the Bun API; one
// connection per command costs a syscall pair per request.
package kv

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/rueidis"
)

// Store is the receiver's Redis surface.
type Store interface {
	// Get returns the value and whether the key exists.
	Get(ctx context.Context, key string) (string, bool, error)
	// SetNX sets key with a TTL only when absent, reporting whether it did.
	SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	// MGet reads keys in order; nil marks a missing key.
	MGet(ctx context.Context, keys []string) ([]*string, error)
	// SetEx sets key with a TTL in seconds.
	SetEx(ctx context.Context, key, value string, ttl time.Duration) error
	Ping(ctx context.Context) error
	Close()
}

// Redis implements Store.
type Redis struct {
	client  rueidis.Client
	timeout time.Duration
}

// Open connects from a redis:// or rediss:// URL.
func Open(url string, timeout time.Duration) (*Redis, error) {
	options, err := rueidis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("kv: REDIS_URL: %w", err)
	}
	// Plain RESP2-compatible commands only; client-side caching needs
	// tracking the API's Redis may not grant.
	options.DisableCache = true
	options.ConnWriteTimeout = timeout
	client, err := rueidis.NewClient(options)
	if err != nil {
		return nil, fmt.Errorf("kv: connect: %w", err)
	}
	return &Redis{client: client, timeout: timeout}, nil
}

func (r *Redis) bounded(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, r.timeout)
}

// Get implements Store.
func (r *Redis) Get(ctx context.Context, key string) (string, bool, error) {
	ctx, cancel := r.bounded(ctx)
	defer cancel()
	value, err := r.client.Do(ctx, r.client.B().Get().Key(key).Build()).ToString()
	if rueidis.IsRedisNil(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("kv: get: %w", err)
	}
	return value, true, nil
}

// SetNX implements Store.
func (r *Redis) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	ctx, cancel := r.bounded(ctx)
	defer cancel()
	err := r.client.Do(ctx, r.client.B().Set().Key(key).Value(value).Nx().PxMilliseconds(ttl.Milliseconds()).Build()).Error()
	if rueidis.IsRedisNil(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("kv: set nx: %w", err)
	}
	return true, nil
}

// MGet implements Store.
func (r *Redis) MGet(ctx context.Context, keys []string) ([]*string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	ctx, cancel := r.bounded(ctx)
	defer cancel()
	messages, err := r.client.Do(ctx, r.client.B().Mget().Key(keys...).Build()).ToArray()
	if err != nil {
		return nil, fmt.Errorf("kv: mget: %w", err)
	}
	out := make([]*string, len(messages))
	for i, message := range messages {
		if message.IsNil() {
			continue
		}
		value, err := message.ToString()
		if err != nil {
			continue
		}
		out[i] = &value
	}
	return out, nil
}

// SetEx implements Store.
func (r *Redis) SetEx(ctx context.Context, key, value string, ttl time.Duration) error {
	ctx, cancel := r.bounded(ctx)
	defer cancel()
	if err := r.client.Do(ctx, r.client.B().Setex().Key(key).Seconds(int64(ttl.Seconds())).Value(value).Build()).Error(); err != nil {
		return fmt.Errorf("kv: setex: %w", err)
	}
	return nil
}

// Ping implements Store.
func (r *Redis) Ping(ctx context.Context) error {
	return r.client.Do(ctx, r.client.B().Ping().Build()).Error()
}

// Close implements Store.
func (r *Redis) Close() { r.client.Close() }

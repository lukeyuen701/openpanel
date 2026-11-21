package sessions

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/rueidis"
)

// cleanupLua is cleanupScript of packages/core/src/buffers/session-buffer.ts,
// verbatim including the newline that opens and closes the template literal:
// the SHA covers it, so a Bun and a Go worker run one cached script.
const cleanupLua = "\nlocal cur = redis.call('GET', KEYS[1])\nif cur then\n  local ok, parsed = pcall(cjson.decode, cur)\n  if ok and parsed and parsed.id ~= ARGV[1] then\n    return 0\n  end\nend\nredis.call('DEL', KEYS[1])\nredis.call('ZREM', KEYS[2], ARGV[2])\nif KEYS[3] ~= '' then\n  redis.call('DEL', KEYS[3])\nend\nreturn 1\n"

// Redis is the cache client's commands the lifecycle sends.
type Redis interface {
	// Get returns the value and whether the key exists.
	Get(ctx context.Context, key string) (string, bool, error)
	// SetNXEx is SET key value EX ttl NX and reports whether the key was set.
	SetNXEx(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	// SetEx is SETEX.
	SetEx(ctx context.Context, key, value string, ttl time.Duration) error
	Del(ctx context.Context, key string) error
	SMembers(ctx context.Context, key string) ([]string, error)
	SRem(ctx context.Context, key, member string) error
	// ZRangeByScore is ZRANGEBYSCORE key 0 max LIMIT 0 limit.
	ZRangeByScore(ctx context.Context, key string, max int64, limit int) ([]string, error)
	ZCard(ctx context.Context, key string) (int64, error)
	ZRem(ctx context.Context, key, member string) error
	RPush(ctx context.Context, key, value string) error
	// Cleanup runs the id-gated cleanup script and returns its reply.
	Cleanup(ctx context.Context, keys [3]string, sessionID, deviceID string) (int64, error)
}

// RueidisRedis is Redis over a rueidis client, every command bounded by
// REDIS_CACHE_COMMAND_TIMEOUT_MS.
type RueidisRedis struct {
	client  rueidis.Client
	timeout time.Duration
	cleanup *rueidis.Lua
}

// NewRueidis wraps client; the caller keeps ownership and closes it.
func NewRueidis(client rueidis.Client, timeout time.Duration) *RueidisRedis {
	return &RueidisRedis{client: client, timeout: timeout, cleanup: rueidis.NewLuaScript(cleanupLua)}
}

func (r *RueidisRedis) do(ctx context.Context, command rueidis.Completed) rueidis.RedisResult {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	return r.client.Do(ctx, command)
}

// Get implements Redis.
func (r *RueidisRedis) Get(ctx context.Context, key string) (string, bool, error) {
	value, err := r.do(ctx, r.client.B().Get().Key(key).Build()).ToString()
	if rueidis.IsRedisNil(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("redis get: %w", err)
	}
	return value, true, nil
}

// SetNXEx implements Redis.
func (r *RueidisRedis) SetNXEx(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	err := r.do(ctx, r.client.B().Set().Key(key).Value(value).Nx().ExSeconds(int64(ttl/time.Second)).Build()).Error()
	if rueidis.IsRedisNil(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("redis set: %w", err)
	}
	return true, nil
}

// SetEx implements Redis.
func (r *RueidisRedis) SetEx(ctx context.Context, key, value string, ttl time.Duration) error {
	if err := r.do(ctx, r.client.B().Setex().Key(key).Seconds(int64(ttl/time.Second)).Value(value).Build()).Error(); err != nil {
		return fmt.Errorf("redis setex: %w", err)
	}
	return nil
}

// Del implements Redis.
func (r *RueidisRedis) Del(ctx context.Context, key string) error {
	if err := r.do(ctx, r.client.B().Del().Key(key).Build()).Error(); err != nil {
		return fmt.Errorf("redis del: %w", err)
	}
	return nil
}

// SMembers implements Redis.
func (r *RueidisRedis) SMembers(ctx context.Context, key string) ([]string, error) {
	members, err := r.do(ctx, r.client.B().Smembers().Key(key).Build()).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("redis smembers: %w", err)
	}
	return members, nil
}

// SRem implements Redis.
func (r *RueidisRedis) SRem(ctx context.Context, key, member string) error {
	if err := r.do(ctx, r.client.B().Srem().Key(key).Member(member).Build()).Error(); err != nil {
		return fmt.Errorf("redis srem: %w", err)
	}
	return nil
}

// ZRangeByScore implements Redis.
func (r *RueidisRedis) ZRangeByScore(ctx context.Context, key string, max int64, limit int) ([]string, error) {
	command := r.client.B().Zrangebyscore().Key(key).Min("0").Max(strconv.FormatInt(max, 10)).Limit(0, int64(limit)).Build()
	members, err := r.do(ctx, command).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("redis zrangebyscore: %w", err)
	}
	return members, nil
}

// ZCard implements Redis.
func (r *RueidisRedis) ZCard(ctx context.Context, key string) (int64, error) {
	count, err := r.do(ctx, r.client.B().Zcard().Key(key).Build()).AsInt64()
	if err != nil {
		return 0, fmt.Errorf("redis zcard: %w", err)
	}
	return count, nil
}

// ZRem implements Redis.
func (r *RueidisRedis) ZRem(ctx context.Context, key, member string) error {
	if err := r.do(ctx, r.client.B().Zrem().Key(key).Member(member).Build()).Error(); err != nil {
		return fmt.Errorf("redis zrem: %w", err)
	}
	return nil
}

// RPush implements Redis.
func (r *RueidisRedis) RPush(ctx context.Context, key, value string) error {
	if err := r.do(ctx, r.client.B().Rpush().Key(key).Element(value).Build()).Error(); err != nil {
		return fmt.Errorf("redis rpush: %w", err)
	}
	return nil
}

// Cleanup implements Redis.
func (r *RueidisRedis) Cleanup(ctx context.Context, keys [3]string, sessionID, deviceID string) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	reply, err := r.cleanup.Exec(ctx, r.client, keys[:], []string{sessionID, deviceID}).AsInt64()
	if err != nil {
		return 0, fmt.Errorf("redis cleanup script: %w", err)
	}
	return reply, nil
}

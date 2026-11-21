package flush

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/rueidis"
)

// Redis is the handful of commands a flush sends: the cache client of
// packages/redis (ioredis there, rueidis here).
type Redis interface {
	LLen(ctx context.Context, key string) (int64, error)
	// LRange is LRANGE key start stop.
	LRange(ctx context.Context, key string, start, stop int64) ([]string, error)
	// LTrim is LTRIM key start stop.
	LTrim(ctx context.Context, key string, start, stop int64) error
	// SetNXEx is SET key value EX ttl NX, reporting whether the key was set.
	SetNXEx(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	HExists(ctx context.Context, key, field string) (bool, error)
	// EvalSHA runs a cached script and returns its integer reply.
	EvalSHA(ctx context.Context, sha string, keys, args []string) (int64, error)
	// Eval runs a script from its text, which also caches it.
	Eval(ctx context.Context, script string, keys, args []string) (int64, error)
}

// ReplyError is an error Redis itself answered with, as opposed to a broken
// connection.
type ReplyError struct{ Message string }

// Error implements error.
func (e *ReplyError) Error() string { return "redis: " + e.Message }

const noScriptPrefix = "NOSCRIPT"

func isNoScript(err error) bool {
	var reply *ReplyError
	return errors.As(err, &reply) && strings.HasPrefix(reply.Message, noScriptPrefix)
}

// Script is a Lua script sent as EVALSHA, so its text crosses the wire only
// when Redis has not cached it yet (NOSCRIPT, then EVAL), as defineRedisScript
// does. The SHA covers the exact text, which is part of the contract: a mixed
// TS/Go fleet hashes to one script.
type Script struct {
	text string
	sha  string
}

// NewScript hashes lua as sent.
func NewScript(lua string) Script {
	sum := sha1.Sum([]byte(lua))
	return Script{text: lua, sha: hex.EncodeToString(sum[:])}
}

// SHA is the hex SHA-1 of the script text.
func (s Script) SHA() string { return s.sha }

// Run executes the script and returns its integer reply.
func (s Script) Run(ctx context.Context, redis Redis, keys, args []string) (int64, error) {
	reply, err := redis.EvalSHA(ctx, s.sha, keys, args)
	if err == nil || !isNoScript(err) {
		return reply, err
	}
	return redis.Eval(ctx, s.text, keys, args)
}

// RueidisRedis adapts a rueidis client to Redis. Every command is bounded by
// timeout, REDIS_CACHE_COMMAND_TIMEOUT_MS.
type RueidisRedis struct {
	client  rueidis.Client
	timeout time.Duration
}

// NewRueidis wraps client; the caller keeps ownership and closes it.
func NewRueidis(client rueidis.Client, timeout time.Duration) *RueidisRedis {
	return &RueidisRedis{client: client, timeout: timeout}
}

func (r *RueidisRedis) do(ctx context.Context, command rueidis.Completed) rueidis.RedisResult {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	return r.client.Do(ctx, command)
}

// LLen implements Redis.
func (r *RueidisRedis) LLen(ctx context.Context, key string) (int64, error) {
	value, err := r.do(ctx, r.client.B().Llen().Key(key).Build()).AsInt64()
	return value, wrapError(err)
}

// LRange implements Redis.
func (r *RueidisRedis) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	items, err := r.do(ctx, r.client.B().Lrange().Key(key).Start(start).Stop(stop).Build()).AsStrSlice()
	return items, wrapError(err)
}

// LTrim implements Redis.
func (r *RueidisRedis) LTrim(ctx context.Context, key string, start, stop int64) error {
	return wrapError(r.do(ctx, r.client.B().Ltrim().Key(key).Start(start).Stop(stop).Build()).Error())
}

// SetNXEx implements Redis.
func (r *RueidisRedis) SetNXEx(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	err := r.do(ctx, r.client.B().Set().Key(key).Value(value).Nx().ExSeconds(int64(ttl/time.Second)).Build()).Error()
	if rueidis.IsRedisNil(err) {
		return false, nil
	}
	if err != nil {
		return false, wrapError(err)
	}
	return true, nil
}

// HExists implements Redis.
func (r *RueidisRedis) HExists(ctx context.Context, key, field string) (bool, error) {
	value, err := r.do(ctx, r.client.B().Hexists().Key(key).Field(field).Build()).AsInt64()
	return value == 1, wrapError(err)
}

// EvalSHA implements Redis.
func (r *RueidisRedis) EvalSHA(ctx context.Context, sha string, keys, args []string) (int64, error) {
	command := r.client.B().Evalsha().Sha1(sha).Numkeys(int64(len(keys))).Key(keys...).Arg(args...).Build()
	value, err := r.do(ctx, command).AsInt64()
	return value, wrapError(err)
}

// Eval implements Redis.
func (r *RueidisRedis) Eval(ctx context.Context, script string, keys, args []string) (int64, error) {
	command := r.client.B().Eval().Script(script).Numkeys(int64(len(keys))).Key(keys...).Arg(args...).Build()
	value, err := r.do(ctx, command).AsInt64()
	return value, wrapError(err)
}

func wrapError(err error) error {
	if err == nil {
		return nil
	}
	if redisErr, ok := rueidis.IsRedisErr(err); ok {
		return &ReplyError{Message: redisErr.Error()}
	}
	return fmt.Errorf("redis: %w", err)
}

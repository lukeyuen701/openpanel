package bullmq

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/redis/rueidis"
)

// Redis is the three commands the producer sends.
type Redis interface {
	// EvalSHA runs a cached script. A server without the script answers with
	// a ReplyError starting NOSCRIPT.
	EvalSHA(ctx context.Context, sha string, keys []string, args []string) (Reply, error)
	// Eval runs a script from its text, which also caches it.
	Eval(ctx context.Context, script string, keys []string, args []string) (Reply, error)
	HSet(ctx context.Context, key string, fieldValues ...string) error
}

// Reply is a script's answer: an integer or a bulk string.
type Reply struct {
	Integer   int64
	Text      string
	IsInteger bool
}

// ReplyError is an error Redis itself answered with (as opposed to a broken
// connection), which retrying cannot fix.
type ReplyError struct{ Message string }

func (e *ReplyError) Error() string { return "redis: " + e.Message }

const (
	noScriptPrefix = "NOSCRIPT"
	loadingPrefix  = "LOADING"
)

func isNoScript(err error) bool {
	var reply *ReplyError
	return errors.As(err, &reply) && strings.HasPrefix(reply.Message, noScriptPrefix)
}

// isTransient reports whether waiting and retrying can succeed: anything but
// a refusal from Redis, plus a server still loading its dataset.
func isTransient(err error) bool {
	var reply *ReplyError
	if !errors.As(err, &reply) {
		return true
	}
	return strings.HasPrefix(reply.Message, loadingPrefix)
}

// RueidisClient adapts a rueidis client to Redis.
type RueidisClient struct{ client rueidis.Client }

// NewRueidis wraps client; the caller keeps ownership and closes it.
func NewRueidis(client rueidis.Client) *RueidisClient { return &RueidisClient{client: client} }

// EvalSHA implements Redis.
func (r *RueidisClient) EvalSHA(ctx context.Context, sha string, keys, args []string) (Reply, error) {
	command := r.client.B().Evalsha().Sha1(sha).Numkeys(int64(len(keys))).Key(keys...).Arg(args...).Build()
	return replyOf(r.client.Do(ctx, command))
}

// Eval implements Redis.
func (r *RueidisClient) Eval(ctx context.Context, script string, keys, args []string) (Reply, error) {
	command := r.client.B().Eval().Script(script).Numkeys(int64(len(keys))).Key(keys...).Arg(args...).Build()
	return replyOf(r.client.Do(ctx, command))
}

// HSet implements Redis.
func (r *RueidisClient) HSet(ctx context.Context, key string, fieldValues ...string) error {
	command := r.client.B().Hset().Key(key).FieldValue()
	for i := 0; i+1 < len(fieldValues); i += 2 {
		command = command.FieldValue(fieldValues[i], fieldValues[i+1])
	}
	return wrapError(r.client.Do(ctx, command.Build()).Error())
}

func replyOf(result rueidis.RedisResult) (Reply, error) {
	message, err := result.ToMessage()
	if err != nil {
		return Reply{}, wrapError(err)
	}
	if message.IsInt64() {
		value, err := message.AsInt64()
		return Reply{Integer: value, IsInteger: true}, err
	}
	text, err := message.ToString()
	if err != nil {
		return Reply{}, fmt.Errorf("bullmq: unexpected script reply: %w", err)
	}
	return Reply{Text: text}, nil
}

func wrapError(err error) error {
	if err == nil {
		return nil
	}
	if redisErr, ok := rueidis.IsRedisErr(err); ok {
		return &ReplyError{Message: redisErr.Error()}
	}
	return err
}

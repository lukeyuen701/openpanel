package flush

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
)

// MemoryRedis is the Redis surface a flush uses, in memory, for tests: lists,
// SET NX EX with real expiry, HEXISTS and the two lock scripts by SHA. It runs
// no other Lua.
type MemoryRedis struct {
	mutex   sync.Mutex
	lists   map[string][]string
	strings map[string]string
	expires map[string]time.Time
	hashes  map[string]map[string]string
	cached  map[string]string // sha -> lua text, what SCRIPT LOAD / EVAL cached

	// failures, by command name, returned once then cleared.
	failures map[string]error
	calls    map[string]int
}

func NewMemoryRedis() *MemoryRedis {
	return &MemoryRedis{
		lists: map[string][]string{}, strings: map[string]string{}, expires: map[string]time.Time{},
		hashes: map[string]map[string]string{}, cached: map[string]string{},
		failures: map[string]error{}, calls: map[string]int{},
	}
}

func (r *MemoryRedis) enter(command string) error {
	r.calls[command]++
	if err := r.failures[command]; err != nil {
		delete(r.failures, command)
		return err
	}
	return nil
}

// FailOnce makes the next call of command (llen, lrange, ltrim, set, hexists,
// extend, release) fail with err.
func (r *MemoryRedis) FailOnce(command string, err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.failures[command] = err
}

// CallCount is how many times command was called.
func (r *MemoryRedis) CallCount(command string) int {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.calls[command]
}

func (r *MemoryRedis) expire(key string) {
	if until, ok := r.expires[key]; ok && !until.After(time.Now()) {
		delete(r.strings, key)
		delete(r.expires, key)
	}
}

// Push appends to a list, as RPUSH.
func (r *MemoryRedis) Push(key string, values ...string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.lists[key] = append(r.lists[key], values...)
}

// List is a copy of a list.
func (r *MemoryRedis) List(key string) []string {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return append([]string(nil), r.lists[key]...)
}

// Get reads a string key, honouring expiry.
func (r *MemoryRedis) Get(key string) (string, bool) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.expire(key)
	value, ok := r.strings[key]
	return value, ok
}

// Set stores a string key with no expiry.
func (r *MemoryRedis) Set(key, value string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.strings[key] = value
}

// TTL is the time left on key, or -1.
func (r *MemoryRedis) TTL(key string) time.Duration {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.expire(key)
	if until, ok := r.expires[key]; ok {
		return time.Until(until)
	}
	return -1
}

func (r *MemoryRedis) LLen(_ context.Context, key string) (int64, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if err := r.enter("llen"); err != nil {
		return 0, err
	}
	return int64(len(r.lists[key])), nil
}

func (r *MemoryRedis) LRange(_ context.Context, key string, start, stop int64) ([]string, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if err := r.enter("lrange"); err != nil {
		return nil, err
	}
	list := r.lists[key]
	if stop < 0 || stop >= int64(len(list)) {
		stop = int64(len(list)) - 1
	}
	if start > stop || start >= int64(len(list)) {
		return []string{}, nil
	}
	return append([]string(nil), list[start:stop+1]...), nil
}

func (r *MemoryRedis) LTrim(_ context.Context, key string, start, stop int64) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if err := r.enter("ltrim"); err != nil {
		return err
	}
	list := r.lists[key]
	if stop < 0 || stop >= int64(len(list)) {
		stop = int64(len(list)) - 1
	}
	if start > stop || start >= int64(len(list)) {
		r.lists[key] = nil
		return nil
	}
	r.lists[key] = append([]string(nil), list[start:stop+1]...)
	return nil
}

func (r *MemoryRedis) SetNXEx(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if err := r.enter("set"); err != nil {
		return false, err
	}
	r.expire(key)
	if _, exists := r.strings[key]; exists {
		return false, nil
	}
	r.strings[key] = value
	r.expires[key] = time.Now().Add(ttl)
	return true, nil
}

func (r *MemoryRedis) HExists(_ context.Context, key, field string) (bool, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if err := r.enter("hexists"); err != nil {
		return false, err
	}
	_, ok := r.hashes[key][field]
	return ok, nil
}

func (r *MemoryRedis) EvalSHA(ctx context.Context, sha string, keys, args []string) (int64, error) {
	r.mutex.Lock()
	text, cached := r.cached[sha]
	r.mutex.Unlock()
	if !cached {
		return 0, &ReplyError{Message: "NOSCRIPT No matching script. Please use EVAL."}
	}
	return r.run(text, keys, args)
}

func (r *MemoryRedis) Eval(_ context.Context, script string, keys, args []string) (int64, error) {
	r.mutex.Lock()
	r.cached[NewScript(script).SHA()] = script
	r.mutex.Unlock()
	return r.run(script, keys, args)
}

// run executes the two lock scripts' semantics.
func (r *MemoryRedis) run(text string, keys, args []string) (int64, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	switch {
	case strings.Contains(text, `"del"`):
		if err := r.enter("release"); err != nil {
			return 0, err
		}
		r.expire(keys[0])
		if r.strings[keys[0]] != args[0] {
			return 0, nil
		}
		delete(r.strings, keys[0])
		delete(r.expires, keys[0])
		return 1, nil
	case strings.Contains(text, `"expire"`):
		if err := r.enter("extend"); err != nil {
			return 0, err
		}
		r.expire(keys[0])
		if r.strings[keys[0]] != args[0] {
			return 0, nil
		}
		seconds := int64(0)
		for _, c := range args[1] {
			seconds = seconds*10 + int64(c-'0')
		}
		r.expires[keys[0]] = time.Now().Add(time.Duration(seconds) * time.Second)
		return 1, nil
	}
	return 0, errors.New("MemoryRedis: unknown script")
}

// HSet stores a hash field, as the cron queue's meta hash is for CronPauseCheck.
func (r *MemoryRedis) HSet(key, field, value string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.hashes[key] == nil {
		r.hashes[key] = map[string]string{}
	}
	r.hashes[key][field] = value
}

// HDel removes a hash field.
func (r *MemoryRedis) HDel(key, field string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	delete(r.hashes[key], field)
}

// HasScript reports whether a script with this SHA is cached.
func (r *MemoryRedis) HasScript(sha string) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	_, ok := r.cached[sha]
	return ok
}

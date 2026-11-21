package bullmq

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func readGolden(t *testing.T, name string, into any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, into); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}

func TestPackMatchesMsgpackr(t *testing.T) {
	var golden map[string]string
	readGolden(t, "msgpack-vectors.json", &golden)

	ints := func(n int) []any {
		out := make([]any, n)
		for i := range out {
			out[i] = i
		}
		return out
	}
	var members packedMap
	for i := 0; i < 16; i++ {
		members = append(members, member{"k" + strconv.Itoa(i), i})
	}
	values := map[string]any{
		"nil":         nil,
		"true":        true,
		"false":       false,
		"zero":        0,
		"fixintMax":   127,
		"uint8":       128,
		"uint8Max":    255,
		"uint16":      256,
		"uint16Max":   65535,
		"uint32":      65536,
		"uint32Max":   4294967295,
		"aboveUint32": 4294967296,
		"timestamp":   int64(1767225600000),
		"fraction":    1.5,
		"emptyString": "",
		"fixstrMax":   strings.Repeat("a", 31),
		"str8":        strings.Repeat("a", 32),
		"str8Max":     strings.Repeat("a", 255),
		"str16":       strings.Repeat("a", 256),
		"unicode":     "héllo ☃ 😀",
		"emptyArray":  []any{},
		"array":       []any{1, "a", nil},
		"array16":     ints(16),
		"emptyMap":    packedMap{},
		"map":         packedMap{{"a", 1}, {"b", []any{true, nil}}},
		"map16":       members,
	}
	for name, want := range golden {
		value, ok := values[name]
		if !ok {
			t.Errorf("vector %q has no Go value", name)
			continue
		}
		if got := hex.EncodeToString(pack(value)); got != want {
			t.Errorf("%s: got %s, want %s", name, got, want)
		}
	}
	if len(values) != len(golden) {
		t.Errorf("Go has %d vectors, golden %d", len(values), len(golden))
	}
}

func TestQueueKey(t *testing.T) {
	cases := []struct {
		namespace string
		cluster   bool
		want      string
	}{
		{"", false, "sessions"},
		{"wparity", false, "sessions-wparity"},
		{"", true, "{sessions}"},
		{"wparity", true, "{sessions-wparity}"},
	}
	for _, c := range cases {
		if got := QueueKey("sessions", c.namespace, c.cluster); got != c.want {
			t.Errorf("QueueKey(%q, %v) = %q, want %q", c.namespace, c.cluster, got, c.want)
		}
	}
}

func TestCustomIDRules(t *testing.T) {
	refused := []string{"12", "0", "-5", "7", "123456789012", "a:b", "a:b:c:d", "sessionEnd:v2:a:b"}
	allowed := []string{"sessionEnd:v2:sess1", "012", "-0", "1e3", "abc", "1.5", "a:b:c", "123456789012345678901", " 1", "1 "}
	for _, id := range refused {
		if refusesCustomID(id) == nil {
			t.Errorf("%q should be refused", id)
		}
	}
	for _, id := range allowed {
		if err := refusesCustomID(id); err != nil {
			t.Errorf("%q should be allowed: %v", id, err)
		}
	}
}

func TestEnvelope(t *testing.T) {
	if got := string(Envelope([]byte(`{"a":1}`), "req-1")); got != `{"payload":{"a":1},"meta":{"requestId":"req-1"}}` {
		t.Errorf("got %s", got)
	}
	if got := string(Envelope([]byte("null"), "")); got != `{"payload":null,"meta":{}}` {
		t.Errorf("got %s", got)
	}
	if got := string(Envelope([]byte("1"), `a"b`)); got != `{"payload":1,"meta":{"requestId":"a\"b"}}` {
		t.Errorf("got %s", got)
	}
}

type goldenCase struct {
	Namespace   string         `json:"namespace"`
	Cluster     bool           `json:"cluster"`
	RequestID   string         `json:"requestId"`
	Queue       string         `json:"queue"`
	Job         string         `json:"job"`
	PayloadJSON string         `json:"payloadJson"`
	Options     *goldenOptions `json:"options"`
}

type goldenOptions struct {
	JobID    string `json:"jobId"`
	Attempts int    `json:"attempts"`
	Backoff  struct {
		Type  string `json:"type"`
		Delay int    `json:"delay"`
	} `json:"backoff"`
}

type goldenCases struct {
	NowMs int64        `json:"nowMs"`
	Cases []goldenCase `json:"cases"`
	IDs   []string     `json:"ids"`
}

func (c goldenCase) addOptions() AddOptions {
	if c.Options == nil {
		return AddOptions{}
	}
	return AddOptions{
		JobID:       c.Options.JobID,
		Attempts:    c.Options.Attempts,
		AttemptsSet: c.Options.Attempts != 0,
		Backoff:     Backoff{Type: c.Options.Backoff.Type, DelayMs: c.Options.Backoff.Delay},
	}
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }
func (c fixedClock) Sleep(ctx context.Context, _ time.Duration) error {
	return ctx.Err()
}

type producerKey struct {
	namespace string
	cluster   bool
}

// replay adds every golden case through producers built on redis.
func replay(t *testing.T, redis Redis, golden goldenCases) []string {
	t.Helper()
	producers := map[producerKey]*Producer{}
	var ids []string
	for _, c := range golden.Cases {
		key := producerKey{c.Namespace, c.Cluster}
		producer, ok := producers[key]
		if !ok {
			var err error
			producer, err = NewProducer(Config{
				Redis:     redis,
				Queues:    []Queue{SessionsQueue(), NotificationQueue()},
				Namespace: c.Namespace,
				Cluster:   c.Cluster,
				Clock:     fixedClock{time.UnixMilli(golden.NowMs)},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := producer.Init(context.Background()); err != nil {
				t.Fatal(err)
			}
			producers[key] = producer
		}
		data := Envelope([]byte(c.PayloadJSON), c.RequestID)
		id, err := producer.Add(context.Background(), c.Queue, c.Job, data, c.addOptions())
		if err != nil {
			t.Fatalf("add %s/%s: %v", c.Queue, c.Job, err)
		}
		ids = append(ids, id)
	}
	return ids
}

type recordedScript struct {
	sha  string
	keys []string
	args []string
}

// scriptRedis records script calls and answers like a Redis that knows the
// script, with the job id the first argument asks for or a counter.
type scriptRedis struct {
	calls     []recordedScript
	hsets     [][]string
	noScript  int
	failures  []error
	evalCalls int
	counter   int64
}

func (r *scriptRedis) next() (Reply, error) {
	if len(r.failures) > 0 {
		err := r.failures[0]
		r.failures = r.failures[1:]
		return Reply{}, err
	}
	r.counter++
	return Reply{Integer: r.counter, IsInteger: true}, nil
}

func (r *scriptRedis) EvalSHA(_ context.Context, sha string, keys, args []string) (Reply, error) {
	if r.noScript > 0 {
		r.noScript--
		return Reply{}, &ReplyError{Message: "NOSCRIPT No matching script. Please use EVAL."}
	}
	r.calls = append(r.calls, recordedScript{sha, keys, args})
	return r.next()
}

func (r *scriptRedis) Eval(_ context.Context, script string, keys, args []string) (Reply, error) {
	r.evalCalls++
	if script != addStandardJobScript {
		return Reply{}, errors.New("unexpected script")
	}
	r.calls = append(r.calls, recordedScript{scriptSHA(script), keys, args})
	return r.next()
}

func (r *scriptRedis) HSet(_ context.Context, key string, fieldValues ...string) error {
	r.hsets = append(r.hsets, append([]string{key}, fieldValues...))
	return nil
}

type capturedCommand struct {
	Command string   `json:"command"`
	Args    []string `json:"args"`
}

func TestArgumentsAreByteIdenticalToBullMQ(t *testing.T) {
	var golden goldenCases
	readGolden(t, "cases.json", &golden)
	var captured []capturedCommand
	readGolden(t, "commands.json", &captured)

	var want []recordedScript
	for _, c := range captured {
		if c.Command != "evalsha" {
			continue
		}
		decoded := make([]string, len(c.Args))
		for i, arg := range c.Args {
			raw, err := base64.StdEncoding.DecodeString(arg)
			if err != nil {
				t.Fatal(err)
			}
			decoded[i] = string(raw)
		}
		const keyCount = 9
		want = append(want, recordedScript{sha: decoded[0], keys: decoded[2 : 2+keyCount], args: decoded[2+keyCount:]})
	}

	redis := &scriptRedis{}
	replay(t, redis, golden)
	if len(redis.calls) != len(want) {
		t.Fatalf("sent %d scripts, BullMQ sent %d", len(redis.calls), len(want))
	}
	for i, got := range redis.calls {
		w := want[i]
		if got.sha != w.sha {
			t.Errorf("call %d: sha %s, want %s", i, got.sha, w.sha)
		}
		if !reflect.DeepEqual(got.keys, w.keys) {
			t.Errorf("call %d: keys %q, want %q", i, got.keys, w.keys)
		}
		for j := range w.args {
			if got.args[j] != w.args[j] {
				t.Errorf("call %d: ARGV[%d] differs:\n got %x\nwant %x", i, j+1, got.args[j], w.args[j])
			}
		}
	}
}

func TestScriptManifest(t *testing.T) {
	var manifest struct {
		Bullmq  string `json:"bullmq"`
		Scripts map[string]struct {
			Keys int    `json:"keys"`
			Sha1 string `json:"sha1"`
		} `json:"scripts"`
	}
	readGolden(t, filepath.Join("..", "scripts", "manifest.json"), &manifest)
	entry := manifest.Scripts["addStandardJob"]
	if got := scriptSHA(addStandardJobScript); got != entry.Sha1 {
		t.Errorf("embedded script sha %s, manifest %s", got, entry.Sha1)
	}
	if entry.Keys != standardJobKeys {
		t.Errorf("keys %d, want %d", entry.Keys, standardJobKeys)
	}
	if "bullmq:"+manifest.Bullmq != metaVersion {
		t.Errorf("meta version %s, vendored bullmq %s", metaVersion, manifest.Bullmq)
	}
}

func newTestProducer(t *testing.T, redis Redis, clock Clock, retry func(int) time.Duration) *Producer {
	t.Helper()
	producer, err := NewProducer(Config{Redis: redis, Queues: []Queue{SessionsQueue()}, Clock: clock, RetryDelay: retry})
	if err != nil {
		t.Fatal(err)
	}
	return producer
}

func TestNoScriptFallsBackToEval(t *testing.T) {
	redis := &scriptRedis{noScript: 1}
	producer := newTestProducer(t, redis, nil, nil)
	id, err := producer.Add(context.Background(), "sessions", SessionJob, []byte("{}"), AddOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if id != "1" || redis.evalCalls != 1 {
		t.Errorf("id %q, eval calls %d", id, redis.evalCalls)
	}
}

func TestMetaIsWrittenOncePerQueue(t *testing.T) {
	redis := &scriptRedis{}
	producer := newTestProducer(t, redis, nil, nil)
	for range 3 {
		if _, err := producer.Add(context.Background(), "sessions", SessionJob, []byte("{}"), AddOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	want := [][]string{{"bull:sessions:meta", "opts.maxLenEvents", "10000", "version", "bullmq:6.7.8"}}
	if !reflect.DeepEqual(redis.hsets, want) {
		t.Errorf("hsets %v", redis.hsets)
	}
}

func TestAddWaitsOutAnOutage(t *testing.T) {
	outage := errors.New("connection refused")
	redis := &scriptRedis{failures: []error{outage, outage, outage}}
	var waits []time.Duration
	clock := recordingClock{waits: &waits}
	producer := newTestProducer(t, redis, clock, func(attempt int) time.Duration { return time.Duration(attempt) * time.Second })
	id, err := producer.Add(context.Background(), "sessions", SessionJob, []byte("{}"), AddOptions{})
	if err != nil || id != "1" {
		t.Fatalf("id %q err %v", id, err)
	}
	if want := []time.Duration{time.Second, 2 * time.Second, 3 * time.Second}; !reflect.DeepEqual(waits, want) {
		t.Errorf("waits %v, want %v", waits, want)
	}
	// Every retry sent the same arguments, timestamp included.
	for i := 1; i < len(redis.calls); i++ {
		if !reflect.DeepEqual(redis.calls[i].args, redis.calls[0].args) {
			t.Errorf("retry %d changed its arguments", i)
		}
	}
}

func TestAddStopsWhenContextEnds(t *testing.T) {
	redis := &scriptRedis{failures: []error{errors.New("down"), errors.New("down"), errors.New("down")}}
	ctx, cancel := context.WithCancel(context.Background())
	producer := newTestProducer(t, redis, cancelingClock{cancel: cancel}, nil)
	_, err := producer.Add(ctx, "sessions", SessionJob, []byte("{}"), AddOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

func TestRedisRefusalIsNotRetried(t *testing.T) {
	redis := &scriptRedis{failures: []error{&ReplyError{Message: "OOM command not allowed"}}}
	producer := newTestProducer(t, redis, nil, nil)
	_, err := producer.Add(context.Background(), "sessions", SessionJob, []byte("{}"), AddOptions{})
	var refusal *ReplyError
	if !errors.As(err, &refusal) {
		t.Fatalf("err = %v", err)
	}
}

func TestAddRefusals(t *testing.T) {
	producer := newTestProducer(t, &scriptRedis{}, nil, nil)
	if _, err := producer.Add(context.Background(), "nope", SessionJob, nil, AddOptions{}); err == nil {
		t.Error("unknown queue accepted")
	}
	if _, err := producer.Add(context.Background(), "sessions", SessionJob, nil, AddOptions{JobID: "12"}); !errors.Is(err, errCustomIDInteger) {
		t.Errorf("integer id: %v", err)
	}
	if _, err := producer.Add(context.Background(), "sessions", SessionJob, nil, AddOptions{JobID: "a:b"}); !errors.Is(err, errCustomIDColon) {
		t.Errorf("colon id: %v", err)
	}
}

type recordingClock struct{ waits *[]time.Duration }

func (recordingClock) Now() time.Time { return time.UnixMilli(1) }
func (c recordingClock) Sleep(_ context.Context, d time.Duration) error {
	*c.waits = append(*c.waits, d)
	return nil
}

type cancelingClock struct{ cancel context.CancelFunc }

func (cancelingClock) Now() time.Time { return time.UnixMilli(1) }
func (c cancelingClock) Sleep(ctx context.Context, _ time.Duration) error {
	c.cancel()
	return ctx.Err()
}

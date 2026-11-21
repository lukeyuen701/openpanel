package bullmq

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestBackoffDelays(t *testing.T) {
	cases := []struct {
		name    string
		options string
		attempt int
		want    int64
	}{
		{"unset", `{"attempts":3}`, 1, 0},
		{"number is fixed", `{"attempts":3,"backoff":250}`, 2, 250},
		{"fixed", `{"attempts":3,"backoff":{"type":"fixed","delay":100}}`, 2, 100},
		{"exponential first", `{"attempts":4,"backoff":{"type":"exponential","delay":100}}`, 1, 100},
		{"exponential third", `{"attempts":4,"backoff":{"type":"exponential","delay":100}}`, 3, 400},
		{"exponential rounds halves up", `{"attempts":4,"backoff":{"type":"exponential","delay":1.5}}`, 1, 2},
	}
	for _, c := range cases {
		options, err := parseOptions(c.options)
		if err != nil {
			t.Fatal(err)
		}
		got, err := options.Backoff.retryDelay(c.attempt)
		if err != nil || got != c.want {
			t.Errorf("%s: delay %d, %v; want %d", c.name, got, err, c.want)
		}
	}
	options, _ := parseOptions(`{"backoff":{"type":"custom"}}`)
	if _, err := options.Backoff.retryDelay(1); !errors.Is(err, errUnknownBackoff) {
		t.Errorf("custom strategy: %v", err)
	}
	jittered, _ := parseOptions(`{"backoff":{"type":"fixed","delay":100,"jitter":0.5}}`)
	for range 50 {
		if got, _ := jittered.Backoff.retryDelay(1); got < 50 || got >= 100 {
			t.Fatalf("jittered delay %d outside [50,100)", got)
		}
	}
}

func TestShouldRetry(t *testing.T) {
	failure := errors.New("boom")
	cases := []struct {
		name      string
		job       Job
		err       error
		wantRetry bool
		wantDelay int64
	}{
		{"last attempt", Job{AttemptsMade: 2, Options: JobOptions{Attempts: 3}}, failure, false, 0},
		{"no attempts configured", Job{Options: JobOptions{}}, failure, false, 0},
		{"attempt left", Job{AttemptsMade: 1, Options: JobOptions{Attempts: 3}}, failure, true, 0},
		{"unrecoverable", Job{Options: JobOptions{Attempts: 3}}, Unrecoverable(failure), false, 0},
		{"wrapped unrecoverable", Job{Options: JobOptions{Attempts: 3}}, errors.Join(errors.New("ctx"), Unrecoverable(failure)), false, 0},
		{"backoff delay", Job{AttemptsMade: 1, Options: JobOptions{Attempts: 3, Backoff: backoffSpec{set: true, kind: "fixed", delay: 70}}}, failure, true, 70},
		{"strategy gives up", Job{Options: JobOptions{Attempts: 3, Backoff: backoffSpec{set: true, kind: "fixed", delay: -1}}}, failure, false, 0},
	}
	for _, c := range cases {
		retry, delay, err := c.job.shouldRetry(c.err)
		if err != nil || retry != c.wantRetry || delay != c.wantDelay {
			t.Errorf("%s: retry %v delay %d err %v; want %v %d", c.name, retry, delay, err, c.wantRetry, c.wantDelay)
		}
	}
}

func TestKeepJobs(t *testing.T) {
	cases := map[string]string{
		"":                           `[{count -1}]`,
		"null":                       `[{count -1}]`,
		"true":                       `[{count 0}]`,
		"false":                      `[{count -1}]`,
		"25":                         `[{count 25}]`,
		`{"age":86400,"count":1000}`: `[{age 86400} {count 1000}]`,
		`{"age":60}`:                 `[{age 60}]`,
	}
	for input, want := range cases {
		if got := keepJobs([]byte(input)); sprintMap(got) != want {
			t.Errorf("keepJobs(%q) = %s, want %s", input, sprintMap(got), want)
		}
	}
}

func sprintMap(m packedMap) string {
	out := "["
	for i, entry := range m {
		if i > 0 {
			out += " "
		}
		out += "{" + entry.key + " " + strconv.FormatFloat(toFloat(entry.value), 'f', -1, 64) + "}"
	}
	return out + "]"
}

func toFloat(v any) float64 {
	switch n := v.(type) {
	case int:
		return float64(n)
	case float64:
		return n
	}
	return 0
}

func TestJobFromHash(t *testing.T) {
	fields := []Reply{}
	for _, text := range []string{
		"name", "session", "data", `{"payload":1}`, "opts", `{"attempts":3,"backoff":{"type":"fixed","delay":5}}`,
		"timestamp", "1700000000000", "atm", "1", "ats", "2", "stc", "1", "processedOn", "1700000000500",
		"stacktrace", `["Error: a"]`, "defa", "job stalled more than allowable limit",
	} {
		fields = append(fields, Reply{Text: text})
	}
	job, err := jobFromHash(fields, "7")
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != "7" || job.Name != "session" || string(job.Data) != `{"payload":1}` || job.Timestamp != 1700000000000 ||
		job.AttemptsMade != 1 || job.AttemptsStarted != 2 || job.StalledCounter != 1 || job.Options.Attempts != 3 ||
		len(job.stacktrace) != 1 || job.deferredFailure == "" {
		t.Errorf("job %+v", job)
	}
	empty, _ := jobFromHash([]Reply{{Text: "name"}, {Text: "x"}}, "1")
	if string(empty.Data) != "{}" {
		t.Errorf("missing data should read as {}, got %q", empty.Data)
	}
}

func TestWorkerOptionsDefaults(t *testing.T) {
	opts, err := WorkerOptions{}.withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if opts.Concurrency != 1 || opts.LockDuration != 30*time.Second || opts.LockRenewTime != 15*time.Second ||
		opts.StalledInterval != 30*time.Second || opts.MaxStalledCount != 1 || opts.DrainDelay != 5*time.Second ||
		opts.RunRetryDelay != 15*time.Second {
		t.Errorf("defaults %+v", opts)
	}
	zero, err := WorkerOptions{MaxStalledCountSet: true}.withDefaults()
	if err != nil || zero.MaxStalledCount != 0 {
		t.Errorf("explicit zero maxStalledCount: %+v %v", zero, err)
	}
	for _, bad := range []WorkerOptions{{Concurrency: -1}, {MaxStalledCount: -1, MaxStalledCountSet: true}, {StalledInterval: -1}} {
		if _, err := bad.withDefaults(); err == nil {
			t.Errorf("%+v should be refused", bad)
		}
	}
}

// fakeQueue answers the worker's scripts from a script of replies, recording
// every call: just enough Redis to pin the argument layouts.
type fakeQueue struct {
	mu      sync.Mutex
	calls   []recordedScript
	replies map[string][]Reply
	blocked chan struct{}
}

func (f *fakeQueue) EvalSHA(_ context.Context, sha string, keys, args []string) (Reply, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, recordedScript{sha, keys, args})
	queue := f.replies[sha]
	if len(queue) == 0 {
		return Reply{Integer: 0, IsInteger: true}, nil
	}
	f.replies[sha] = queue[1:]
	return queue[0], nil
}

func (f *fakeQueue) Eval(context.Context, string, []string, []string) (Reply, error) {
	return Reply{}, errors.New("unexpected EVAL")
}

func (f *fakeQueue) HSet(context.Context, string, ...string) error { return nil }

func (f *fakeQueue) BZPopMin(ctx context.Context, _ string, _ time.Duration) (int64, bool, error) {
	<-ctx.Done()
	return 0, false, ctx.Err()
}

func jobReply(id string, opts string) Reply {
	fields := []Reply{{Text: "name"}, {Text: "session"}, {Text: "data"}, {Text: "{}"}, {Text: "opts"}, {Text: opts}, {Text: "processedOn"}, {Text: "1"}}
	return Reply{IsArray: true, Array: []Reply{
		{IsArray: true, Array: fields}, {Text: id}, {IsInteger: true}, {IsInteger: true},
	}}
}

func runOnce(t *testing.T, replies map[string][]Reply, handler Handler, opts WorkerOptions) *fakeQueue {
	t.Helper()
	fake := &fakeQueue{replies: replies}
	opts.SkipStalledCheck = true
	opts.OnError = func(err error) { t.Errorf("worker error: %v", err) }
	worker, err := NewWorker(fake, "sessions", handler, opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- worker.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		fake.mu.Lock()
		finished := len(fake.calls) >= 2
		fake.mu.Unlock()
		if finished {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	return fake
}

func TestWorkerCompletesWithTheScriptsLayout(t *testing.T) {
	replies := map[string][]Reply{moveToActiveScript.sha: {jobReply("9", `{"attempts":1,"removeOnComplete":true}`)}}
	fake := runOnce(t, replies, func(context.Context, Job) error { return nil }, WorkerOptions{Namespace: "ns"})
	if len(fake.calls) < 2 || fake.calls[0].sha != moveToActiveScript.sha || fake.calls[1].sha != moveToFinishedScript.sha {
		t.Fatalf("calls %+v", fake.calls)
	}
	move := fake.calls[1]
	if len(move.keys) != 14 || move.keys[10] != "bull:sessions-ns:completed" || move.keys[11] != "bull:sessions-ns:9" ||
		move.keys[12] != "bull:sessions-ns:metrics:completed" || move.keys[13] != "bull:sessions-ns:marker" {
		t.Errorf("moveToFinished keys %v", move.keys)
	}
	if move.args[0] != "9" || move.args[2] != "returnvalue" || move.args[3] != "null" || move.args[4] != "completed" ||
		move.args[5] != "1" || move.args[6] != "bull:sessions-ns:" || move.args[8] != "" {
		t.Errorf("moveToFinished args %q", move.args)
	}
	if len(fake.calls[0].keys) != 11 || fake.calls[0].keys[10] != "bull:sessions-ns:marker" {
		t.Errorf("moveToActive keys %v", fake.calls[0].keys)
	}
}

func TestWorkerRoutesFailures(t *testing.T) {
	cases := []struct {
		name    string
		opts    string
		err     error
		wantSHA string
	}{
		{"retry now", `{"attempts":2}`, errors.New("x"), retryJobScript.sha},
		{"retry later", `{"attempts":2,"backoff":50}`, errors.New("x"), moveToDelayedScript.sha},
		{"last attempt", `{"attempts":1}`, errors.New("x"), moveToFinishedScript.sha},
		{"unrecoverable", `{"attempts":5}`, Unrecoverable(errors.New("x")), moveToFinishedScript.sha},
	}
	for _, c := range cases {
		replies := map[string][]Reply{moveToActiveScript.sha: {jobReply("3", c.opts)}}
		fake := runOnce(t, replies, func(context.Context, Job) error { return c.err }, WorkerOptions{})
		if len(fake.calls) < 2 || fake.calls[1].sha != c.wantSHA {
			t.Errorf("%s: calls %+v", c.name, fake.calls)
		}
	}
}

func TestWorkerPanicFailsTheJob(t *testing.T) {
	replies := map[string][]Reply{moveToActiveScript.sha: {jobReply("3", `{"attempts":1}`)}}
	fake := runOnce(t, replies, func(context.Context, Job) error { panic("bad") }, WorkerOptions{})
	if len(fake.calls) < 2 || fake.calls[1].args[4] != "failed" || fake.calls[1].args[3] != "panic: bad" {
		t.Errorf("calls %+v", fake.calls)
	}
}

func TestScriptCodesAreErrors(t *testing.T) {
	var err error = &ScriptError{Command: "moveToFinished", JobID: "1", Code: codeLockMismatch}
	var scriptErr *ScriptError
	if !errors.As(err, &scriptErr) || scriptErr.Error() == "" {
		t.Error("ScriptError should unwrap")
	}
}

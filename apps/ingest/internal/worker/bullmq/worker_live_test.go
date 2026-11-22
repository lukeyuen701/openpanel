package bullmq

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/redis/rueidis"
)

// The interoperability tests need a live Redis (WPARITY_REDIS_URL, database
// 14) and Bun with the repo's node_modules (WPARITY_BUN, else bun on PATH):
// the TypeScript side is the real BullMQ 6.7.8, parity/bullmq-consumer.ts.

const (
	liveWait         = 30 * time.Second
	livePoll         = 100 * time.Millisecond
	childEnv         = "WPARITY_BULLMQ_CHILD"
	childLockMs      = 1500
	childStalledMs   = 500
	sharedJobs       = 300
	sharedConcurrent = 5
)

type liveEnv struct {
	t      *testing.T
	client rueidis.Client
	redis  *RueidisClient
	host   string
	port   int
	bun    string
}

func newLiveEnv(t *testing.T, needBun bool) *liveEnv {
	t.Helper()
	client := liveClient(t)
	raw := os.Getenv("WPARITY_REDIS_URL")
	parsed, err := url.Parse(raw)
	check(t, err)
	port, err := strconv.Atoi(parsed.Port())
	if err != nil {
		port = 6379
	}
	env := &liveEnv{t: t, client: client, redis: NewRueidis(client), host: parsed.Hostname(), port: port}
	if needBun {
		env.bun = os.Getenv("WPARITY_BUN")
		if env.bun == "" {
			env.bun, _ = exec.LookPath("bun")
		}
		if env.bun == "" {
			t.Skip("set WPARITY_BUN (or put bun on PATH) to run the TypeScript side")
		}
	}
	return env
}

// namespace returns a QUEUE_NAMESPACE no other test shares and removes every
// key of it (and of the counters tagged with it) when the test ends.
func (e *liveEnv) namespace(label string) string {
	e.t.Helper()
	raw := make([]byte, 4)
	_, _ = rand.Read(raw)
	ns := "wparity-" + label + "-" + hex.EncodeToString(raw)
	e.t.Cleanup(func() {
		for _, pattern := range []string{"bull:*" + ns + "*", "wparity:*:" + ns + "*"} {
			e.deleteMatching(pattern)
		}
	})
	return ns
}

func (e *liveEnv) deleteMatching(pattern string) {
	ctx := context.Background()
	keys, err := e.client.Do(ctx, e.client.B().Keys().Pattern(pattern).Build()).AsStrSlice()
	if err != nil || len(keys) == 0 {
		return
	}
	e.client.Do(ctx, e.client.B().Del().Key(keys...).Build())
}

func (e *liveEnv) config(queue, tag string, extra map[string]any) string {
	cfg := map[string]any{"host": e.host, "port": e.port, "db": liveDB, "queue": queue, "tag": tag}
	for key, value := range extra {
		cfg[key] = value
	}
	raw, err := json.Marshal(cfg)
	check(e.t, err)
	return string(raw)
}

func (e *liveEnv) tsScript() []string {
	return []string{"../../../parity/bullmq-consumer.ts"}
}

// produce adds jobs with the real BullMQ Queue.add.
func (e *liveEnv) produce(queue, tag string, jobs []map[string]any) {
	e.t.Helper()
	args := append(e.tsScript(), "produce", e.config(queue, tag, map[string]any{"jobs": jobs}))
	output, err := exec.Command(e.bun, args...).CombinedOutput()
	if err != nil {
		e.t.Fatalf("ts produce: %v\n%s", err, output)
	}
}

type tsWorker struct {
	cmd  *exec.Cmd
	done chan struct{}
}

// startTSWorker runs a BullMQ Worker in Bun until stop.
func (e *liveEnv) startTSWorker(queue, tag string, extra map[string]any) *tsWorker {
	e.t.Helper()
	args := append(e.tsScript(), "work", e.config(queue, tag, extra))
	cmd := exec.Command(e.bun, args...)
	stdout, err := cmd.StdoutPipe()
	check(e.t, err)
	cmd.Stderr = os.Stderr
	check(e.t, cmd.Start())
	worker := &tsWorker{cmd: cmd, done: make(chan struct{})}
	ready := make(chan struct{})
	go func() {
		buffer := make([]byte, 64)
		signalled := false
		for {
			n, err := stdout.Read(buffer)
			if n > 0 && !signalled && strings.Contains(string(buffer[:n]), "ready") {
				signalled = true
				close(ready)
			}
			if err != nil {
				break
			}
		}
		_ = cmd.Wait()
		close(worker.done)
	}()
	select {
	case <-ready:
	case <-time.After(liveWait):
		_ = cmd.Process.Kill()
		e.t.Fatal("ts worker did not become ready")
	}
	e.t.Cleanup(func() { worker.stop() })
	return worker
}

func (w *tsWorker) stop() {
	_ = w.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-w.done:
	case <-time.After(liveWait):
		_ = w.cmd.Process.Kill()
		<-w.done
	}
}

func (e *liveEnv) do(cmd func(b rueidis.Builder) rueidis.Completed) rueidis.RedisResult {
	return e.client.Do(context.Background(), cmd(e.client.B()))
}

func (e *liveEnv) listLen(key string) int64 {
	n, _ := e.do(func(b rueidis.Builder) rueidis.Completed { return b.Llen().Key(key).Build() }).AsInt64()
	return n
}

func (e *liveEnv) zsetLen(key string) int64 {
	n, _ := e.do(func(b rueidis.Builder) rueidis.Completed { return b.Zcard().Key(key).Build() }).AsInt64()
	return n
}

func (e *liveEnv) hash(key string) map[string]string {
	h, _ := e.do(func(b rueidis.Builder) rueidis.Completed { return b.Hgetall().Key(key).Build() }).AsStrMap()
	return h
}

func (e *liveEnv) waitFor(what string, condition func() bool) {
	e.t.Helper()
	deadline := time.Now().Add(liveWait)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(livePoll)
	}
	e.t.Fatalf("timed out waiting for %s", what)
}

// settled is true when the queue holds no job that is waiting, running or
// delayed, twice in a row.
func (e *liveEnv) waitSettled(queue string) {
	e.t.Helper()
	base := keyPrefix + ":" + queue + ":"
	quiet := 0
	e.waitFor("queue "+queue+" to settle", func() bool {
		busy := e.listLen(base+"wait") + e.listLen(base+"active") + e.zsetLen(base+"delayed") + e.zsetLen(base+"prioritized")
		if busy == 0 {
			quiet++
		} else {
			quiet = 0
		}
		return quiet >= 3
	})
}

// goWorker runs a Go Worker until stop.
type goWorker struct {
	cancel context.CancelFunc
	done   chan struct{}
	errs   chan error
}

func (e *liveEnv) startGoWorker(queue string, handler Handler, opts WorkerOptions) *goWorker {
	e.t.Helper()
	errs := make(chan error, 64)
	opts.OnError = func(err error) {
		select {
		case errs <- err:
		default:
		}
	}
	worker, err := NewWorker(e.redis, queue, handler, opts)
	check(e.t, err)
	ctx, cancel := context.WithCancel(context.Background())
	running := &goWorker{cancel: cancel, done: make(chan struct{}), errs: errs}
	go func() {
		defer close(running.done)
		check(e.t, worker.Run(ctx))
	}()
	e.t.Cleanup(running.stop)
	return running
}

func (w *goWorker) stop() {
	w.cancel()
	<-w.done
}

func (w *goWorker) drainErrors() []error {
	var out []error
	for {
		select {
		case err := <-w.errs:
			out = append(out, err)
		default:
			return out
		}
	}
}

type testJobData struct {
	Mode  string `json:"mode"`
	Times int    `json:"times"`
	Ms    int    `json:"ms"`
	MaxMs int    `json:"maxMs"`
}

// modeHandler mirrors the handlers in parity/bullmq-consumer.ts.
func (e *liveEnv) modeHandler(tag string) Handler {
	return func(ctx context.Context, job Job) error {
		var data testJobData
		if err := json.Unmarshal(job.Data, &data); err != nil {
			return err
		}
		switch data.Mode {
		case "sleep":
			time.Sleep(time.Duration(data.Ms) * time.Millisecond)
		case "failTimes":
			if job.AttemptsMade < data.Times {
				return fmt.Errorf("boom")
			}
		case "unrecoverable":
			return Unrecoverable(fmt.Errorf("nope"))
		case "count":
			time.Sleep(time.Duration(time.Now().UnixNano()%int64(max(data.MaxMs, 1))) * time.Millisecond)
			e.client.Do(ctx, e.client.B().Incr().Key("wparity:proc:"+tag+":"+job.ID).Build())
			e.client.Do(ctx, e.client.B().Hset().Key("wparity:by:"+tag).FieldValue().FieldValue(job.ID, "go").Build())
		case "hangFirst":
			if job.AttemptsStarted <= 1 {
				e.client.Do(ctx, e.client.B().Set().Key("wparity:started:"+tag).Value(job.ID).Build())
				<-ctx.Done()
				return ctx.Err()
			}
		}
		return nil
	}
}

// ---- state snapshots ----

// queueState is a queue's Redis state with what differs by clock or worker
// removed: timestamps become markers, stack traces keep their first lines.
func (e *liveEnv) queueState(queue string) map[string]any {
	e.t.Helper()
	ctx := context.Background()
	prefix := keyPrefix + ":" + queue + ":"
	keys, err := e.client.Do(ctx, e.client.B().Keys().Pattern(prefix+"*").Build()).AsStrSlice()
	check(e.t, err)
	state := map[string]any{}
	for _, key := range keys {
		name := strings.TrimPrefix(key, prefix)
		kind, err := e.client.Do(ctx, e.client.B().Type().Key(key).Build()).ToString()
		check(e.t, err)
		switch kind {
		case "string":
			value, _ := e.client.Do(ctx, e.client.B().Get().Key(key).Build()).ToString()
			if name == "stalled-check" || name == "limiter" {
				value = "<set>"
			}
			state[name] = value
		case "hash":
			state[name] = normalisedHash(name, e.hash(key))
		case "list":
			items, _ := e.client.Do(ctx, e.client.B().Lrange().Key(key).Start(0).Stop(-1).Build()).AsStrSlice()
			state[name] = items
		case "zset":
			items, _ := e.client.Do(ctx, e.client.B().Zrange().Key(key).Min("0").Max("-1").Build()).AsStrSlice()
			// Jobs finishing in the same millisecond tie on score.
			slices.Sort(items)
			state[name] = items
		case "set":
			items, _ := e.client.Do(ctx, e.client.B().Smembers().Key(key).Build()).AsStrSlice()
			state[name] = items
		case "stream":
			state[name] = e.eventsByJob(key)
		}
	}
	return state
}

var clockFields = map[string]bool{"timestamp": true, "processedOn": true, "finishedOn": true}

func normalisedHash(name string, hash map[string]string) map[string]any {
	out := map[string]any{}
	for field, value := range hash {
		switch {
		case clockFields[field]:
			out[field] = "<time>"
		case field == "pb":
			continue
		case field == "opts":
			var parsed any
			_ = json.Unmarshal([]byte(value), &parsed)
			out[field] = parsed
		case field == "stacktrace":
			var traces []string
			_ = json.Unmarshal([]byte(value), &traces)
			firstLines := make([]string, 0, len(traces))
			for _, trace := range traces {
				line, _, _ := strings.Cut(trace, "\n")
				firstLines = append(firstLines, line)
			}
			out[field] = firstLines
		default:
			out[field] = value
		}
	}
	return out
}

// eventsByJob groups the event stream by job id, keeping each job's order.
func (e *liveEnv) eventsByJob(key string) map[string][]map[string]string {
	ctx := context.Background()
	entries, err := e.client.Do(ctx, e.client.B().Xrange().Key(key).Start("-").End("+").Build()).AsXRange()
	check(e.t, err)
	out := map[string][]map[string]string{}
	for _, entry := range entries {
		fields := map[string]string{}
		for field, value := range entry.FieldValues {
			fields[field] = value
		}
		if _, ok := fields["delay"]; ok {
			fields["delay"] = "<time>"
		}
		out[fields["jobId"]] = append(out[fields["jobId"]], fields)
	}
	return out
}

// ---- 1: state parity ----

func job(name, id string, data map[string]any, opts map[string]any) map[string]any {
	options := map[string]any{"jobId": id}
	for key, value := range opts {
		options[key] = value
	}
	return map[string]any{"name": name, "data": data, "opts": options}
}

// TestWorkerLeavesWhatBullMQLeaves has the TypeScript producer add the same
// jobs to two queues, a TypeScript Worker drain one and the Go Worker the
// other, and compares everything left in Redis.
func TestWorkerLeavesWhatBullMQLeaves(t *testing.T) {
	env := newLiveEnv(t, true)
	ns := env.namespace("state")
	tsQueue := QueueKey("sessions", ns+"-ts", false)
	goQueue := QueueKey("sessions", ns+"-go", false)

	const backoffDelay = 80
	jobs := []map[string]any{
		job("session", "ok-keep", map[string]any{"mode": "ok"}, nil),
		job("session", "ok-remove", map[string]any{"mode": "ok"}, map[string]any{"removeOnComplete": true}),
		job("session", "ok-last-a", map[string]any{"mode": "ok"}, map[string]any{"removeOnComplete": 1}),
		job("session", "ok-last-b", map[string]any{"mode": "ok"}, map[string]any{"removeOnComplete": map[string]any{"age": 3600, "count": 1}}),
		job("session", "fail-once", map[string]any{"mode": "failTimes", "times": 99}, nil),
		job("session", "fail-remove", map[string]any{"mode": "failTimes", "times": 99}, map[string]any{"removeOnFail": true}),
		job("session", "fail-attempts", map[string]any{"mode": "failTimes", "times": 99}, map[string]any{"attempts": 3}),
		job("session", "retry-fixed", map[string]any{"mode": "failTimes", "times": 2}, map[string]any{"attempts": 3, "backoff": map[string]any{"type": "fixed", "delay": backoffDelay}}),
		job("session", "retry-number", map[string]any{"mode": "failTimes", "times": 1}, map[string]any{"attempts": 2, "backoff": backoffDelay}),
		job("session", "retry-exp", map[string]any{"mode": "failTimes", "times": 3}, map[string]any{"attempts": 4, "backoff": map[string]any{"type": "exponential", "delay": 40}}),
		job("session", "retry-lifo", map[string]any{"mode": "failTimes", "times": 1}, map[string]any{"attempts": 2, "lifo": true}),
		job("session", "unrecoverable", map[string]any{"mode": "unrecoverable"}, map[string]any{"attempts": 5}),
		job("session", "keep-failed", map[string]any{"mode": "failTimes", "times": 99}, map[string]any{"removeOnFail": map[string]any{"age": 86400, "count": 1000}}),
	}
	env.produce(tsQueue, ns, jobs)
	env.produce(goQueue, ns, jobs)

	tsWorker := env.startTSWorker(tsQueue, ns, nil)
	env.waitSettled(tsQueue)
	tsWorker.stop()

	goRun := env.startGoWorker("sessions", env.modeHandler(ns), WorkerOptions{Namespace: ns + "-go", DrainDelay: time.Second})
	env.waitSettled(goQueue)
	goRun.stop()
	for _, err := range goRun.drainErrors() {
		t.Errorf("go worker error: %v", err)
	}

	got, want := env.queueState(goQueue), env.queueState(tsQueue)
	reportStateDiff(t, got, want)
	if n := env.zsetLen(keyPrefix + ":" + goQueue + ":failed"); n == 0 {
		t.Error("no job failed: the comparison proves nothing")
	}
}

// reportStateDiff fails the test with only the keys (and, for the event
// stream and job hashes, only the entries) that differ.
func reportStateDiff(t *testing.T, got, want map[string]any) {
	t.Helper()
	names := map[string]bool{}
	for name := range got {
		names[name] = true
	}
	for name := range want {
		names[name] = true
	}
	for name := range names {
		if reflect.DeepEqual(got[name], want[name]) {
			continue
		}
		gotMap, gotOK := got[name].(map[string][]map[string]string)
		wantMap, wantOK := want[name].(map[string][]map[string]string)
		if gotOK && wantOK {
			for id := range gotMap {
				if !reflect.DeepEqual(gotMap[id], wantMap[id]) {
					g, _ := json.Marshal(gotMap[id])
					w, _ := json.Marshal(wantMap[id])
					t.Errorf("%s[%q]:\n go %s\n ts %s", name, id, g, w)
				}
			}
			for id := range wantMap {
				if _, ok := gotMap[id]; !ok {
					t.Errorf("%s[%q] missing in Go", name, id)
				}
			}
			continue
		}
		g, _ := json.Marshal(got[name])
		w, _ := json.Marshal(want[name])
		t.Errorf("%s:\n go %s\n ts %s", name, g, w)
	}
}

// ---- 2: shared queue ----

// TestGoAndTypeScriptWorkersShareAQueue has both consume one queue at once:
// every job must run exactly once.
func TestGoAndTypeScriptWorkersShareAQueue(t *testing.T) {
	env := newLiveEnv(t, true)
	ns := env.namespace("share")
	queue := QueueKey("sessions", ns, false)
	tag := ns

	tsWorker := env.startTSWorker(queue, tag, map[string]any{"concurrency": sharedConcurrent})
	var goRuns atomic.Int64
	handler := env.modeHandler(tag)
	goRun := env.startGoWorker("sessions", func(ctx context.Context, j Job) error {
		goRuns.Add(1)
		return handler(ctx, j)
	}, WorkerOptions{Namespace: ns, Concurrency: sharedConcurrent, DrainDelay: time.Second})

	jobs := make([]map[string]any, 0, sharedJobs)
	for i := range sharedJobs {
		jobs = append(jobs, job("session", fmt.Sprintf("job-%03d", i), map[string]any{"mode": "count", "maxMs": 15}, nil))
	}
	env.produce(queue, tag, jobs)

	completed := keyPrefix + ":" + queue + ":completed"
	env.waitFor("every job to complete", func() bool { return env.zsetLen(completed) == sharedJobs })
	env.waitSettled(queue)
	goRun.stop()
	tsWorker.stop()

	for i := range sharedJobs {
		id := fmt.Sprintf("job-%03d", i)
		count, _ := env.do(func(b rueidis.Builder) rueidis.Completed {
			return b.Get().Key("wparity:proc:" + tag + ":" + id).Build()
		}).AsInt64()
		if count != 1 {
			t.Errorf("%s ran %d times", id, count)
		}
	}
	by := env.hash("wparity:by:" + tag)
	counts := map[string]int{}
	for _, who := range by {
		counts[who]++
	}
	t.Logf("processed by %v", counts)
	if counts["go"] == 0 || counts["ts"] == 0 {
		t.Errorf("one side did all the work: %v", counts)
	}
	if int(goRuns.Load()) != counts["go"] {
		t.Errorf("go handler ran %d times for %d jobs", goRuns.Load(), counts["go"])
	}
	for _, err := range goRun.drainErrors() {
		t.Errorf("go worker error: %v", err)
	}
}

// ---- 3: crash ----

// TestWorkerChildProcess is the process the crash test kills; it is a test
// only so the test binary can start itself.
func TestWorkerChildProcess(t *testing.T) {
	if os.Getenv(childEnv) == "" {
		t.Skip("helper process of TestKilledGoWorkerJobIsRecovered")
	}
	env := newLiveEnv(t, false)
	ns := os.Getenv(childEnv)
	worker, err := NewWorker(env.redis, "sessions", env.modeHandler(ns), WorkerOptions{
		Namespace:        ns,
		LockDuration:     childLockMs * time.Millisecond,
		SkipStalledCheck: true,
	})
	check(t, err)
	check(t, worker.Run(context.Background()))
}

// TestKilledGoWorkerJobIsRecovered kills a Go worker process while it runs a
// job and checks the stalled checker, Go's or BullMQ's, puts the job back.
func TestKilledGoWorkerJobIsRecovered(t *testing.T) {
	cases := []struct {
		name   string
		viaGo  bool
		maxStc *int
		want   func(t *testing.T, env *liveEnv, jobKey string)
	}{
		{"go checker retries", true, nil, expectRecovered},
		{"ts checker retries", false, nil, expectRecovered},
		{"go checker fails a job that stalled too often", true, new(int), expectStalledFailure},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := newLiveEnv(t, true)
			ns := env.namespace("crash")
			queue := QueueKey("sessions", ns, false)
			base := keyPrefix + ":" + queue + ":"

			env.produce(queue, ns, []map[string]any{job("session", "victim", map[string]any{"mode": "hangFirst"}, map[string]any{"removeOnComplete": false})})

			child := exec.Command(os.Args[0], "-test.run=^TestWorkerChildProcess$")
			child.Env = append(os.Environ(), childEnv+"="+ns)
			check(t, child.Start())
			t.Cleanup(func() { _ = child.Process.Kill() })
			env.waitFor("the child to start the job", func() bool {
				started, _ := env.do(func(b rueidis.Builder) rueidis.Completed { return b.Get().Key("wparity:started:" + ns).Build() }).ToString()
				return started == "victim"
			})
			check(t, child.Process.Kill())
			_ = child.Wait()

			if env.listLen(base+"active") != 1 {
				t.Fatal("job is not active after the kill")
			}

			var recovered []string
			var mu sync.Mutex
			if c.viaGo {
				opts := WorkerOptions{
					Namespace:       ns,
					LockDuration:    childLockMs * time.Millisecond,
					StalledInterval: childStalledMs * time.Millisecond,
					DrainDelay:      time.Second,
					OnStalled: func(ids []string) {
						mu.Lock()
						recovered = append(recovered, ids...)
						mu.Unlock()
					},
				}
				if c.maxStc != nil {
					opts.MaxStalledCount, opts.MaxStalledCountSet = *c.maxStc, true
				}
				env.startGoWorker("sessions", env.modeHandler(ns), opts)
			} else {
				env.startTSWorker(queue, ns, map[string]any{"lockDuration": childLockMs, "stalledInterval": childStalledMs})
			}
			finished := func() bool {
				return env.zsetLen(base+"completed")+env.zsetLen(base+"failed") == 1
			}
			env.waitFor("the job to be recovered and finished", finished)
			c.want(t, env, base+"victim")
			mu.Lock()
			defer mu.Unlock()
			if c.viaGo && len(recovered) != 1 {
				t.Errorf("Go checker reported %v, want the one job", recovered)
			}
		})
	}
}

func expectRecovered(t *testing.T, env *liveEnv, jobKey string) {
	t.Helper()
	hash := env.hash(jobKey)
	if hash["stc"] != "1" || hash["ats"] != "2" || hash["returnvalue"] != "null" || hash["failedReason"] != "" {
		t.Errorf("job after recovery: %v", hash)
	}
}

func expectStalledFailure(t *testing.T, env *liveEnv, jobKey string) {
	t.Helper()
	hash := env.hash(jobKey)
	if hash["failedReason"] != "job stalled more than allowable limit" || hash["stc"] != "1" {
		t.Errorf("job after too many stalls: %v", hash)
	}
}

package flush

import (
	"bufio"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redis/rueidis"

	"openpanel/ingest/internal/worker/clock"
	"openpanel/ingest/internal/worker/flush/chtest"
)

const (
	liveLockName   = "wparity-lock-compat"
	tsLockScript   = "../../../parity/flush-ts-lock.ts"
	repoCoreDir    = "../../../../../packages/core"
	bunEnv         = "WPARITY_BUN"
	liveWaitBudget = 30 * time.Second
)

func bunBinary(t *testing.T) string {
	t.Helper()
	if path := os.Getenv(bunEnv); path != "" {
		return path
	}
	path, err := exec.LookPath("bun")
	if err != nil {
		t.Skipf("no bun: set %s", bunEnv)
	}
	return path
}

func liveRedis(t *testing.T) (*RueidisRedis, rueidis.Client, string) {
	t.Helper()
	url := chtest.RedisURL(t)
	options, err := rueidis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	options.DisableCache = true
	client, err := rueidis.NewClient(options)
	if err != nil {
		t.Fatalf("Redis %s: %v", url, err)
	}
	t.Cleanup(client.Close)
	return NewRueidis(client, 5*time.Second), client, url
}

// tsProbe is a TypeScript BaseBuffer (packages/core) in a Bun process.
type tsProbe struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	output *bufio.Scanner
}

func startProbe(t *testing.T, mode, redisURL string) *tsProbe {
	t.Helper()
	script, err := filepath.Abs(tsLockScript)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bunBinary(t), script, mode, liveLockName)
	cmd.Dir = repoCoreDir
	cmd.Env = append(os.Environ(), "REDIS_URL="+redisURL)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64<<10), 16<<20) // the client's startup logs can be long lines
	cmd.Stderr = os.Stderr
	return &tsProbe{cmd: cmd, stdin: stdin, output: scanner}
}

// waitLine returns the first output line with prefix, within the budget.
func (p *tsProbe) waitLine(t *testing.T, prefix string) string {
	t.Helper()
	type answer struct {
		rest  string
		found bool
	}
	found := make(chan answer, 1)
	go func() {
		for p.output.Scan() {
			if line := p.output.Text(); strings.HasPrefix(line, prefix) {
				found <- answer{strings.TrimSpace(strings.TrimPrefix(line, prefix)), true}
				return
			}
		}
		found <- answer{}
	}()
	select {
	case got := <-found:
		if !got.found {
			t.Fatalf("the TypeScript process ended without a %q line", prefix)
		}
		return got.rest
	case <-time.After(liveWaitBudget):
		t.Fatalf("no %q line from the TypeScript process", prefix)
		return ""
	}
}

func goRunner(t *testing.T, redis Redis, flusher Flusher, obs Observer) *Runner {
	t.Helper()
	runner, err := NewRunner(flusher, Deps{Redis: redis, Clock: clock.System{}, Observer: obs, Paused: NeverPaused})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func cleanLock(t *testing.T, client rueidis.Client) {
	t.Helper()
	del := func() { client.Do(context.Background(), client.B().Del().Key("lock:"+liveLockName).Build()) }
	del()
	t.Cleanup(del)
}

// A TypeScript flush holding lock:<name> blocks the Go flush, and the other way
// round: same key, same Lua, so a buffer can change hands tick by tick.
func TestGoAndTypeScriptFlushesExcludeEachOther(t *testing.T) {
	chtest.BaseURL(t) // the WPARITY_CH gate
	redis, client, url := liveRedis(t)
	ctx := context.Background()
	cleanLock(t, client)
	obs := &collector{}

	t.Run("a TypeScript flush in progress blocks the Go flush", func(t *testing.T) {
		held := startProbe(t, "hold", url)
		held.waitLine(t, "HOLDING")
		value, err := client.Do(ctx, client.B().Get().Key("lock:"+liveLockName).Build()).ToString()
		if err != nil || !strings.HasPrefix(value, "lock_") {
			t.Fatalf("the TypeScript lock is %q (%v)", value, err)
		}

		runner := goRunner(t, redis, namedFlusher{name: liveLockName, flush: func(context.Context, *Run) error {
			t.Error("Go flushed under a TypeScript lock")
			return nil
		}}, obs)
		if err := runner.TryFlush(ctx, TriggerCron); err != nil {
			t.Fatal(err)
		}
		if got := obs.results(); len(got) != 1 || got[0] != ResultLocked {
			t.Fatalf("results %v", got)
		}
		if after, _ := client.Do(ctx, client.B().Get().Key("lock:"+liveLockName).Build()).ToString(); after != value {
			t.Fatalf("the Go flush disturbed the TypeScript lock: %q", after)
		}

		if _, err := held.stdin.Write([]byte("release\n")); err != nil {
			t.Fatal(err)
		}
		if got := held.waitLine(t, "RESULT"); got != "success" {
			t.Fatalf("TypeScript flush: %s", got)
		}

		ran := false
		runner = goRunner(t, redis, namedFlusher{name: liveLockName, flush: func(context.Context, *Run) error { ran = true; return nil }}, obs)
		if err := runner.TryFlush(ctx, TriggerCron); err != nil || !ran {
			t.Fatalf("Go flush after the release: ran=%v err=%v", ran, err)
		}
		// The TypeScript release cached the very script text Go hashes to.
		reply, err := redis.EvalSHA(ctx, releaseLockScript.SHA(), []string{"lock:" + liveLockName}, []string{"nobody"})
		if err != nil || reply != 0 {
			t.Fatalf("EVALSHA of the Go release script: %d %v (NOSCRIPT would mean the texts differ)", reply, err)
		}
		_ = held.cmd.Wait()
	})

	t.Run("a Go flush in progress blocks the TypeScript flush", func(t *testing.T) {
		started, finish := make(chan struct{}), make(chan struct{})
		runner := goRunner(t, redis, namedFlusher{name: liveLockName, flush: func(context.Context, *Run) error {
			close(started)
			<-finish
			return nil
		}}, nil)
		done := make(chan error, 1)
		go func() { done <- runner.TryFlush(ctx, TriggerCron) }()
		select {
		case <-started:
		case err := <-done:
			t.Fatalf("the Go flush never ran (lock held by someone else?): %v", err)
		case <-time.After(liveWaitBudget):
			t.Fatal("the Go flush never started")
		}

		probe := startProbe(t, "try", url)
		if got := probe.waitLine(t, "RESULT"); got != "locked" {
			t.Fatalf("TypeScript flush under a Go lock: %s", got)
		}
		close(finish)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if exists, _ := client.Do(ctx, client.B().Exists().Key("lock:"+liveLockName).Build()).AsInt64(); exists != 0 {
			t.Fatal("the Go flush left its lock behind")
		}
		probe = startProbe(t, "try", url)
		if got := probe.waitLine(t, "RESULT"); got != "success" {
			t.Fatalf("TypeScript flush after the Go release: %s", got)
		}
	})
}

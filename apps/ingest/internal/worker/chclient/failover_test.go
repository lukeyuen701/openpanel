package chclient

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"openpanel/ingest/internal/worker/clock"
)

// fakeClock records the waits instead of sleeping them.
type fakeClock struct {
	mutex  sync.Mutex
	now    time.Time
	sleeps []time.Duration
}

func (c *fakeClock) Now() time.Time { c.mutex.Lock(); defer c.mutex.Unlock(); return c.now }
func (c *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	c.mutex.Lock()
	c.sleeps = append(c.sleeps, d)
	c.now = c.now.Add(d)
	c.mutex.Unlock()
	return ctx.Err()
}
func (c *fakeClock) Tick(time.Duration) (<-chan time.Time, func()) { return nil, func() {} }

var _ clock.Clock = (*fakeClock)(nil)

// node is a scripted ClickHouse node on a real socket.
type scriptedNode struct {
	url      string
	mutex    sync.Mutex
	requests int
	bodies   []string
}

func (n *scriptedNode) count() int { n.mutex.Lock(); defer n.mutex.Unlock(); return n.requests }

// httpNode answers every request with status and body; it keeps the
// decompressed bodies it received.
func httpNode(t *testing.T, status int, answer string) *scriptedNode {
	t.Helper()
	node := &scriptedNode{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if r.Header.Get("Content-Encoding") == "gzip" {
			reader, err := gzip.NewReader(bytes.NewReader(raw))
			if err == nil {
				raw, _ = io.ReadAll(reader)
			}
		}
		node.mutex.Lock()
		node.requests++
		node.bodies = append(node.bodies, string(raw))
		node.mutex.Unlock()
		w.WriteHeader(status)
		_, _ = io.WriteString(w, answer)
	}))
	t.Cleanup(server.Close)
	node.url = server.URL + "/db"
	return node
}

// socketNode accepts connections and hands each one to behave.
func socketNode(t *testing.T, behave func(net.Conn)) *scriptedNode {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	node := &scriptedNode{url: "http://" + listener.Addr().String() + "/db"}
	done := make(chan struct{})
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			node.mutex.Lock()
			node.requests++
			node.mutex.Unlock()
			go behave(conn)
		}
	}()
	t.Cleanup(func() { listener.Close(); close(done) })
	return node
}

func refusedURL(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	return "http://" + address + "/db"
}

func newClient(t *testing.T, urls ...string) (*Client, *fakeClock) {
	t.Helper()
	cfg := defaultTestConfig()
	cfg.RequestTimeout = 400 * time.Millisecond
	clk := &fakeClock{now: time.Unix(1000, 0)}
	client, err := New(Config{URLs: urls, ClickHouseConfig: cfg}, Deps{Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	return client, clk
}

var threeRows = []string{`{"a":1}`, `{"a":2}`, `{"a":3}`}

const threeRowsBody = "{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n"

// readRequestHead consumes a request up to the blank line, then some body.
func readRequestHead(conn net.Conn) {
	buffer := make([]byte, 1<<16)
	var received []byte
	for !bytes.Contains(received, []byte("\r\n\r\n")) {
		n, err := conn.Read(buffer)
		if err != nil {
			return
		}
		received = append(received, buffer[:n]...)
	}
}

// clickhouse-failover.test.ts: a refused first node, the insert lands on the second.
func TestInsertFailsOverFromARefusedNode(t *testing.T) {
	live := httpNode(t, 200, "")
	client, clk := newClient(t, refusedURL(t), live.url)
	if err := client.Insert(context.Background(), TableEvents, threeRows, nil); err != nil {
		t.Fatal(err)
	}
	if live.count() != 1 || live.bodies[0] != threeRowsBody {
		t.Fatalf("live node got %d requests: %q", live.count(), live.bodies)
	}
	if len(clk.sleeps) != 1 || clk.sleeps[0] != 200*time.Millisecond {
		t.Fatalf("backoff %v", clk.sleeps)
	}
}

// P2.4 HAZARD: the node resets the connection after the body started to
// flow. The retry must send every row again; the TypeScript client sent an
// empty body here and the buffer then trimmed rows that never landed.
func TestRetryAfterAResetMidInsertSendsTheWholeBodyAgain(t *testing.T) {
	resetting := socketNode(t, func(conn net.Conn) {
		readRequestHead(conn)
		if tcp, ok := conn.(*net.TCPConn); ok {
			tcp.SetLinger(0) // close with RST
		}
		conn.Close()
	})
	live := httpNode(t, 200, "")
	client, _ := newClient(t, resetting.url, live.url)
	if err := client.Insert(context.Background(), TableEvents, threeRows, nil); err != nil {
		t.Fatal(err)
	}
	if resetting.count() != 1 || live.count() != 1 {
		t.Fatalf("attempts: reset node %d, live node %d", resetting.count(), live.count())
	}
	if live.bodies[0] != threeRowsBody {
		t.Fatalf("the retry sent %q", live.bodies[0])
	}
}

func TestAHangUpWithoutAnAnswerIsRetried(t *testing.T) {
	hangingUp := socketNode(t, func(conn net.Conn) { readRequestHead(conn); conn.Close() })
	live := httpNode(t, 200, "")
	client, _ := newClient(t, hangingUp.url, live.url)
	if err := client.Insert(context.Background(), TableBots, threeRows, nil); err != nil {
		t.Fatal(err)
	}
	if live.bodies[0] != threeRowsBody {
		t.Fatalf("retry body %q", live.bodies[0])
	}
}

// Node's "Timeout error." is not retried: the insert may have landed.
func TestARequestTimeoutIsNotRetried(t *testing.T) {
	silent := socketNode(t, func(conn net.Conn) { readRequestHead(conn); time.Sleep(3 * time.Second); conn.Close() })
	live := httpNode(t, 200, "")
	client, clk := newClient(t, silent.url, live.url)
	err := client.Insert(context.Background(), TableEvents, threeRows, nil)
	if err == nil {
		t.Fatal("a silent node must fail the insert")
	}
	if live.count() != 0 || len(clk.sleeps) != 0 {
		t.Fatalf("retried after a timeout: live node %d requests, waits %v", live.count(), clk.sleeps)
	}
}

func TestServerAnswersAreRetriedOnlyForTooManySimultaneousQueries(t *testing.T) {
	cases := []struct {
		name         string
		status       int
		answer       string
		wantRetry    bool
		wantServerEr bool
	}{
		{"202 is retried", 500, "Code: 202. DB::Exception: Too many simultaneous queries. (TOO_MANY_SIMULTANEOUS_QUERIES)\n", true, false},
		{"241 is not", 500, "Code: 241. DB::Exception: Memory limit exceeded. (MEMORY_LIMIT_EXCEEDED)\n", false, true},
		{"a proxy page is not", 502, "<html>Bad Gateway</html>\n", false, true},
		{"unknown table is not", 404, "Code: 60. DB::Exception: Table x does not exist. (UNKNOWN_TABLE)\n", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			failing := httpNode(t, tc.status, tc.answer)
			live := httpNode(t, 200, "")
			client, _ := newClient(t, failing.url, live.url)
			err := client.Insert(context.Background(), TableEvents, threeRows, nil)
			if tc.wantRetry {
				if err != nil || live.count() != 1 {
					t.Fatalf("err=%v, live node requests=%d", err, live.count())
				}
				return
			}
			var server *ServerError
			if !errors.As(err, &server) || live.count() != 0 {
				t.Fatalf("err=%v, live node requests=%d", err, live.count())
			}
		})
	}
}

func TestEveryNodeDownFailsAfterThreeAttemptsWithBackoff(t *testing.T) {
	client, clk := newClient(t, refusedURL(t), refusedURL(t))
	err := client.Insert(context.Background(), TableEvents, threeRows, nil)
	if err == nil || Classify(err) != ClassNodeDown {
		t.Fatalf("err = %v", err)
	}
	want := []time.Duration{200 * time.Millisecond, 400 * time.Millisecond}
	if len(clk.sleeps) != 2 || clk.sleeps[0] != want[0] || clk.sleeps[1] != want[1] {
		t.Fatalf("backoff %v, want %v", clk.sleeps, want)
	}
}

func TestMoreNodesMeanMoreAttempts(t *testing.T) {
	client, clk := newClient(t, refusedURL(t), refusedURL(t), refusedURL(t))
	if err := client.Insert(context.Background(), TableEvents, threeRows, nil); err == nil {
		t.Fatal("must fail")
	}
	if len(clk.sleeps) != 3 || clk.sleeps[2] != 800*time.Millisecond {
		t.Fatalf("four attempts leave three waits: %v", clk.sleeps)
	}
}

func TestACancelledContextStopsTheRetries(t *testing.T) {
	client, clk := newClient(t, refusedURL(t), refusedURL(t))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := client.Insert(ctx, TableEvents, threeRows, nil)
	if !errors.Is(err, context.Canceled) || len(clk.sleeps) != 0 {
		t.Fatalf("err=%v waits=%v", err, clk.sleeps)
	}
}

func TestANodeThatRefusedIsSkippedForTheUnhealthyMark(t *testing.T) {
	live := httpNode(t, 200, "")
	cfg := defaultTestConfig()
	cfg.UnhealthyMark = 5 * time.Second
	clk := &fakeClock{now: time.Unix(1000, 0)}
	client, err := New(Config{URLs: []string{refusedURL(t), live.url}, ClickHouseConfig: cfg}, Deps{Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := client.Insert(context.Background(), TableEvents, threeRows, nil); err != nil {
			t.Fatal(err)
		}
	}
	if len(clk.sleeps) != 1 {
		t.Fatalf("only the first call may touch the dead node, waits: %v", clk.sleeps)
	}
}

func TestQueryAndCommandRetryToo(t *testing.T) {
	live := httpNode(t, 200, `{"meta":[],"data":[{"value":1}],"rows":1}`)
	client, _ := newClient(t, refusedURL(t), live.url)
	result, err := client.Query(context.Background(), "SELECT 1 AS value", nil, nil)
	if err != nil || result.Rows != 1 {
		t.Fatalf("query: %v %+v", err, result)
	}
	if err := client.Command(context.Background(), "ALTER TABLE x UPDATE a = 1 WHERE 1", nil); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(live.bodies[1], "ALTER TABLE") {
		t.Fatalf("command body %q", live.bodies[1])
	}
}

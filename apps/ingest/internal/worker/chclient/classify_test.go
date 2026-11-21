package chclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
)

func dialFailure(errno syscall.Errno) error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: errno}}
}

func readFailure(errno syscall.Errno) error {
	return &net.OpError{Op: "read", Net: "tcp", Err: &os.SyscallError{Syscall: "read", Err: errno}}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

// The cases of packages/db/src/clickhouse/round-robin.test.ts, in the shapes
// Go's net/http produces for the same situations.
func TestClassify(t *testing.T) {
	cases := []struct {
		label string
		err   error
		want  ErrorClass
	}{
		{"SQL syntax error", &ServerError{Status: 400, Code: 62, Message: "Code: 62. DB::Exception: Syntax error"}, ClassServer},
		{"unknown identifier", &ServerError{Status: 400, Code: 47, Message: "Code: 47. DB::Exception: Unknown identifier"}, ClassServer},
		{"TOO_MANY_PARTS is back-pressure, not connectivity", &ServerError{Status: 500, Code: 252, Message: "Code: 252. DB::Exception: Too many parts"}, ClassServer},
		{"MEMORY_LIMIT_EXCEEDED", &ServerError{Status: 500, Code: 241, Message: "Code: 241. DB::Exception: Memory limit"}, ClassServer},
		{"TIMEOUT_EXCEEDED, server-side", &ServerError{Status: 500, Code: 159, Message: "Code: 159. DB::Exception: Timeout exceeded"}, ClassServer},
		{"authentication denied", &ServerError{Status: 403, Code: 516, Message: "Code: 516. DB::Exception: Authentication failed"}, ClassServer},
		{"a body that mentions connect", &ServerError{Status: 500, Code: 999, Message: "Code: 999. DB::Exception: Cannot connect to ZooKeeper"}, ClassServer},
		{"TOO_MANY_SIMULTANEOUS_QUERIES by code", &ServerError{Status: 500, Code: 202, Message: "Code: 202. DB::Exception: Too many simultaneous queries"}, ClassTransient},
		{"TOO_MANY_SIMULTANEOUS_QUERIES by name only", &ServerError{Status: 500, Message: "(TOO_MANY_SIMULTANEOUS_QUERIES)"}, ClassTransient},
		{"proxy HTML error page", &ServerError{Status: 502, Message: "<html>Bad Gateway</html>"}, ClassOther},

		{"ECONNREFUSED", dialFailure(syscall.ECONNREFUSED), ClassNodeDown},
		{"EHOSTUNREACH", dialFailure(syscall.EHOSTUNREACH), ClassNodeDown},
		{"EHOSTDOWN", dialFailure(syscall.EHOSTDOWN), ClassNodeDown},
		{"ENETUNREACH", dialFailure(syscall.ENETUNREACH), ClassNodeDown},
		{"connect ETIMEDOUT", dialFailure(syscall.ETIMEDOUT), ClassNodeDown},
		{"temporary DNS failure", &net.DNSError{Err: "server misbehaving", IsTemporary: true}, ClassNodeDown},
		{"DNS timeout", &net.DNSError{Err: "i/o timeout", IsTimeout: true}, ClassNodeDown},
		{"connect that never completes", &net.OpError{Op: "dial", Net: "tcp", Err: timeoutError{}}, ClassNodeDown},
		{"cause-chained ECONNREFUSED", fmt.Errorf("Post: %w", dialFailure(syscall.ECONNREFUSED)), ClassNodeDown},

		{"ECONNRESET", readFailure(syscall.ECONNRESET), ClassTransient},
		{"EPIPE", &net.OpError{Op: "write", Err: &os.SyscallError{Syscall: "write", Err: syscall.EPIPE}}, ClassTransient},
		{"socket hang up (EOF before a response)", io.EOF, ClassTransient},
		{"premature close", fmt.Errorf("Post: %w", io.ErrUnexpectedEOF), ClassTransient},
		{"stale keep-alive socket", errors.New("http: server closed idle connection"), ClassTransient},
		{"deeply nested ECONNRESET", fmt.Errorf("a: %w", fmt.Errorf("b: %w", readFailure(syscall.ECONNRESET))), ClassTransient},

		{"nil", nil, ClassOther},
		{"unrecognised", errors.New("whatever happened was weird"), ClassOther},
		{"a bare connect in a non-network message", errors.New("Cannot connect the dots in this query plan"), ClassOther},
		{"DNS name not found", &net.DNSError{Err: "no such host", IsNotFound: true}, ClassOther},
		{"request timeout (Node: Timeout error.)", fmt.Errorf("Post: %w", context.DeadlineExceeded), ClassOther},
		{"response header timeout", errors.New("net/http: timeout awaiting response headers"), ClassOther},
		{"cancelled", context.Canceled, ClassOther},
	}
	for _, tc := range cases {
		if got := Classify(tc.err); got != tc.want {
			t.Errorf("%s: classified %q, want %q", tc.label, got, tc.want)
		}
	}
}

func TestServerErrorTakesItsCodeFromTheBodyOrTheHeader(t *testing.T) {
	if err := newServerError(500, "Code: 241. DB::Exception: Memory limit", ""); err.Code != 241 {
		t.Errorf("body code: %d", err.Code)
	}
	if err := newServerError(500, "no code here", "202"); err.Code != 202 {
		t.Errorf("header code: %d", err.Code)
	}
	if err := newServerError(502, "<html>", ""); err.Code != 0 {
		t.Errorf("no code: %d", err.Code)
	}
}

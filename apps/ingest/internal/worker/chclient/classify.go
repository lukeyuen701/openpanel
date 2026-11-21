package chclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"syscall"
)

// ErrorClass is the four-way split of packages/db/src/clickhouse/round-robin.ts.
type ErrorClass string

const (
	// ClassNodeDown: the node is unreachable. Retry on the next node and skip
	// this one for UnhealthyMark.
	ClassNodeDown ErrorClass = "node-down"
	// ClassTransient: a live socket reset or hung up, or the server is
	// saturated (code 202). Retry on the next node; the node is not marked.
	ClassTransient ErrorClass = "transient"
	// ClassServer: ClickHouse rejected the request. No retry.
	ClassServer ErrorClass = "ch-server"
	// ClassOther: unknown shape, including a request timeout. No retry.
	ClassOther ErrorClass = "other"
)

const tooManySimultaneousQueriesCode = 202

// ServerError is a non-2xx answer from ClickHouse.
type ServerError struct {
	Status int
	// Code is the ClickHouse exception code, 0 when the body carried none.
	Code    int
	Message string
}

// Error implements error.
func (e *ServerError) Error() string {
	return fmt.Sprintf("clickhouse: HTTP %d: %s", e.Status, e.Message)
}

var exceptionCodePattern = regexp.MustCompile(`^\s*Code:\s*(\d+)`)

func newServerError(status int, body, codeHeader string) *ServerError {
	message := strings.TrimSpace(body)
	code := 0
	if match := exceptionCodePattern.FindStringSubmatch(message); match != nil {
		code, _ = strconv.Atoi(match[1])
	} else if parsed, err := strconv.Atoi(strings.TrimSpace(codeHeader)); err == nil {
		code = parsed
	}
	return &ServerError{Status: status, Code: code, Message: message}
}

// Classify decides what the retry loop does with err.
func Classify(err error) ErrorClass {
	if err == nil {
		return ClassOther
	}
	var server *ServerError
	if errors.As(err, &server) {
		return classifyServer(server)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ClassOther
	}
	for _, errno := range nodeDownErrnos {
		if errors.Is(err, errno) {
			return ClassNodeDown
		}
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		if dns.IsTemporary || dns.IsTimeout {
			return ClassNodeDown
		}
		return ClassOther
	}
	// Nothing was sent when the connection never opened, so retrying on
	// another node is safe even though Node would wait out its request timeout.
	var op *net.OpError
	if errors.As(err, &op) && op.Op == "dial" && op.Timeout() {
		return ClassNodeDown
	}
	for _, errno := range transientErrnos {
		if errors.Is(err, errno) {
			return ClassTransient
		}
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return ClassTransient
	}
	message := err.Error()
	for _, keyword := range transientMessages {
		if strings.Contains(message, keyword) {
			return ClassTransient
		}
	}
	return ClassOther
}

func classifyServer(err *ServerError) ErrorClass {
	if err.Code == tooManySimultaneousQueriesCode ||
		strings.Contains(err.Message, "TOO_MANY_SIMULTANEOUS_QUERIES") ||
		strings.Contains(err.Message, "Too many simultaneous queries") {
		return ClassTransient
	}
	if err.Code != 0 {
		return ClassServer
	}
	return ClassOther
}

var nodeDownErrnos = []error{
	syscall.ECONNREFUSED, syscall.EHOSTUNREACH, syscall.EHOSTDOWN, syscall.ENETUNREACH, syscall.ETIMEDOUT,
}

var transientErrnos = []error{syscall.ECONNRESET, syscall.EPIPE}

// transientMessages are the shapes of a socket that died under a request when
// the wrapped errno was lost.
var transientMessages = []string{
	"connection reset by peer", "broken pipe", "server closed idle connection",
	"use of closed network connection",
}

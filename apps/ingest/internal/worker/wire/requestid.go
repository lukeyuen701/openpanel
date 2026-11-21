package wire

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
)

const (
	// RequestIDLength is the length of a freshly minted request id.
	RequestIDLength    = 21
	requestIDMaxLength = 64
	// idAlphabet is nanoid's, which the TypeScript generateId uses.
	idAlphabet = "useandom-26T198340PX75pxJACKVERYMINDBUSHWOLF_GQZbfghjklqvwyzrict"
	// RequestIDHeader is the payload header that carries the request id.
	RequestIDHeader = "request-id"
)

// ErrRequestIDType is what resolveRequestId's candidate.replace throws for a
// request-id header that is neither absent nor a string.
var ErrRequestIDType = errors.New("candidate.replace is not a function")

// IDSource mints random ids, so a rig can make them deterministic.
type IDSource interface {
	NewID(length int) string
}

// RandomIDs is the production IDSource.
type RandomIDs struct{}

// NewID returns length characters of nanoid's alphabet.
func (RandomIDs) NewID(length int) string {
	buf := make([]byte, length)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	for i, b := range buf {
		buf[i] = idAlphabet[b&63]
	}
	return string(buf)
}

// ResolveRequestID ports resolveRequestId (core/shared/request-id.ts): the
// header value with every character outside [A-Za-z0-9_-] removed and cut to
// 64, or a fresh id when nothing is left. Absent, null, "", false and 0 are
// falsy in JavaScript and count as absent; any other non-string value is an
// error, as it is in the TypeScript handler.
func ResolveRequestID(raw json.RawMessage, ids IDSource) (string, error) {
	candidate, err := requestIDCandidate(raw)
	if err != nil {
		return "", err
	}
	cleaned := make([]byte, 0, len(candidate))
	for i := 0; i < len(candidate) && len(cleaned) < requestIDMaxLength; i++ {
		if isRequestIDByte(candidate[i]) {
			cleaned = append(cleaned, candidate[i])
		}
	}
	if len(cleaned) == 0 {
		return ids.NewID(RequestIDLength), nil
	}
	return string(cleaned), nil
}

func isRequestIDByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-'
}

func requestIDCandidate(raw json.RawMessage) (string, error) {
	trimmed := bytes.TrimSpace(raw)
	if isNull(trimmed) || string(trimmed) == "false" {
		return "", nil
	}
	if trimmed[0] == '"' {
		var s string
		if err := json.Unmarshal(trimmed, &s); err != nil {
			return "", err
		}
		return s, nil
	}
	var number float64
	if err := json.Unmarshal(trimmed, &number); err == nil && number == 0 {
		return "", nil
	}
	return "", ErrRequestIDType
}

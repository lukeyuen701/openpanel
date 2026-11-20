// Package identity derives the ids /track answers with: the salted device id
// and the session id, resolved against the live session blobs the worker
// keeps in Redis.
package identity

import (
	"context"
	"crypto/sha256"
	"crypto/sha3"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"
)

const (
	deviceIDBytes       = 16
	sessionIDBytes      = 16
	sessionGraceCeiling = 5000
	graceWindowDivisor  = 6
	// clickhouseDateLayout is the session blob's ended_at, always UTC.
	clickhouseDateLayout = "2006-01-02 15:04:05.000"
)

// DeviceID is generateDeviceId: shake256("ua:ip:origin:salt") as 32 hex chars.
func DeviceID(salt, ua, ip, origin string) string {
	out := sha3.SumSHAKE256([]byte(ua+":"+ip+":"+origin+":"+salt), deviceIDBytes)
	return hex.EncodeToString(out)
}

// SessionID is device-id.ts's getSessionId: a deterministic id per device and
// idle window, with a grace period at the start of each window that keeps an
// event on the previous bucket.
func SessionID(projectID, deviceID string, eventMs, windowMs, graceMs int64) (string, error) {
	if projectID == "" || deviceID == "" {
		return "", fmt.Errorf("identity: projectId and deviceId are required")
	}
	if windowMs <= 0 || graceMs < 0 || graceMs >= windowMs {
		return "", fmt.Errorf("identity: invalid session window %d/%d", windowMs, graceMs)
	}
	bucket := int64(math.Floor(float64(eventMs) / float64(windowMs)))
	offset := eventMs - bucket*windowMs
	if offset < graceMs {
		bucket--
	}
	digest := sha256.Sum256([]byte("sess:v1:" + projectID + ":" + deviceID + ":" + strconv.FormatInt(bucket, 10)))
	return base64.RawURLEncoding.EncodeToString(digest[:sessionIDBytes]), nil
}

// SessionGraceMs is the grace the API applies for a given idle timeout.
func SessionGraceMs(timeoutMs int64) int64 {
	return min(sessionGraceCeiling, timeoutMs/graceWindowDivisor)
}

// LiveSession is the part of a session blob the API reads.
type LiveSession struct {
	ID      string `json:"id"`
	EndedAt string `json:"ended_at"`
}

// endedAtMs parses ended_at the way convertClickhouseDateToJs does; false for
// a value that would be an Invalid Date.
func (s LiveSession) endedAtMs() (int64, bool) {
	t, err := time.ParseInLocation(clickhouseDateLayout, s.EndedAt, time.UTC)
	if err != nil {
		// new Date("YYYY-MM-DDTHH:mm:ssZ") also parses without milliseconds.
		t, err = time.ParseInLocation("2006-01-02 15:04:05", s.EndedAt, time.UTC)
		if err != nil {
			return 0, false
		}
	}
	return t.UnixMilli(), true
}

// IsLive reports whether an event at eventMs extends this session.
func (s LiveSession) IsLive(eventMs, timeoutMs int64) bool {
	ended, ok := s.endedAtMs()
	return ok && eventMs-ended < timeoutMs
}

// SessionReader reads the live session blobs of devices, in order; a nil
// entry is a device with no blob.
type SessionReader interface {
	ExistingSessions(ctx context.Context, projectID string, deviceIDs []string) ([]*LiveSession, error)
}

// Result is the device and session an event is filed under.
type Result struct {
	DeviceID  string
	SessionID string
}

// Resolver picks the device and session for an event.
type Resolver struct {
	Sessions  SessionReader
	TimeoutMs int64
	// OnError is told about a failed session read; resolution then falls back
	// to the deterministic id, exactly as the API does.
	OnError func(error)
}

// Resolve is getInfoFromSession: the first candidate with a live session
// wins; otherwise the first candidate and its deterministic session id.
func (r *Resolver) Resolve(ctx context.Context, projectID string, candidates []string, eventMs int64) (Result, error) {
	candidates = dedupeNonEmpty(candidates)
	primary := ""
	if len(candidates) > 0 {
		primary = candidates[0]
	}
	sessions, err := r.Sessions.ExistingSessions(ctx, projectID, candidates)
	if err != nil {
		if r.OnError != nil {
			r.OnError(err)
		}
		sessions = nil
	}
	for index, session := range sessions {
		if session != nil && session.IsLive(eventMs, r.TimeoutMs) {
			return Result{DeviceID: candidates[index], SessionID: session.ID}, nil
		}
	}
	sessionID, err := SessionID(projectID, primary, eventMs, r.TimeoutMs, SessionGraceMs(r.TimeoutMs))
	if err != nil {
		return Result{}, err
	}
	return Result{DeviceID: primary, SessionID: sessionID}, nil
}

func dedupeNonEmpty(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		duplicate := false
		for _, existing := range out {
			if existing == value {
				duplicate = true
				break
			}
		}
		if !duplicate {
			out = append(out, value)
		}
	}
	return out
}

// DecodeSession parses a blob the way getSafeJson does: nil when it is not
// JSON or has no usable fields.
func DecodeSession(raw string) *LiveSession {
	var s LiveSession
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		return nil
	}
	return &s
}

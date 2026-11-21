package consumer

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
)

func readGolden[T any](t *testing.T, name string) []T {
	t.Helper()
	raw, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	var cases []T
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	return cases
}

func parseOffset(t *testing.T, s string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestDecodeTextMatchesBufferToString compares with Buffer#toString output of
// the real runtime (parity/golden-consumer.ts).
func TestDecodeTextMatchesBufferToString(t *testing.T) {
	type golden struct {
		Hex  string `json:"hex"`
		Text string `json:"text"`
	}
	for _, c := range readGolden[golden](t, "utf8.json") {
		if got := decodeText(mustHex(t, c.Hex)); got != c.Text {
			t.Errorf("%s: got %q, want %q", c.Hex, got, c.Text)
		}
	}
}

func TestEncodeDeadLetterMatchesJSONStringifyOfTheRecord(t *testing.T) {
	type goldenHeader struct {
		Name string `json:"name"`
		Hex  string `json:"hex"`
	}
	type golden struct {
		Key     *string        `json:"key"`
		Value   *string        `json:"value"`
		Headers []goldenHeader `json:"headers"`
		Offset  string         `json:"offset"`
		Reason  string         `json:"reason"`
		Error   string         `json:"error"`
		Out     string         `json:"out"`
	}
	for i, c := range readGolden[golden](t, "deadletter.json") {
		message := DeadLetterMessage{Topic: "events", Partition: 3, Reason: c.Reason, Err: c.Error}
		if c.Key != nil {
			message.Key = mustHex(t, *c.Key)
			if message.Key == nil {
				message.Key = []byte{}
			}
		}
		if c.Value != nil {
			message.Value = mustHex(t, *c.Value)
			if message.Value == nil {
				message.Value = []byte{}
			}
		}
		for _, h := range c.Headers {
			message.Headers = append(message.Headers, Header{Key: h.Name, Value: mustHex(t, h.Hex)})
		}
		message.Offset = parseOffset(t, c.Offset)
		if got := EncodeDeadLetter(message, testEpoch); got != c.Out {
			t.Errorf("case %d:\n got %s\nwant %s", i, got, c.Out)
		}
	}
}

// memoryList is Redis's list semantics for LPUSH + LTRIM.
type memoryList struct {
	mu      sync.Mutex
	entries []string
	err     error
}

func (m *memoryList) PushCapped(_ context.Context, _, value string, maxEntries int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.entries = slices.Insert(m.entries, 0, value)
	if len(m.entries) > maxEntries {
		m.entries = m.entries[:maxEntries]
	}
	return nil
}

func TestRedisDeadLetterKeepsTheMostRecentEntriesNewestFirst(t *testing.T) {
	list := &memoryList{}
	sink := NewRedisDeadLetter(list, &fakeClock{}, 3)
	for offset := range int64(5) {
		message := DeadLetterMessage{Topic: "events", Offset: offset, Reason: ReasonHandlerError, Err: "x"}
		if err := sink.Record(context.Background(), message); err != nil {
			t.Fatal(err)
		}
	}
	if len(list.entries) != 3 {
		t.Fatalf("entries = %d", len(list.entries))
	}
	for i, wantOffset := range []string{"4", "3", "2"} {
		var record struct{ Offset string }
		if err := json.Unmarshal([]byte(list.entries[i]), &record); err != nil || record.Offset != wantOffset {
			t.Fatalf("entry %d = %s", i, list.entries[i])
		}
	}
}

func TestRedisDeadLetterReportsAFailedWrite(t *testing.T) {
	sink := NewRedisDeadLetter(&memoryList{err: errRedisDown}, &fakeClock{}, 3)
	if err := sink.Record(context.Background(), DeadLetterMessage{}); err == nil {
		t.Fatal("expected the write error")
	}
}

package events

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"os"
	"regexp"
	"strings"
	"testing"

	"openpanel/ingest/internal/worker/bullmq"
	"openpanel/ingest/internal/worker/consumer"
	"openpanel/ingest/internal/worker/jsport"
	"openpanel/ingest/internal/worker/wire"
)

// handlerGolden is one record of testdata/handler.json.gz: what the TypeScript
// incomingEvent did with it (bun run golden:events).
type handlerGolden struct {
	Label    string   `json:"label"`
	Value    string   `json:"value"`
	Error    bool     `json:"error"`
	Events   []string `json:"events"`
	Profiles []struct {
		Row         string `json:"row"`
		IsFromEvent bool   `json:"isFromEvent"`
	} `json:"profiles"`
	Notifications []string `json:"notifications"`
	SessionEnds   []struct {
		Data    string `json:"data"`
		Options struct {
			JobID    string `json:"jobId"`
			Attempts int    `json:"attempts"`
			Backoff  struct {
				Type  string `json:"type"`
				Delay int    `json:"delay"`
			} `json:"backoff"`
		} `json:"options"`
	} `json:"sessionEnds"`
	AI                 []string `json:"ai"`
	SessionStarted     []string `json:"sessionStarted"`
	SessionEndEnqueued []string `json:"sessionEndEnqueued"`
	SessionRows        []string `json:"sessionRows"`
	SessionBlob        *string  `json:"sessionBlob"`
	Pointer            *string  `json:"pointer"`
}

var (
	clockFieldPattern = regexp.MustCompile(`"(inserted_at|created_at|last_seen_at)":"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{3})?"`)
)

// maskClock is the golden script's mask: the insert time of an event and both
// timestamps of a profile are the wall clock.
func maskClock(line string, isProfile bool) string {
	return clockFieldPattern.ReplaceAllStringFunc(line, func(match string) string {
		field := match[1 : strings.Index(match[1:], `"`)+1]
		if field == "inserted_at" || isProfile {
			return `"` + field + `":"<now>"`
		}
		return match
	})
}

func loadHandlerGolden(t *testing.T) []handlerGolden {
	t.Helper()
	file, err := os.Open("testdata/handler.json.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	text, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Records []handlerGolden `json:"records"`
	}
	if err := json.NewDecoder(bytes.NewReader(text)).Decode(&doc); err != nil {
		t.Fatal(err)
	}
	return doc.Records
}

func sameLines(t *testing.T, label, what string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: %s: %d lines, want %d\n got: %v\nwant: %v", label, what, len(got), len(want), got, want)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s: %s[%d]\n got: %s\nwant: %s", label, what, i, got[i], want[i])
		}
	}
}

// TestHandlerMatchesTypeScript replays the records the golden script ran
// through the real TypeScript handler and compares every effect byte for byte:
// the event rows, profile rows, session blob and rows, the session-end jobs,
// the payloads handed to the notification check and the AI rows.
func TestHandlerMatchesTypeScript(t *testing.T) {
	records := loadHandlerGolden(t)
	r := newRig(t)
	// The golden run filtered project A's exclusion event and kept
	// project C's AI content private; see parity/golden-events.ts.
	r.projects.excludedByName["wparity-a"] = "wparity_excluded"
	r.projects.aiStoreContent["wparity-c"] = false

	var seen = map[string]int{}
	listDelta := func(key string) []string {
		all := r.redis.list(key)
		delta := all[seen[key]:]
		seen[key] = len(all)
		return delta
	}
	ctx := context.Background()
	for _, record := range records {
		r.notifier.payloads = nil
		r.queue.jobs = nil
		r.metrics.started, r.metrics.ended = nil, nil

		decoded, err := wire.Decode([]byte(record.Value))
		if err != nil {
			t.Fatalf("%s: %v", record.Label, err)
		}
		handleErr := decoded.ShapeErr
		if handleErr == nil {
			handleErr = r.handler.Handle(ctx, decoded.Payload, consumer.Delivery{})
		}
		r.flush(t)
		if (handleErr != nil) != record.Error {
			t.Errorf("%s: error = %v, TypeScript failed = %v", record.Label, handleErr, record.Error)
			continue
		}

		var events []string
		for _, line := range listDelta(EventQueueKey) {
			events = append(events, maskClock(line, false))
		}
		sameLines(t, record.Label, "events", events, record.Events)

		var profiles []string
		for _, line := range listDelta(ProfileBufferKey) {
			profiles = append(profiles, maskClock(line, true))
		}
		var wantProfiles []string
		for _, profile := range record.Profiles {
			wantProfiles = append(wantProfiles, profile.Row)
			if !profile.IsFromEvent {
				t.Errorf("%s: golden profile not from an event", record.Label)
			}
		}
		sameLines(t, record.Label, "profiles", profiles, wantProfiles)

		sameLines(t, record.Label, "session rows", listDelta(SessionBufferKey), record.SessionRows)
		sameLines(t, record.Label, "ai rows", listDelta(AIEventBufferKey), record.AI)
		sameLines(t, record.Label, "notification payloads", r.notifier.payloads, record.Notifications)
		sameLines(t, record.Label, "sessions started", r.metrics.started, record.SessionStarted)
		sameLines(t, record.Label, "session ends counted", r.metrics.ended, record.SessionEndEnqueued)

		if len(r.queue.jobs) != len(record.SessionEnds) {
			t.Errorf("%s: %d session-end jobs, want %d", record.Label, len(r.queue.jobs), len(record.SessionEnds))
		} else {
			for i, want := range record.SessionEnds {
				job := r.queue.jobs[i]
				if !strings.HasPrefix(job.data, `{"payload":`+want.Data+`,"meta":`) {
					t.Errorf("%s: session-end data\n got: %s\nwant payload: %s", record.Label, job.data, want.Data)
				}
				if job.queue != "sessions" || job.name != bullmq.SessionJob || job.opts.JobID != want.Options.JobID ||
					job.opts.Attempts != want.Options.Attempts || job.opts.Backoff.Type != want.Options.Backoff.Type ||
					job.opts.Backoff.DelayMs != want.Options.Backoff.Delay {
					t.Errorf("%s: session-end job %+v, want %+v", record.Label, job, want.Options)
				}
			}
		}

		deviceID := decoded.Payload.DeviceID
		if deviceID != "" {
			blob, ok, _ := r.redis.Get(ctx, "session:"+decoded.Payload.ProjectID+":"+deviceID)
			switch {
			case record.SessionBlob == nil && ok:
				t.Errorf("%s: session blob present, TypeScript has none", record.Label)
			case record.SessionBlob != nil && (!ok || blob != *record.SessionBlob):
				t.Errorf("%s: session blob\n got: %s\nwant: %s", record.Label, blob, *record.SessionBlob)
			}
		}
		if profileID := profileText(decoded.Payload.Event.ProfileID); profileID != "" {
			pointer, ok, _ := r.redis.Get(ctx, "session:profile:"+decoded.Payload.ProjectID+":"+profileID)
			switch {
			case record.Pointer == nil && ok:
				t.Errorf("%s: profile pointer present, TypeScript has none", record.Label)
			case record.Pointer != nil && (!ok || pointer != *record.Pointer):
				t.Errorf("%s: profile pointer %q, want %q", record.Label, pointer, *record.Pointer)
			}
		}
	}
}

// profileText is String(profileId) for a truthy raw profile id, "" otherwise.
func profileText(raw json.RawMessage) string {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	case float64:
		if v == 0 {
			return ""
		}
		return jsport.NumberToString(v)
	}
	return ""
}

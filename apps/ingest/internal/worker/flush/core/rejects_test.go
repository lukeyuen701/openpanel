package core

import (
	"context"
	"errors"
	"testing"

	"openpanel/ingest/internal/worker/flush"
)

var errListDown = errors.New("redis down")

const deadLetterKey = "dead_letter:buffer"

func requireRecord(t *testing.T, record map[string]string, buffer, reason, row string) {
	t.Helper()
	if record["buffer"] != buffer || record["reason"] != reason || record["row"] != row || record["recordedAt"] != "2026-01-02T03:04:05.006Z" {
		t.Fatalf("record %+v, want %s/%s/%q", record, buffer, reason, row)
	}
}

func TestSessionRowsThatAreNotObjectsAreRecordedBeforeTheyAreTrimmed(t *testing.T) {
	r := newRig()
	r.redis.Push(SessionKey, "5", "[]", `"s"`, "null", "{", sessionRow("ok", 1, 1))
	observation := r.flush(t, NewSession(buffersConfig(t, nil), r.deps, Overrides{}))
	if observation.Result != flush.ResultSuccess || len(r.redis.List(SessionKey)) != 0 {
		t.Fatalf("observation %+v", observation)
	}
	records := r.dead.records(t) // newest first
	if r.dead.listKey != deadLetterKey || len(records) != 5 {
		t.Fatalf("key %q, %d records", r.dead.listKey, len(records))
	}
	requireRecord(t, records[0], "session", RejectUnreadable, "{")
	requireRecord(t, records[1], "session", RejectNotObject, "null")
	requireRecord(t, records[4], "session", RejectNotObject, "5")
	if r.counted.rejected["session/not_object"] != 4 || r.counted.rejected["session/unreadable_json"] != 1 {
		t.Fatalf("counted %+v", r.counted.rejected)
	}
}

func TestSessionFlushKeepsEveryRowWhenTheRecordFails(t *testing.T) {
	r := newRig()
	r.dead.err = errListDown
	r.redis.Push(SessionKey, "5", sessionRow("ok", 1, 1))
	observation := r.flush(t, NewSession(buffersConfig(t, nil), r.deps, Overrides{}))
	if observation.Result != flush.ResultError || len(r.redis.List(SessionKey)) != 2 || len(r.ch.inserts) != 0 || r.counted.failed != 1 {
		t.Fatalf("observation %+v, %d left, %d inserts, %d failed", observation, len(r.redis.List(SessionKey)), len(r.ch.inserts), r.counted.failed)
	}
}

func TestFlushWithoutARejectSinkKeepsTheRowsInsteadOfDroppingThem(t *testing.T) {
	r := newRig()
	r.deps.Rejects = nil
	r.redis.Push(SessionKey, "5", sessionRow("ok", 1, 1))
	observation := r.flush(t, NewSession(buffersConfig(t, nil), r.deps, Overrides{}))
	if observation.Result != flush.ResultError || len(r.redis.List(SessionKey)) != 2 {
		t.Fatalf("observation %+v", observation)
	}
}

func TestProfileFlushRecordsUnreadableRowsBeforeTrimmingThem(t *testing.T) {
	r := newRig()
	r.redis.Push(ProfileKey, "not json", "null", "7", "[]")
	observation := r.flush(t, NewProfile(buffersConfig(t, nil), r.deps, Overrides{}))
	if observation.Result != flush.ResultSuccess || len(r.redis.List(ProfileKey)) != 0 || len(r.ch.inserts) != 0 {
		t.Fatalf("observation %+v", observation)
	}
	records := r.dead.records(t)
	if len(records) != 4 {
		t.Fatalf("%d records", len(records))
	}
	requireRecord(t, records[3], "profile", RejectUnreadable, "not json")
	requireRecord(t, records[0], "profile", RejectNotObject, "[]")
	if r.counted.rejected["profile/not_object"] != 3 || r.counted.rejected["profile/unreadable_json"] != 1 {
		t.Fatalf("counted %+v", r.counted.rejected)
	}
}

func TestProfileFlushRecordsTheBadRowsOfAMixedBatchAndInsertsTheRest(t *testing.T) {
	r := newRig()
	r.redis.Push(ProfileKey, "null", profileLine("ok", "A", ""))
	observation := r.flush(t, NewProfile(buffersConfig(t, nil), r.deps, Overrides{}))
	if observation.Result != flush.ResultSuccess || len(r.ch.insertedLines()) != 1 || len(r.dead.records(t)) != 1 {
		t.Fatalf("observation %+v", observation)
	}
}

func TestProfileFlushKeepsTheRowsWhenTheRecordFails(t *testing.T) {
	r := newRig()
	r.dead.err = errListDown
	r.redis.Push(ProfileKey, "null")
	observation := r.flush(t, NewProfile(buffersConfig(t, nil), r.deps, Overrides{}))
	if observation.Result != flush.ResultError || len(r.redis.List(ProfileKey)) != 1 || r.counted.failed != 1 {
		t.Fatalf("observation %+v", observation)
	}
}

func TestBackfillRecordsRowsWithoutThreeStringsBeforeTrimmingThem(t *testing.T) {
	r := newRig()
	r.redis.Push(ProfileBackfillKey, `{"projectId":"p","sessionId":"s"}`, `[1]`, `{"projectId":"p","sessionId":"s","profileId":"u"}`)
	observation := r.flush(t, NewProfileBackfill(buffersConfig(t, nil), r.deps, Overrides{}))
	if observation.Result != flush.ResultSuccess || len(r.redis.List(ProfileBackfillKey)) != 0 || len(r.ch.commands) != 1 {
		t.Fatalf("observation %+v", observation)
	}
	records := r.dead.records(t)
	if len(records) != 2 {
		t.Fatalf("%d records", len(records))
	}
	requireRecord(t, records[1], "profile-backfill", RejectMissingFields, `{"projectId":"p","sessionId":"s"}`)
	requireRecord(t, records[0], "profile-backfill", RejectNotObject, "[1]")
}

func TestBackfillKeepsTheRowsWhenTheRecordFails(t *testing.T) {
	r := newRig()
	r.dead.err = errListDown
	r.redis.Push(ProfileBackfillKey, `[1]`)
	observation := r.flush(t, NewProfileBackfill(buffersConfig(t, nil), r.deps, Overrides{}))
	if observation.Result != flush.ResultError || len(r.redis.List(ProfileBackfillKey)) != 1 || len(r.ch.commands) != 0 {
		t.Fatalf("observation %+v", observation)
	}
}

func TestRejectsKeepTheMostRecentEntriesOnly(t *testing.T) {
	list := &memList{}
	rejects := NewRedisRejects(list, fixedClock{}, 3, nil)
	var rows []RejectedRow
	for _, raw := range []string{"1", "2", "3", "4", "5"} {
		rows = append(rows, RejectedRow{Reason: RejectNotObject, Raw: raw})
	}
	if err := rejects.Reject(context.Background(), "session", rows); err != nil {
		t.Fatal(err)
	}
	records := list.records(t)
	if len(records) != 3 {
		t.Fatalf("%d records", len(records))
	}
	requireRecord(t, records[0], "session", RejectNotObject, "5")
	requireRecord(t, records[2], "session", RejectNotObject, "3")
}

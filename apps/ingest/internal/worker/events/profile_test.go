package events

import (
	"context"
	"testing"

	"openpanel/ingest/internal/jsjson"
)

func TestProfileRowKeepsKeyOrderNumbersAndDropsEmptyValues(t *testing.T) {
	payload := makePayload(baseTime, map[string]*jsjson.Value{
		"profileId": str("wparity-user"), "country": str("GB"), "city": str("London"), "region": str(""),
		"longitude": num(-0.1), "latitude": num(51.5), "os": str("Mac OS"), "referrer": str("https://www.google.com"),
	})
	row := profileRow(payload.obj, fixedClock{now: fixedNow})
	got, err := stringify(jsjson.NewObject(row))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"id":"wparity-user","first_name":"","last_name":"","email":"","avatar":"","properties":{"path":"/home","country":"GB","city":"London","longitude":-0.1,"latitude":51.5,"os":"Mac OS","referrer":"https://www.google.com"},"project_id":"project-1","created_at":"2026-11-22 12:00:00","last_seen_at":"2026-11-22 12:00:00","is_external":true,"groups":[]}`
	if got != want {
		t.Fatalf("\n got: %s\nwant: %s", got, want)
	}
}

func TestProfileAddSkipsACachedProfileOnlyWhenItComesFromAnEvent(t *testing.T) {
	redis := newFakeRedis()
	redis.existsCache["profile-cache:project-1:wparity-user"] = true
	metrics := &recordingMetrics{}
	buffer := NewProfileBuffer(redis, metrics, nil)
	row := profileRow(makePayload(baseTime, map[string]*jsjson.Value{"profileId": str("wparity-user")}).obj, fixedClock{now: fixedNow})

	buffer.Add(context.Background(), row, true)
	if len(redis.list(ProfileBufferKey)) != 0 || len(metrics.skipped) != 1 || metrics.added["profile"] != 0 {
		t.Fatalf("list %v skipped %v added %v", redis.list(ProfileBufferKey), metrics.skipped, metrics.added)
	}
	buffer.Add(context.Background(), row, false)
	if len(redis.list(ProfileBufferKey)) != 1 || metrics.added["profile"] != 1 {
		t.Fatalf("list %v added %v", redis.list(ProfileBufferKey), metrics.added)
	}
}

func TestProfileAddSwallowsRedisErrors(t *testing.T) {
	redis := newFakeRedis()
	redis.failRPush = func(string, []string) error { return errRedisDown }
	buffer := NewProfileBuffer(redis, nil, nil)
	buffer.Add(context.Background(), profileRow(makePayload(baseTime, nil).obj, fixedClock{now: fixedNow}), true)
}

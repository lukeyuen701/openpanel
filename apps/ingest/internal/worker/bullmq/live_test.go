package bullmq

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/redis/rueidis"
)

// liveDB is the Redis database the live test owns and flushes; the TS golden
// script uses 13. Never change it to a database anything else uses.
const liveDB = 14

const liveConnectTimeout = 2 * time.Second

func liveClient(t *testing.T) rueidis.Client {
	t.Helper()
	url := os.Getenv("WPARITY_REDIS_URL")
	if url == "" {
		t.Skip("set WPARITY_REDIS_URL to run against a live Redis")
	}
	options, err := rueidis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	options.SelectDB = liveDB
	options.DisableCache = true
	options.Dialer.Timeout = liveConnectTimeout
	client, err := rueidis.NewClient(options)
	if err != nil {
		t.Skipf("redis unavailable: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

type keySnapshot struct {
	String *string           `json:"string"`
	Hash   map[string]string `json:"hash"`
	List   []string          `json:"list"`
	Zset   []string          `json:"zset"`
	Set    []string          `json:"set"`
	Stream [][]string        `json:"stream"`
}

// normalised turns a snapshot into comparable values: stream fields as maps
// (their ids are server time), and a job's opts as parsed JSON (the script
// re-encodes it with cjson, whose key order is not a contract).
func (s keySnapshot) normalised(t *testing.T) any {
	t.Helper()
	out := map[string]any{}
	if s.String != nil {
		out["string"] = *s.String
	}
	if s.Hash != nil {
		hash := map[string]any{}
		for field, value := range s.Hash {
			hash[field] = value
			if field == "opts" {
				var parsed any
				if err := json.Unmarshal([]byte(value), &parsed); err != nil {
					t.Fatalf("opts %q: %v", value, err)
				}
				hash[field] = parsed
			}
		}
		out["hash"] = hash
	}
	if s.List != nil {
		out["list"] = s.List
	}
	if s.Zset != nil {
		out["zset"] = s.Zset
	}
	if s.Set != nil {
		out["set"] = s.Set
	}
	if s.Stream != nil {
		entries := make([]map[string]string, 0, len(s.Stream))
		for _, fields := range s.Stream {
			entry := map[string]string{}
			for i := 0; i+1 < len(fields); i += 2 {
				entry[fields[i]] = fields[i+1]
			}
			entries = append(entries, entry)
		}
		out["stream"] = entries
	}
	return out
}

func liveSnapshot(ctx context.Context, t *testing.T, client rueidis.Client) map[string]keySnapshot {
	t.Helper()
	keys, err := client.Do(ctx, client.B().Keys().Pattern("*").Build()).AsStrSlice()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]keySnapshot{}
	for _, key := range keys {
		kind, err := client.Do(ctx, client.B().Type().Key(key).Build()).ToString()
		if err != nil {
			t.Fatal(err)
		}
		var snap keySnapshot
		switch kind {
		case "string":
			value, err := client.Do(ctx, client.B().Get().Key(key).Build()).ToString()
			snap.String = &value
			check(t, err)
		case "hash":
			snap.Hash, err = client.Do(ctx, client.B().Hgetall().Key(key).Build()).AsStrMap()
			check(t, err)
		case "list":
			snap.List, err = client.Do(ctx, client.B().Lrange().Key(key).Start(0).Stop(-1).Build()).AsStrSlice()
			check(t, err)
		case "zset":
			scores, err := client.Do(ctx, client.B().Zrange().Key(key).Min("0").Max("-1").Withscores().Build()).AsZScores()
			check(t, err)
			snap.Zset = []string{}
			for _, score := range scores {
				snap.Zset = append(snap.Zset, score.Member, strconv.FormatFloat(score.Score, 'f', -1, 64))
			}
		case "set":
			snap.Set, err = client.Do(ctx, client.B().Smembers().Key(key).Build()).AsStrSlice()
			check(t, err)
		case "stream":
			entries, err := client.Do(ctx, client.B().Xrange().Key(key).Start("-").End("+").Build()).AsXRange()
			check(t, err)
			snap.Stream = [][]string{}
			for _, entry := range entries {
				fields := []string{}
				for field, value := range entry.FieldValues {
					fields = append(fields, field, value)
				}
				snap.Stream = append(snap.Stream, fields)
			}
		default:
			t.Fatalf("unexpected type %s for %s", kind, key)
		}
		out[key] = snap
	}
	return out
}

func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// TestLiveMatchesBullMQ replays the jobs golden-bullmq.ts added with the real
// BullMQ and compares every key and field it left behind.
func TestLiveMatchesBullMQ(t *testing.T) {
	client := liveClient(t)
	ctx := context.Background()
	check(t, client.Do(ctx, client.B().Flushdb().Build()).Error())
	t.Cleanup(func() { client.Do(context.Background(), client.B().Flushdb().Build()) })

	var golden goldenCases
	readGolden(t, "cases.json", &golden)
	var want map[string]keySnapshot
	readGolden(t, "snapshot.json", &want)

	ids := replay(t, NewRueidis(client), golden)
	if !reflect.DeepEqual(ids, golden.IDs) {
		t.Errorf("job ids %q, BullMQ returned %q", ids, golden.IDs)
	}

	got := liveSnapshot(ctx, t, client)
	for key := range want {
		if _, ok := got[key]; !ok {
			t.Errorf("missing key %s", key)
		}
	}
	for key, snap := range got {
		expected, ok := want[key]
		if !ok {
			t.Errorf("unexpected key %s", key)
			continue
		}
		if !reflect.DeepEqual(snap.normalised(t), expected.normalised(t)) {
			t.Errorf("key %s:\n got %+v\nwant %+v", key, snap.normalised(t), expected.normalised(t))
		}
	}
}

// TestLiveAddWithCustomID adds one namespaced job with a custom id.
func TestLiveAddWithCustomID(t *testing.T) {
	client := liveClient(t)
	ctx := context.Background()
	check(t, client.Do(ctx, client.B().Flushdb().Build()).Error())
	t.Cleanup(func() { client.Do(context.Background(), client.B().Flushdb().Build()) })

	producer, err := NewProducer(Config{Redis: NewRueidis(client), Queues: []Queue{SessionsQueue()}, Namespace: "wparity"})
	check(t, err)
	id, err := producer.Add(ctx, "sessions", SessionJob, Envelope([]byte("{}"), ""), AddOptions{JobID: "sessionEnd:v2:x"})
	check(t, err)
	if id != "sessionEnd:v2:x" {
		t.Errorf("id %q", id)
	}
}

package consumer

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/rueidis"
)

const redisIntegrationEnv = "WPARITY_REDIS"

// TestRueidisListStoreWritesTheCappedList runs the real MULTI/LPUSH/LTRIM/EXEC
// against Redis, on a wparity-prefixed key it removes afterwards. Set
// WPARITY_REDIS to a URL of db 11-15, e.g. redis://127.0.0.1:23379/11.
func TestRueidisListStoreWritesTheCappedList(t *testing.T) {
	url := os.Getenv(redisIntegrationEnv)
	if url == "" {
		t.Skipf("%s is not set", redisIntegrationEnv)
	}
	options, err := rueidis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	options.DisableCache = true
	client, err := rueidis.NewClient(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close) // runs after the key cleanups registered below
	const key = "wparity:dead_letter:consumer-test"
	ctx := context.Background()
	t.Cleanup(func() { client.Do(ctx, client.B().Del().Key(key).Build()) })

	store := NewRueidisListStore(client, 2*time.Second)
	for _, value := range []string{"a", "b", "c", "d"} {
		if err := store.PushCapped(ctx, key, value, 3); err != nil {
			t.Fatal(err)
		}
	}
	got, err := client.Do(ctx, client.B().Lrange().Key(key).Start(0).Stop(-1).Build()).AsStrSlice()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0] != "d" || got[2] != "b" {
		t.Fatalf("list = %v, want [d c b]", got)
	}

	// A key of another type makes LPUSH fail inside the MULTI: reported, not swallowed.
	const wrongType = "wparity:dead_letter:consumer-test-string"
	client.Do(ctx, client.B().Set().Key(wrongType).Value("x").Build())
	t.Cleanup(func() { client.Do(ctx, client.B().Del().Key(wrongType).Build()) })
	if err := store.PushCapped(ctx, wrongType, "a", 3); err == nil {
		t.Fatal("a WRONGTYPE inside the MULTI must fail the write")
	}
}

package kafka

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestPartitionsMatchKafkaJS holds the mapping to the fixture kafkajs 2.2.4
// recorded, which the Bun producer is also held to.
func TestPartitionsMatchKafkaJS(t *testing.T) {
	data, err := os.ReadFile("../../../../packages/core/src/modules/ingest/src/partitioner.fixture.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Counts []int                `json:"counts"`
		Keys   [][2]json.RawMessage `json:"keys"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	topic := Partitioner().ForTopic("events")
	for _, entry := range fixture.Keys {
		var key string
		var want []int
		if err := json.Unmarshal(entry[0], &key); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(entry[1], &want); err != nil {
			t.Fatal(err)
		}
		for i, count := range fixture.Counts {
			got := topic.Partition(&kgo.Record{Key: []byte(key)}, count)
			if got != want[i] {
				t.Errorf("key %s with %d partitions: got %d want %d", key, count, got, want[i])
			}
		}
	}
}

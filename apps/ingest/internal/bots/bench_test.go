package bots

import (
	"encoding/json"
	"os"
	"testing"
)

func loadInputs(b *testing.B) []string {
	data, err := os.ReadFile(os.Getenv("INGEST_BENCH_UAS"))
	if err != nil {
		b.Skip("INGEST_BENCH_UAS not set")
	}
	var uas []string
	if err := json.Unmarshal(data, &uas); err != nil {
		b.Fatal(err)
	}
	return uas
}

func BenchmarkDetectUncached(b *testing.B) {
	uas := loadInputs(b)
	d, err := New()
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.detect(uas[i%len(uas)])
	}
}

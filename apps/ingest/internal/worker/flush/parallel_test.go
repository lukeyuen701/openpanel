package flush

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestParallelLimitCapsConcurrency(t *testing.T) {
	var running, peak atomic.Int32
	items := make([]int, 20)
	err := ParallelLimit(3, items, func(int, int) error {
		now := running.Add(1)
		for {
			seen := peak.Load()
			if now <= seen || peak.CompareAndSwap(seen, now) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		running.Add(-1)
		return nil
	})
	if err != nil || peak.Load() != 3 {
		t.Fatalf("err=%v peak=%d", err, peak.Load())
	}
}

func TestParallelLimitStopsStartingAfterAFailureAndWaitsForTheRunning(t *testing.T) {
	var started, finished atomic.Int32
	boom := errors.New("boom")
	err := ParallelLimit(2, make([]int, 10), func(index int, _ int) error {
		started.Add(1)
		if index == 0 {
			return boom
		}
		time.Sleep(50 * time.Millisecond)
		finished.Add(1)
		return nil
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if started.Load() >= 10 {
		t.Fatalf("%d items started despite the failure", started.Load())
	}
	if finished.Load() != started.Load()-1 {
		t.Fatalf("returned before the running items settled: started %d, finished %d", started.Load(), finished.Load())
	}
}

func TestParallelLimitSequentialCases(t *testing.T) {
	for _, tc := range []struct{ concurrency, items int }{{1, 5}, {0, 5}, {5, 1}} {
		var mutex sync.Mutex
		var order []int
		err := ParallelLimit(tc.concurrency, make([]int, tc.items), func(index, _ int) error {
			mutex.Lock()
			order = append(order, index)
			mutex.Unlock()
			return nil
		})
		if err != nil || len(order) != tc.items {
			t.Fatalf("%+v: %v %v", tc, err, order)
		}
		for i, got := range order {
			if got != i {
				t.Fatalf("%+v: ran out of order: %v", tc, order)
			}
		}
	}
	if err := ParallelLimit(3, []int{}, func(int, int) error { t.Fatal("ran"); return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestChunks(t *testing.T) {
	got := Chunks([]int{1, 2, 3, 4, 5}, 2)
	if len(got) != 3 || len(got[2]) != 1 || got[0][1] != 2 {
		t.Fatalf("%v", got)
	}
	if got := Chunks([]int{1, 2, 3}, 0); len(got) != 1 || len(got[0]) != 3 {
		t.Fatalf("size 0 is one chunk: %v", got)
	}
	if got := Chunks([]int{}, 5); len(got) != 0 {
		t.Fatalf("%v", got)
	}
}

package chclient

import (
	"testing"
	"time"
)

func TestSingleNodePickerAlwaysReturnsIt(t *testing.T) {
	p := newPicker(1, time.Minute)
	now := time.Unix(100, 0)
	p.markUnhealthy(0, now)
	for range 3 {
		if p.pick(now) != 0 {
			t.Fatal("single node")
		}
	}
}

func TestPickerRotatesAndSkipsAMarkedNodeUntilItsMarkExpires(t *testing.T) {
	p := newPicker(3, 5*time.Second)
	now := time.Unix(100, 0)
	if got := []int{p.pick(now), p.pick(now), p.pick(now), p.pick(now)}; got[0] != 0 || got[1] != 1 || got[2] != 2 || got[3] != 0 {
		t.Fatalf("round robin: %v", got)
	}
	p.markUnhealthy(1, now)
	for _, want := range []int{2, 0, 2, 0} {
		if got := p.pick(now); got != want {
			t.Fatalf("picked %d, want %d", got, want)
		}
	}
	later := now.Add(5 * time.Second)
	seen := map[int]bool{}
	for range 3 {
		seen[p.pick(later)] = true
	}
	if !seen[1] {
		t.Fatal("node 1 stays marked after its mark expired")
	}
}

func TestPickerTriesTheNextNodeWhenEveryNodeIsMarked(t *testing.T) {
	p := newPicker(2, time.Minute)
	now := time.Unix(100, 0)
	p.markUnhealthy(0, now)
	p.markUnhealthy(1, now)
	// The fallback picks the next node in turn and forgives it, so the call
	// after it finds that node healthy again, as in RoundRobinPicker.next.
	if first, second := p.pick(now), p.pick(now); first != 0 || second != 0 {
		t.Fatalf("picked %d then %d", first, second)
	}
}

func TestMarkHealthyClearsTheMark(t *testing.T) {
	p := newPicker(2, time.Minute)
	now := time.Unix(100, 0)
	p.markUnhealthy(0, now)
	p.markHealthy(0)
	if got := []int{p.pick(now), p.pick(now)}; got[0] != 0 || got[1] != 1 {
		t.Fatalf("after markHealthy: %v", got)
	}
}

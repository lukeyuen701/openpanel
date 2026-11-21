package chclient

import (
	"sync"
	"time"
)

// picker is RoundRobinPicker: nodes in turn, skipping the ones marked
// unhealthy; when every node is marked, the next one is tried anyway.
type picker struct {
	mutex          sync.Mutex
	size           int
	next           int
	unhealthyUntil map[int]time.Time
	markFor        time.Duration
}

func newPicker(size int, markFor time.Duration) *picker {
	return &picker{size: size, markFor: markFor, unhealthyUntil: map[int]time.Time{}}
}

func (p *picker) pick(now time.Time) int {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.size == 1 {
		return 0
	}
	for range p.size {
		index := p.next
		p.next = (p.next + 1) % p.size
		until, marked := p.unhealthyUntil[index]
		if !marked || !until.After(now) {
			return index
		}
	}
	index := p.next
	p.next = (p.next + 1) % p.size
	delete(p.unhealthyUntil, index)
	return index
}

func (p *picker) markUnhealthy(index int, now time.Time) {
	if p.size == 1 {
		return
	}
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.unhealthyUntil[index] = now.Add(p.markFor)
}

func (p *picker) markHealthy(index int) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	delete(p.unhealthyUntil, index)
}

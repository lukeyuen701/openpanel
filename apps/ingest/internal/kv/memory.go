package kv

import (
	"context"
	"sync"
	"time"
)

// Memory is an in-process Store for tests.
type Memory struct {
	mu      sync.Mutex
	values  map[string]string
	expires map[string]time.Time
}

// NewMemory returns an empty Memory.
func NewMemory() *Memory {
	return &Memory{values: map[string]string{}, expires: map[string]time.Time{}}
}

func (m *Memory) live(key string) (string, bool) {
	if deadline, ok := m.expires[key]; ok && time.Now().After(deadline) {
		delete(m.values, key)
		delete(m.expires, key)
	}
	value, ok := m.values[key]
	return value, ok
}

// Set stores a value with no TTL.
func (m *Memory) Set(key, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.values[key] = value
	delete(m.expires, key)
}

// Get implements Store.
func (m *Memory) Get(_ context.Context, key string) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	value, ok := m.live(key)
	return value, ok, nil
}

// SetNX implements Store.
func (m *Memory) SetNX(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.live(key); ok {
		return false, nil
	}
	m.values[key] = value
	m.expires[key] = time.Now().Add(ttl)
	return true, nil
}

// MGet implements Store.
func (m *Memory) MGet(_ context.Context, keys []string) ([]*string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*string, len(keys))
	for i, key := range keys {
		if value, ok := m.live(key); ok {
			out[i] = &value
		}
	}
	return out, nil
}

// SetEx implements Store.
func (m *Memory) SetEx(_ context.Context, key, value string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.values[key] = value
	m.expires[key] = time.Now().Add(ttl)
	return nil
}

// Ping implements Store.
func (m *Memory) Ping(context.Context) error { return nil }

// Close implements Store.
func (m *Memory) Close() {}

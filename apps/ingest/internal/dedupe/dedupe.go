// Package dedupe collapses the web SDK's double fire: a 100 ms Redis lock
// keyed on the stable hash of the payload, the ip, the origin and the client.
package dedupe

import (
	"context"
	"fmt"
	"time"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/kv"
)

const (
	keyPrefix = "fastify:deduplicate:"
	lockTTL   = 100 * time.Millisecond
)

// Key is the lock key isDuplicatedEvent sets: {...payload, ip, origin,
// projectId} hashed, where projectId is the client id header.
func Key(payload *jsjson.Value, ip, origin, clientID string) string {
	merged := jsjson.NewObj()
	if payload.IsObject() {
		for _, key := range payload.Obj.Keys() {
			merged.Set(key, payload.Obj.Get(key))
		}
	}
	merged.Set("ip", jsjson.NewString(ip))
	merged.Set("origin", jsjson.NewString(origin))
	merged.Set("projectId", jsjson.NewString(clientID))
	return keyPrefix + jsjson.StableMD5(jsjson.NewObject(merged))
}

// Checker takes the lock.
type Checker struct {
	Redis kv.Store
}

// IsDuplicate reports whether the same request arrived within the window.
func (c Checker) IsDuplicate(ctx context.Context, key string) (bool, error) {
	ok, err := c.Redis.SetNX(ctx, key, "1", lockTTL)
	if err != nil {
		return false, fmt.Errorf("dedupe: take lock: %w", err)
	}
	return !ok, nil
}

package identity

import (
	"context"
	"fmt"

	"openpanel/ingest/internal/kv"
)

// RedisSessions reads the worker's session blobs: session:<project>:<device>.
type RedisSessions struct {
	Client kv.Store
}

func sessionKey(projectID, deviceID string) string {
	return "session:" + projectID + ":" + deviceID
}

// ExistingSessions implements SessionReader with one MGET.
func (r RedisSessions) ExistingSessions(ctx context.Context, projectID string, deviceIDs []string) ([]*LiveSession, error) {
	if len(deviceIDs) == 0 {
		return nil, nil
	}
	keys := make([]string, len(deviceIDs))
	for i, deviceID := range deviceIDs {
		keys[i] = sessionKey(projectID, deviceID)
	}
	values, err := r.Client.MGet(ctx, keys)
	if err != nil {
		return nil, fmt.Errorf("identity: read session blobs: %w", err)
	}
	out := make([]*LiveSession, len(keys))
	for i, value := range values {
		if value != nil {
			out[i] = DecodeSession(*value)
		}
	}
	return out, nil
}

package identity

import (
	"context"
	"encoding/json"
	"os"
	"testing"
)

func readGolden(t *testing.T, name string, into any) {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	if err := json.Unmarshal(data, into); err != nil {
		t.Fatalf("decode %s: %v", name, err)
	}
}

func TestDeviceIDMatchesTypeScript(t *testing.T) {
	var cases []struct{ Salt, UA, IP, Origin, ID string }
	readGolden(t, "deviceid.json", &cases)
	for _, c := range cases {
		if got := DeviceID(c.Salt, c.UA, c.IP, c.Origin); got != c.ID {
			t.Errorf("DeviceID(%q,%q,%q,%q) = %s, want %s", c.Salt, c.UA, c.IP, c.Origin, got, c.ID)
		}
	}
}

type stubSessions []*LiveSession

func (s stubSessions) ExistingSessions(context.Context, string, []string) ([]*LiveSession, error) {
	return s, nil
}

func TestResolveMatchesTypeScript(t *testing.T) {
	var cases []struct {
		ProjectID   string
		DeviceID    string
		EventTimeMs int64
		Timeout     int64
		Sessions    []*LiveSession
		Result      struct{ DeviceID, SessionID string }
	}
	readGolden(t, "session.json", &cases)
	for _, c := range cases {
		r := Resolver{Sessions: stubSessions(c.Sessions), TimeoutMs: c.Timeout}
		got, err := r.Resolve(context.Background(), c.ProjectID, []string{c.DeviceID}, c.EventTimeMs)
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if got.DeviceID != c.Result.DeviceID || got.SessionID != c.Result.SessionID {
			t.Errorf("%+v: got %+v want %+v", c, got, c.Result)
		}
	}
}

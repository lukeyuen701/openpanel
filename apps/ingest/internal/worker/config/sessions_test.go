package config

import (
	"strings"
	"testing"
	"time"
)

func envOf(pairs map[string]string) func(string) string {
	return func(name string) string { return pairs[name] }
}

func TestGoWorkerQueuesAcceptOnlySessions(t *testing.T) {
	cfg, err := Load(envOf(map[string]string{"ROLE": "worker", "ENABLED_QUEUES": "cron", "GO_WORKER_QUEUES": "sessions", "SESSIONS_CONCURRENCY": "4", "KAFKA_BROKERS": "b:1"}))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.GoWorkerQueues) != 1 || cfg.GoWorkerQueues[0] != "sessions" || cfg.QueueConcurrency["sessions"] != 4 {
		t.Fatalf("queues = %v, concurrency = %v", cfg.GoWorkerQueues, cfg.QueueConcurrency)
	}
	if cfg.ConsumesEvents() {
		t.Error("ENABLED_QUEUES=cron does not start the events consumer")
	}
	_, err = Load(envOf(map[string]string{"ROLE": "worker", "GO_WORKER_QUEUES": "sessions,import", "KAFKA_BROKERS": "b:1"}))
	if err == nil || !strings.Contains(err.Error(), `unknown queue "import". Accepted values: sessions`) {
		t.Fatalf("err = %v", err)
	}
	cfg, err = Load(envOf(map[string]string{"ROLE": "worker", "GO_WORKER_QUEUES": "sessions", "SESSIONS_CONCURRENCY": "abc", "KAFKA_BROKERS": "b:1"}))
	if err != nil || cfg.QueueConcurrency["sessions"] != 1 {
		t.Fatalf("a malformed concurrency keeps the default: %v, %v", cfg.QueueConcurrency, err)
	}
}

func TestSessionsConfigDefaultsAndInvariants(t *testing.T) {
	cfg, err := Load(envOf(map[string]string{"ROLE": "worker", "KAFKA_BROKERS": "b:1"}))
	if err != nil {
		t.Fatal(err)
	}
	s := cfg.Sessions
	if !s.ReaperEnabled || !s.VacuumEnabled || s.ReaperBatch != 5000 || s.VacuumBatch != 1000 ||
		s.ReaperDeadman != 30*time.Minute || s.VacuumStale != 7*24*time.Hour || s.ProfileBackfill {
		t.Fatalf("defaults = %+v", s)
	}
	cfg, err = Load(envOf(map[string]string{"ROLE": "worker", "KAFKA_BROKERS": "b:1", "SESSION_REAPER": "0", "SESSION_VACUUM": "0", "EXPERIMENTAL_PROFILE_BACKFILL": "1", "EXPERIMENTAL_PROFILE_BACKFILL_PROJECTS": "a, b"}))
	if err != nil || cfg.Sessions.ReaperEnabled || cfg.Sessions.VacuumEnabled || !cfg.Sessions.ProfileBackfill || len(cfg.Sessions.ProfileBackfillProjects) != 2 {
		t.Fatalf("cfg = %+v, err = %v", cfg.Sessions, err)
	}
	for name, env := range map[string]map[string]string{
		"deadman below timeout":  {"SESSION_REAPER_WALLCLOCK_DEADMAN_MS": "1000", "SESSION_TIMEOUT_MS": "2000"},
		"vacuum below deadman":   {"SESSION_VACUUM_STALE_THRESHOLD_MS": "1000", "SESSION_TIMEOUT_MS": "2000"},
		"allowlist without flag": {"EXPERIMENTAL_PROFILE_BACKFILL_PROJECTS": "a"},
	} {
		env["ROLE"], env["KAFKA_BROKERS"] = "worker", "b:1"
		if _, err := Load(envOf(env)); err == nil {
			t.Errorf("%s: expected the load to fail", name)
		}
	}
}

func TestAProcessWithOnlyQueuesOrCronJobsNeedsNoEnabledQueues(t *testing.T) {
	if _, err := Load(envOf(map[string]string{"ROLE": "worker", "ENABLED_QUEUES": "cron", "GO_WORKER_CRON_JOBS": "sessionReaper"})); err != nil {
		t.Fatal(err)
	}
}

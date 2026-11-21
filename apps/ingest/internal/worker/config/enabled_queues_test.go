package config

import "testing"

func TestTheEventsConsumerNeedsAnExplicitEnabledQueues(t *testing.T) {
	unset, err := Load(envOf(map[string]string{"ROLE": "worker", "GO_WORKER_CRON_JOBS": "sessionReaper", "KAFKA_BROKERS": "b:1"}))
	if err != nil {
		t.Fatal(err)
	}
	if unset.ConsumesEvents() {
		t.Error("an unset ENABLED_QUEUES (the Bun default: every queue) started the Go events consumer")
	}
	explicit, err := Load(envOf(map[string]string{"ROLE": "worker", "ENABLED_QUEUES": "events", "KAFKA_BROKERS": "b:1"}))
	if err != nil {
		t.Fatal(err)
	}
	if !explicit.ConsumesEvents() {
		t.Error("ENABLED_QUEUES=events did not start the events consumer")
	}
}

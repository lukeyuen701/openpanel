// The events consumer-loop heartbeat. It is what turns "the process is up"
// into "the Kafka consumer is still turning": the timestamp is refreshed on
// every idle fetch and as a batch progresses (consumer.ts's `onActivity`), so
// a healthy loop refreshes it even with no traffic, and a wedged one goes
// stale. Not on the client's heartbeats: those keep going while a batch hangs.
//
// Module state rather than a field on `AppDeps` because the readiness probe
// and the consumer are on opposite sides of the process and neither owns the
// other; there is exactly one consumer per process.

let enabled = false;
let lastActivityAt = Date.now();

/** Called only by a role that actually starts the consumer. */
export function enableEventsHeartbeat(): void {
  enabled = true;
  lastActivityAt = Date.now();
}

export function markEventsActivity(): void {
  lastActivityAt = Date.now();
}

export function getEventsHeartbeat(): {
  enabled: boolean;
  lastActivityAt: number;
} {
  return { enabled, lastActivityAt };
}

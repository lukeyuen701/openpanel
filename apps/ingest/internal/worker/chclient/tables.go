package chclient

// Table names the flushes write, the literals of packages/core/src/shared/ch-tables.ts.
// Inserts and reads use the plain name under CLICKHOUSE_CLUSTER too: there it
// is the Distributed table over `<name>_replicated`.
const (
	TableEvents       = "events"
	TableSessions     = "sessions"
	TableProfiles     = "profiles"
	TableBots         = "events_bots"
	TableReplayChunks = "session_replay_chunks"
	TableGroups       = "groups"
	TableAiEvents     = "ai_events"
)

// ReplicatedTarget is the table a mutation names (replicatedTarget in
// ch-tables.ts): only the profile backfill's UPDATE reaches below the
// Distributed table.
func ReplicatedTarget(clustered bool, table string) string {
	if clustered {
		return table + "_replicated ON CLUSTER '{cluster}'"
	}
	return table
}

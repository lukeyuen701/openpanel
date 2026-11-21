package sessions

import "time"

// Keys and TTLs shared with ingest and the Bun worker (P3.2).
const (
	// ProjectsKey is the set of projects with a live session.
	ProjectsKey = "session:projects"
	// ProfileBackfillKey is the list the profile-backfill flush drains.
	ProfileBackfillKey = "profile-backfill-buffer"

	claimTTL      = 2 * time.Hour
	reaperLockTTL = time.Minute
	// usageCacheTTL is how long updateEventsCount skips a project.
	usageCacheTTL = time.Hour
)

func sessionKey(projectID, deviceID string) string {
	return "session:" + projectID + ":" + deviceID
}

func wallclockKey(projectID string) string { return "session:wallclock:" + projectID }

func profilePointerKey(projectID, profileID string) string {
	return "session:profile:" + projectID + ":" + profileID
}

func claimKey(projectID, deviceID, sessionID string) string {
	return "session:end:emitted:" + projectID + ":" + deviceID + ":" + sessionID
}

func reaperLockKey(projectID string) string { return "session:reaper:lock:" + projectID }

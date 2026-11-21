package sessions

import (
	"context"
	"time"

	"openpanel/ingest/internal/worker/flush"
)

// vacuumHour is the hour of the daily vacuum ('0 4 * * *'), in the process's
// timezone: BullMQ's cron parser is given no tz.
const vacuumHour = 4

// nextVacuum is the next 04:00 local time after now.
func nextVacuum(now time.Time) time.Time {
	next := time.Date(now.Year(), now.Month(), now.Day(), vacuumHour, 0, 0, 0, now.Location())
	if !next.After(now) {
		next = next.AddDate(0, 0, 1)
	}
	return next
}

// CronJobs are the reaper and the vacuum, under the cron job ids of the Bun
// registry so GO_WORKER_CRON_JOBS selects them.
func CronJobs(reaper *Reaper, vacuum *Vacuum, paused flush.PauseCheck) []flush.Job {
	return []flush.Job{
		flush.NewTaskJob(ReaperJob, ReaperInterval, nil, paused, func(ctx context.Context) error { return reaper.Run(ctx) }),
		flush.NewTaskJob(VacuumJob, 24*time.Hour, nextVacuum, paused, func(ctx context.Context) error { return vacuum.Run(ctx) }),
	}
}

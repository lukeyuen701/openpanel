package sessions

import (
	"context"
	"log/slog"
	"time"

	"openpanel/ingest/internal/worker/events"
)

// VacuumConfig is the vacuum's environment.
type VacuumConfig struct {
	Enabled   bool
	BatchSize int
	// StaleThreshold is how old a wall-clock entry must be to be removed.
	StaleThreshold time.Duration
}

// Vacuum is vacuumStaleSessions: the daily backstop for state cleanup missed.
// It takes no lock: two workers racing both delete, which is harmless.
type Vacuum struct {
	Redis   Redis
	Clock   events.Clock
	Config  VacuumConfig
	Metrics Metrics
	Logger  *slog.Logger
}

// Run removes stale sessions of every project, one batch per project.
func (v *Vacuum) Run(ctx context.Context) error {
	if !v.Config.Enabled {
		return nil
	}
	logger := v.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	metrics := v.Metrics
	if metrics == nil {
		metrics = NopMetrics{}
	}
	projectIDs, err := v.Redis.SMembers(ctx, ProjectsKey)
	if err != nil || len(projectIDs) == 0 {
		return err
	}
	logger.Info("Vacuum tick starting", slog.Int("projectCount", len(projectIDs)))
	cutoff := v.Clock.Now().UnixMilli() - v.Config.StaleThreshold.Milliseconds()
	total, staleBlobs, missingBlobs := 0, 0, 0
	for _, projectID := range projectIDs {
		stale, missing, err := v.vacuumProject(ctx, metrics, projectID, cutoff)
		total += stale + missing
		staleBlobs += stale
		missingBlobs += missing
		if err != nil {
			logger.Error("Vacuum failed for project", slog.Any("err", err), slog.String("projectId", projectID))
		}
	}
	if total > 0 {
		logger.Info("Vacuum tick complete", slog.Int("total", total), slog.Int("staleBlobs", staleBlobs), slog.Int("missingBlobs", missingBlobs))
	}
	return nil
}

func (v *Vacuum) vacuumProject(ctx context.Context, metrics Metrics, projectID string, cutoff int64) (stale, missing int, err error) {
	candidates, err := v.Redis.ZRangeByScore(ctx, wallclockKey(projectID), cutoff, v.Config.BatchSize)
	if err != nil {
		return 0, 0, err
	}
	for _, deviceID := range candidates {
		session, err := getExistingSession(ctx, v.Redis, projectID, deviceID)
		if err != nil {
			return stale, missing, err
		}
		if isTruthy(session) {
			if err := cleanup(ctx, v.Redis, session); err != nil {
				return stale, missing, err
			}
			metrics.Vacuumed(reasonStaleBlob)
			stale++
			continue
		}
		if err := v.Redis.ZRem(ctx, wallclockKey(projectID), deviceID); err != nil {
			return stale, missing, err
		}
		metrics.Vacuumed(reasonMissing)
		missing++
	}
	return stale, missing, nil
}

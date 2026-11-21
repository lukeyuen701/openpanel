package sessions

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/chclient"
	"openpanel/ingest/internal/worker/jsport"
	"openpanel/ingest/internal/worker/notify"
)

const (
	usageCacheName       = "updateEventsCount"
	int4Max              = 2_147_483_647
	usageWarningFraction = 0.8
	trialingStatus       = "trialing"
	yearlyInterval       = "year"

	// UsageAlertQueue and UsageAlertJob are the Bun job that sends the e-mails.
	UsageAlertQueue = "notification"
	UsageAlertJob   = "sendUsageAlert"

	organizationStatement = `SELECT o."id", o."subscriptionStatus", o."createdAt", o."subscriptionStartsAt", o."subscriptionEndsAt",
  o."subscriptionInterval", o."subscriptionPeriodEventsLimit", o."subscriptionPeriodEventsCountExceededAt",
  o."usageExceededSentAt", o."usageWarningSentAt"
FROM "organizations" o
WHERE EXISTS (SELECT 1 FROM "projects" p WHERE p."organizationId" = o."id" AND p."id" = $1)
LIMIT 1`
	organizationProjectsStatement = `SELECT "id" FROM "projects" WHERE "organizationId" = $1`
	organizationCountStatement    = `SELECT COUNT(*) AS count
FROM events e
WHERE project_id IN {p1:Array(String)}
  AND created_at BETWEEN {p2:String} AND {p3:String}
  AND name NOT IN {p4:Array(String)}`
	projectCountStatement = `SELECT sum(event_count) as count FROM distinct_event_names_mv WHERE project_id = {p1:String} AND name NOT IN {p2:Array(String)}`
	updateProject         = `UPDATE "projects" SET "eventsCount" = $1, "updatedAt" = $2 WHERE "id" = $3`
	updateOrganization    = `UPDATE "organizations" SET "subscriptionPeriodEventsCount" = $1, "subscriptionPeriodEventsCountExceededAt" = $2, "updatedAt" = $3 WHERE "id" = $4`
)

var nonBillableEventNames = []string{"session_start", "session_end"}

// AlertQueue hands a usage alert to the Bun worker, which renders and sends
// the e-mails with the React Email templates.
type AlertQueue interface {
	EnqueueUsageAlert(ctx context.Context, organizationID string, count int64) error
}

// Usage is updateEventsCount: at most once an hour per project, the project's
// and organization's event counters, and the usage alerts.
type Usage struct {
	DB         notify.DB
	CH         Querier
	Redis      Redis
	Alerts     AlertQueue
	Clock      interface{ Now() time.Time }
	SelfHosted bool
	// PeriodsDisabled is process.env.SELF_HOSTED === 'true', which makes the
	// billing period null in the Prisma client (not the same switch as SelfHosted).
	PeriodsDisabled bool
	Logger          *slog.Logger
}

type organization struct {
	id                  string
	status              *string
	createdAt           time.Time
	startsAt, endsAt    *time.Time
	interval            *string
	eventsLimit         int32
	exceededAt          *time.Time
	usageExceededSentAt *time.Time
	usageWarningSentAt  *time.Time
	projectIDs          []string
}

// UpdateEventsCount refreshes the counters of projectID's organization.
func (u *Usage) UpdateEventsCount(ctx context.Context, projectID string) error {
	cacheKey := notify.CacheKey(usageCacheName, projectID)
	cached, found, err := u.Redis.Get(ctx, cacheKey)
	if err != nil {
		return err
	}
	if found && cached != "" {
		return nil
	}
	done, err := u.update(ctx, projectID)
	if err != nil || !done {
		return err
	}
	// Plain SETEX after the work, as cacheable does, and its failure is ignored.
	_ = u.Redis.SetEx(ctx, cacheKey, "true", usageCacheTTL)
	return nil
}

func (u *Usage) update(ctx context.Context, projectID string) (bool, error) {
	org, err := u.findOrganization(ctx, projectID)
	if err != nil || org == nil {
		return false, err
	}
	organizationCount, err := u.organizationCount(ctx, org)
	if err != nil {
		return false, err
	}
	projectCount, err := u.projectCount(ctx, projectID)
	if err != nil {
		return false, err
	}
	now := u.Clock.Now().UTC().Truncate(time.Millisecond)

	if projectCount != 0 {
		if _, err := u.DB.Exec(ctx, updateProject, clampInt4(projectCount), now, projectID); err != nil {
			return false, fmt.Errorf("sessions: update project: %w", err)
		}
	}
	if organizationCount == 0 {
		return true, nil
	}
	exceededAt := u.nextExceededAt(org, organizationCount, now)
	if _, err := u.DB.Exec(ctx, updateOrganization, clampInt4(organizationCount), exceededAt, now, org.id); err != nil {
		return false, fmt.Errorf("sessions: update organization: %w", err)
	}
	if !u.SelfHosted {
		if err := u.alert(ctx, org, organizationCount); err != nil {
			u.logger().Error("Failed to send usage alert emails", slog.Any("err", err), slog.String("organizationId", org.id))
		}
	}
	return true, nil
}

func (u *Usage) logger() *slog.Logger {
	if u.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return u.Logger
}

func clampInt4(count float64) int64 { return int64(math.Min(count, int4Max)) }

func (u *Usage) findOrganization(ctx context.Context, projectID string) (*organization, error) {
	rows, err := u.DB.Query(ctx, organizationStatement, projectID)
	if err != nil {
		return nil, fmt.Errorf("sessions: find organization: %w", err)
	}
	var org *organization
	for rows.Next() {
		found := organization{}
		if err := rows.Scan(&found.id, &found.status, &found.createdAt, &found.startsAt, &found.endsAt, &found.interval,
			&found.eventsLimit, &found.exceededAt, &found.usageExceededSentAt, &found.usageWarningSentAt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("sessions: find organization: %w", err)
		}
		org = &found
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sessions: find organization: %w", err)
	}
	if org == nil {
		return nil, nil
	}
	projects, err := u.DB.Query(ctx, organizationProjectsStatement, org.id)
	if err != nil {
		return nil, fmt.Errorf("sessions: organization projects: %w", err)
	}
	defer projects.Close()
	for projects.Next() {
		var id string
		if err := projects.Scan(&id); err != nil {
			return nil, fmt.Errorf("sessions: organization projects: %w", err)
		}
		org.projectIDs = append(org.projectIDs, id)
	}
	return org, projects.Err()
}

// currentPeriod is the computed subscriptionCurrentPeriodStart and End of the
// Prisma client.
func (u *Usage) currentPeriod(org *organization) (start, end *time.Time) {
	if u.PeriodsDisabled || org.startsAt == nil {
		return nil, nil
	}
	if org.interval != nil && *org.interval == yearlyInterval {
		now := u.Clock.Now().UTC()
		day := org.startsAt.UTC().Day()
		first := time.Date(now.Year(), now.Month(), day, 0, 0, 0, 0, time.UTC)
		last := time.Date(now.Year(), now.Month()+1, day-1, 0, 0, 0, 0, time.UTC)
		return &first, &last
	}
	return org.startsAt, org.endsAt
}

// organizationCount is getOrganizationBillingEventsCount; 0 stands for both 0
// and undefined, which the caller treats alike.
func (u *Usage) organizationCount(ctx context.Context, org *organization) (float64, error) {
	start, end := u.currentPeriod(org)
	trialing := org.status != nil && *org.status == trialingStatus
	if start == nil && trialing {
		start = &org.createdAt
	}
	if end == nil && trialing {
		end = org.endsAt
	}
	if start == nil || end == nil || len(org.projectIDs) == 0 {
		return 0, nil
	}
	params := []chclient.Param{
		{Name: "p1", Value: chclient.FormatStringArray(org.projectIDs)},
		{Name: "p2", Value: chclient.FormatString(formatClickhouseDate(float64(start.UnixMilli())))},
		{Name: "p3", Value: chclient.FormatString(formatClickhouseDate(float64(end.UnixMilli())))},
		{Name: "p4", Value: chclient.FormatStringArray(nonBillableEventNames)},
	}
	return u.count(ctx, organizationCountStatement, params)
}

func (u *Usage) projectCount(ctx context.Context, projectID string) (float64, error) {
	params := []chclient.Param{
		{Name: "p1", Value: chclient.FormatString(projectID)},
		{Name: "p2", Value: chclient.FormatStringArray(nonBillableEventNames)},
	}
	return u.count(ctx, projectCountStatement, params)
}

// count is res[0]?.count after chQuery's coercion of Int columns.
func (u *Usage) count(ctx context.Context, statement string, params []chclient.Param) (float64, error) {
	result, err := u.CH.Query(ctx, statement, params, nil)
	if err != nil {
		return 0, err
	}
	if len(result.Data) == 0 {
		return 0, nil
	}
	row, err := jsport.ParseJSONBytes(result.Data[0])
	if err != nil {
		return 0, fmt.Errorf("sessions: count row: %w", err)
	}
	value := row.Get("count")
	switch {
	case value == nil:
		return 0, nil
	case value.Kind == jsjson.String:
		return jsport.ParseFloat(value.Str), nil
	case value.Kind == jsjson.Number:
		return value.Num, nil
	}
	return 0, nil
}

// nextExceededAt keeps the marker of when the organization crossed its limit.
func (u *Usage) nextExceededAt(org *organization, count float64, now time.Time) *time.Time {
	if u.SelfHosted {
		return nil
	}
	limit := float64(org.eventsLimit)
	if count > limit && org.exceededAt == nil {
		return &now
	}
	if count <= limit {
		return nil
	}
	return org.exceededAt
}

// alert hands the 80% and 100% e-mails to the Bun worker when one is due; the
// worker claims the alert in Postgres before it sends, so a repeat is a no-op.
func (u *Usage) alert(ctx context.Context, org *organization, count float64) error {
	limit := float64(org.eventsLimit)
	if limit <= 0 {
		return nil
	}
	exceeded := count > limit && org.usageExceededSentAt == nil
	nearLimit := !exceeded && count >= limit*usageWarningFraction && count <= limit && org.usageWarningSentAt == nil
	if (!exceeded && !nearLimit) || u.Alerts == nil {
		return nil
	}
	return u.Alerts.EnqueueUsageAlert(ctx, org.id, int64(count))
}

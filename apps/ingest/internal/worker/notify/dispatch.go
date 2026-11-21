package notify

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"openpanel/ingest/internal/jsjson"
)

const (
	// QueueName and JobName address the notification queue's only job.
	QueueName = "notification"
	JobName   = "sendNotification"

	appIntegrationID   = "app"
	emailIntegrationID = "email"

	rowTimePrecision = time.Millisecond
)

var profilePlaceholder = regexp.MustCompile("{{profile\\.[^}]*}}")

// Clock is the wall clock the notification row's timestamps come from.
type Clock interface{ Now() time.Time }

// JobOptions are per-job BullMQ options. The zero value asks for the queue's
// defaults, which the Enqueuer owns (the notification queue: removeOnComplete
// 10, removeOnFail {age 86400, count 100}, attempts 0, no job id).
type JobOptions struct {
	JobID        string
	Attempts     int
	BackoffDelay time.Duration
}

// Enqueuer adds a job to a BullMQ queue and returns its id. data is the JSON
// text the job stores, the complete {payload, meta} envelope, so an
// implementation must not wrap it again; it applies the queue's default options.
type Enqueuer interface {
	Add(ctx context.Context, queue, name string, data []byte, opts JobOptions) (string, error)
}

// ProjectNames reads a project's name, "" when the project is unknown.
type ProjectNames interface {
	ProjectName(ctx context.Context, projectID string) (string, error)
}

// Profiles loads the profile a template refers to, nil when unknown.
type Profiles interface {
	ByID(ctx context.Context, profileID, projectID string) (*jsjson.Value, error)
}

// Rules reads a project's notification rules.
type Rules interface {
	ByProject(ctx context.Context, projectID string) ([]Rule, error)
}

// Dispatcher is checkNotificationRulesForEvent.
type Dispatcher struct {
	Matcher  *Matcher
	Projects ProjectNames
	Rules    Rules
	Profiles Profiles
	DB       DB
	Queue    Enqueuer
	Clock    Clock
}

// Check matches the event payload against the project's rules and delivers
// each match. It may attach the profile to payload (payload.profile), which
// the caller then stores with the event. requestID goes into the job envelope.
//
// A failed delivery does not stop the others; the first error is returned
// after all were tried. A rule whose matching throws (ErrTypeError) stops the
// evaluation of the rules after it, but the deliveries of the rules before it
// still run.
func (d *Dispatcher) Check(ctx context.Context, requestID string, payload *jsjson.Value) error {
	projectID := jsjson.JSString(payload.Get("projectId"))
	projectName, err := d.Projects.ProjectName(ctx, projectID)
	if err != nil {
		return err
	}
	rules, err := d.Rules.ByProject(ctx, projectID)
	if err != nil {
		return err
	}
	if err := d.attachProfile(ctx, payload, projectID, rules); err != nil {
		return err
	}

	var pending []delivery
	var matchErr error
	for _, rule := range rules {
		matched, err := d.matches(payload, rule)
		if err != nil {
			matchErr = err
			break
		}
		if !matched {
			continue
		}
		pending = append(pending, d.deliveries(payload, rule, projectID, projectName)...)
	}
	return errors.Join(d.deliver(ctx, requestID, pending), matchErr)
}

// attachProfile loads the profile once when any rule's template mentions one.
func (d *Dispatcher) attachProfile(ctx context.Context, payload *jsjson.Value, projectID string, rules []Rule) error {
	profileID := payload.Get("profileId")
	if !isTruthy(profileID) {
		return nil
	}
	needed := false
	for _, rule := range rules {
		if profilePlaceholder.MatchString(rule.Template) {
			needed = true
			break
		}
	}
	if !needed {
		return nil
	}
	profile, err := d.Profiles.ByID(ctx, jsjson.JSString(profileID), projectID)
	if err != nil {
		return err
	}
	if profile != nil {
		payload.Obj.Set("profile", profile)
	}
	return nil
}

func (d *Dispatcher) matches(payload *jsjson.Value, rule Rule) (bool, error) {
	if rule.Config == nil || rule.Config.Kind == jsjson.Null {
		return false, typeError("Cannot read properties of null (reading 'type')")
	}
	if kind := rule.Config.Get("type"); kind == nil || kind.Kind != jsjson.String || kind.Str != "events" {
		return false, nil
	}
	events := rule.Config.Get("events")
	if events == nil || events.Kind != jsjson.Array {
		return false, typeError("rule.config.events.find is not a function")
	}
	for _, event := range events.Arr {
		matched, err := d.Matcher.MatchEvent(payload, event)
		if err != nil || matched {
			return matched, err
		}
	}
	return false, nil
}

// delivery is one createNotification call.
type delivery struct {
	title, message, projectID string
	ruleID, integrationID     string
	payload                   *jsjson.Value
}

// deliveries are a match's notifications in creation order: one per
// integration, then the dashboard, then email.
func (d *Dispatcher) deliveries(payload *jsjson.Value, rule Rule, projectID, projectName string) []delivery {
	base := delivery{
		title:     TemplateEvent(payload, rule),
		projectID: projectID,
		ruleID:    rule.ID,
		payload:   eventPayload(payload),
	}
	if projectName != "" {
		base.message = "Project: " + projectName
	}
	var out []delivery
	for _, id := range rule.IntegrationIDs {
		item := base
		item.integrationID = id
		out = append(out, item)
	}
	if rule.SendToApp {
		item := base
		item.integrationID = appIntegrationID
		out = append(out, item)
	}
	if rule.SendToEmail {
		item := base
		item.integrationID = emailIntegrationID
		out = append(out, item)
	}
	return out
}

func eventPayload(payload *jsjson.Value) *jsjson.Value {
	wrapper := jsjson.NewObj()
	wrapper.Set("type", jsjson.NewString("event"))
	wrapper.Set("event", payload)
	return jsjson.NewObject(wrapper)
}

// deliver sends the integration and email jobs first, then inserts the
// dashboard rows and enqueues their jobs: the order the TypeScript produces,
// because the dashboard delivery waits for its insert and the others do not.
func (d *Dispatcher) deliver(ctx context.Context, requestID string, pending []delivery) error {
	var errs []error
	var dashboard []delivery
	for _, item := range pending {
		if item.integrationID == appIntegrationID {
			dashboard = append(dashboard, item)
			continue
		}
		errs = append(errs, d.enqueue(ctx, requestID, item))
	}
	for _, item := range dashboard {
		if err := d.insert(ctx, item); err != nil {
			errs = append(errs, err)
			continue
		}
		errs = append(errs, d.enqueue(ctx, requestID, item))
	}
	return errors.Join(errs...)
}

// notificationData is createNotification's `data`.
type notificationData struct {
	title, message, projectID string
	ruleID                    string
	integrationID             *string
	sendToApp, sendToEmail    bool
	payload                   *jsjson.Value
}

func newNotificationData(item delivery) notificationData {
	data := notificationData{
		title:     item.title,
		message:   item.message,
		projectID: item.projectID,
		ruleID:    item.ruleID,
		payload:   stripNullChars(item.payload),
	}
	switch item.integrationID {
	case appIntegrationID:
		data.sendToApp = true
	case emailIntegrationID:
		data.sendToEmail = true
	default:
		id := item.integrationID
		data.integrationID = &id
	}
	return data
}

// insert is the notifications row of a dashboard delivery; id is the
// column's default and both timestamps are one clock read.
func (d *Dispatcher) insert(ctx context.Context, item delivery) error {
	data := newNotificationData(item)
	now := d.Clock.Now().UTC().Truncate(rowTimePrecision)
	_, err := d.DB.Exec(ctx, `INSERT INTO "notifications" ("projectId","title","message","createdAt","updatedAt","sendToApp","sendToEmail","integrationId","notificationRuleId","payload")
VALUES ($1,$2,$3,$4,$5,$6,$7,$8::text::uuid,$9::text::uuid,$10::text::jsonb)`,
		data.projectID, data.title, data.message, now, now, data.sendToApp, data.sendToEmail,
		data.integrationID, data.ruleID, string(jsjson.Stringify(data.payload)))
	if err != nil {
		return fmt.Errorf("notify: insert notification: %w", err)
	}
	return nil
}

// enqueue adds the sendNotification job. The notification is the job schema's
// parse of data: its keys in schema order, no id.
func (d *Dispatcher) enqueue(ctx context.Context, requestID string, item delivery) error {
	data := newNotificationData(item)
	notification := jsjson.NewObj()
	notification.Set("projectId", jsjson.NewString(data.projectID))
	notification.Set("title", jsjson.NewString(data.title))
	notification.Set("message", jsjson.NewString(data.message))
	notification.Set("sendToApp", jsjson.NewBool(data.sendToApp))
	notification.Set("sendToEmail", jsjson.NewBool(data.sendToEmail))
	if data.integrationID == nil {
		notification.Set("integrationId", jsjson.NewNull())
	} else {
		notification.Set("integrationId", jsjson.NewString(*data.integrationID))
	}
	notification.Set("notificationRuleId", jsjson.NewString(data.ruleID))
	notification.Set("payload", data.payload)

	inner := jsjson.NewObj()
	inner.Set("notification", jsjson.NewObject(notification))
	meta := jsjson.NewObj()
	meta.Set("requestId", jsjson.NewString(requestID))
	envelope := jsjson.NewObj()
	envelope.Set("payload", jsjson.NewObject(inner))
	envelope.Set("meta", jsjson.NewObject(meta))

	if _, err := d.Queue.Add(ctx, QueueName, JobName, jsjson.Stringify(jsjson.NewObject(envelope)), JobOptions{}); err != nil {
		return fmt.Errorf("notify: enqueue notification: %w", err)
	}
	return nil
}

// stripNullChars removes U+0000 from every string value (keys are kept),
// because jsonb cannot store it.
func stripNullChars(v *jsjson.Value) *jsjson.Value {
	switch v.Kind {
	case jsjson.String:
		return jsjson.NewString(strings.ReplaceAll(v.Str, "\x00", ""))
	case jsjson.Array:
		out := &jsjson.Value{Kind: jsjson.Array, Arr: make([]*jsjson.Value, len(v.Arr))}
		for i, item := range v.Arr {
			out.Arr[i] = stripNullChars(item)
		}
		return out
	case jsjson.Object:
		out := jsjson.NewObj()
		for _, key := range v.Obj.Keys() {
			out.Set(key, stripNullChars(v.Obj.Get(key)))
		}
		return jsjson.NewObject(out)
	}
	return v
}

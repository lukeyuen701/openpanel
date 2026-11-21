package notify

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/jsport"
)

const funnelTitleSeparator = " -> "

// SessionEvent is an IServiceEvent as the session-end check sees it: its JSON
// tree (createdAt as an ISO string, undefined members absent) and createdAt in
// milliseconds, NaN for an invalid date.
type SessionEvent struct {
	Value       *jsjson.Value
	CreatedAtMs float64
}

// IsFunnelRule is rule.config.type === 'funnel'. A rule without a config is a
// TypeError, as it is when the TypeScript reads it.
func IsFunnelRule(rule Rule) (bool, error) {
	if rule.Config == nil || rule.Config.Kind == jsjson.Null {
		return false, typeError("Cannot read properties of null (reading 'type')")
	}
	kind := rule.Config.Get("type")
	return kind != nil && kind.Kind == jsjson.String && kind.Str == "funnel", nil
}

// HasFunnelRules is getHasFunnelRules.
func HasFunnelRules(rules []Rule) (bool, error) {
	for _, rule := range rules {
		funnel, err := IsFunnelRule(rule)
		if err != nil || funnel {
			return funnel, err
		}
	}
	return false, nil
}

// CheckSessionEnd is checkNotificationRulesForSessionEnd: the events, oldest
// first, are walked once per funnel rule, each step matched greedily in order,
// and a completed funnel notifies every destination of its rule. A rule whose
// matching throws stops the evaluation of the rules after it; the notifications
// of the rules before it are still delivered.
func (d *Dispatcher) CheckSessionEnd(ctx context.Context, requestID string, events []SessionEvent) error {
	sorted := append([]SessionEvent(nil), events...)
	// Array.prototype.sort treats a NaN comparison as a tie.
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].CreatedAtMs < sorted[j].CreatedAtMs })
	if len(sorted) == 0 || !isTruthy(sorted[0].Value.Get("projectId")) {
		return nil
	}
	projectID := jsjson.JSString(sorted[0].Value.Get("projectId"))
	projectName, err := d.Projects.ProjectName(ctx, projectID)
	if err != nil {
		return err
	}
	rules, err := d.Rules.ByProject(ctx, projectID)
	if err != nil {
		return err
	}

	var pending []delivery
	var matchErr error
	for _, rule := range rules {
		funnel, err := IsFunnelRule(rule)
		if err != nil {
			matchErr = err
			break
		}
		if !funnel {
			continue
		}
		matched, completed, err := matchFunnel(d.Matcher, sorted, rule)
		if err != nil {
			matchErr = err
			break
		}
		if !completed {
			continue
		}
		pending = append(pending, funnelDeliveries(matched, rule, projectID, projectName)...)
	}
	return errors.Join(d.deliver(ctx, requestID, pending), matchErr)
}

// matchFunnel walks the events once and matches the rule's steps in order.
func matchFunnel(matcher *Matcher, events []SessionEvent, rule Rule) ([]*jsjson.Value, bool, error) {
	steps := rule.Config.Get("events")
	if steps == nil || steps.Kind != jsjson.Array {
		return nil, false, typeError("Cannot read properties of undefined (reading '0')")
	}
	var matched []*jsjson.Value
	for _, event := range events {
		var step *jsjson.Value
		if len(matched) < len(steps.Arr) {
			step = steps.Arr[len(matched)]
		}
		ok, err := matcher.MatchEvent(event.Value, step)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			continue
		}
		matched = append(matched, event.Value)
		if len(matched) == len(steps.Arr) {
			break
		}
	}
	return matched, len(matched) >= len(steps.Arr), nil
}

// TemplateFunnel is notificationTemplateFunnel.
func TemplateFunnel(events []*jsjson.Value, rule Rule) string {
	if rule.Template == "" {
		return fmt.Sprintf("Funnel \"%s\" completed", rule.Name)
	}
	names := make([]string, len(events))
	for i, event := range events {
		name := event.Get("name")
		// join() renders undefined and null as nothing.
		if name != nil && name.Kind != jsjson.Null {
			names[i] = jsjson.JSString(name)
		}
	}
	template := jsport.ReplaceAll(rule.Template, "$EVENT_NAME", strings.Join(names, funnelTitleSeparator))
	return jsport.ReplaceAll(template, "$RULE_NAME", rule.Name)
}

func funnelDeliveries(events []*jsjson.Value, rule Rule, projectID, projectName string) []delivery {
	funnel := &jsjson.Value{Kind: jsjson.Array, Arr: events}
	wrapper := jsjson.NewObj()
	wrapper.Set("type", jsjson.NewString("funnel"))
	wrapper.Set("funnel", funnel)
	base := delivery{
		title:     TemplateFunnel(events, rule),
		projectID: projectID,
		ruleID:    rule.ID,
		payload:   jsjson.NewObject(wrapper),
	}
	if projectName != "" {
		base.message = "Project: " + projectName
	}
	return destinations(base, rule)
}

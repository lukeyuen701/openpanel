package notify

import (
	"fmt"
	"regexp"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/jsport"
)

var placeholderPattern = regexp.MustCompile("{{[^}]+}}")

// TemplateEvent is notificationTemplateEvent: the notification title for an
// event a rule matched.
func TemplateEvent(payload *jsjson.Value, rule Rule) string {
	if rule.Template == "" {
		return fmt.Sprintf("You received a new \"%s\" event", jsjson.JSString(payload.Get("name")))
	}
	template := jsport.ReplaceAll(rule.Template, "$EVENT_NAME", jsjson.JSString(payload.Get("name")))
	template = jsport.ReplaceAll(template, "$RULE_NAME", rule.Name)
	template = jsport.ReplaceAll(template, "{{rule_name}}", rule.Name)

	for _, placeholder := range placeholderPattern.FindAllString(template, -1) {
		value := pathOr(payload, splitPath(placeholder[2:len(placeholder)-2]))
		if value.isBuiltin {
			// A builtin is a function the replaceAll would call; not modelled.
			continue
		}
		if replacement, ok := placeholderText(value.value, value.isDate); ok {
			template = jsport.ReplaceAll(template, placeholder, replacement)
		}
	}
	return template
}

// placeholderText is what a truthy value becomes inside a template: objects,
// arrays and Dates through JSON.stringify, the rest as String(value). Falsy
// values leave the placeholder untouched.
func placeholderText(value *jsjson.Value, isDate bool) (string, bool) {
	if isDate {
		return stringifyText(value), true
	}
	if !isTruthy(value) {
		return "", false
	}
	if value.Kind == jsjson.Array || value.Kind == jsjson.Object {
		return stringifyText(value), true
	}
	return jsjson.JSString(value), true
}

// stringifyText is JSON.stringify(value) for the values a template reads,
// keeping unpaired surrogates as escapes.
func stringifyText(value *jsjson.Value) string {
	text, err := jsport.Stringify(value)
	if err != nil {
		return ""
	}
	return text
}

func splitPath(path string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(path); i++ {
		if path[i] == '.' {
			parts = append(parts, path[start:i])
			start = i + 1
		}
	}
	return append(parts, path[start:])
}

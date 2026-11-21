package notify

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"openpanel/ingest/internal/jsjson"
)

const (
	rulesCacheName     = "getNotificationRulesByProjectId"
	localCacheTTL      = 60 * time.Second
	localCacheMaxItems = 1000
)

// Cache is the Redis surface the shared `cachable:*` entries are read through.
// Entries are only ever read here: the Bun API writes them, and a Go writer
// would have to produce the full Prisma-shaped row.
type Cache interface {
	// Get returns the value and whether the key exists.
	Get(ctx context.Context, key string) (string, bool, error)
}

// DB is the Postgres surface the notification code uses; *pgxpool.Pool and
// pgx.Tx both satisfy it.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Rule is a cached notification rule: the columns the consumer selects.
type Rule struct {
	ID          string
	Name        string
	SendToApp   bool
	SendToEmail bool
	// Config is the rule's jsonb as parsed: {type, events}.
	Config *jsjson.Value
	// Template is empty when the rule has none.
	Template       string
	IntegrationIDs []string
}

// RuleStore is getNotificationRulesByProjectId: the API's Redis entry first,
// Postgres on a miss, and a process-local copy for a minute.
type RuleStore struct {
	redis Cache
	db    DB
	local *expirable.LRU[string, []Rule]
}

// NewRuleStore builds the store.
func NewRuleStore(redis Cache, db DB) *RuleStore {
	return &RuleStore{
		redis: redis,
		db:    db,
		local: expirable.NewLRU[string, []Rule](localCacheMaxItems, nil, localCacheTTL),
	}
}

// CacheKey is the cachable key for a project id: stringify([id]) with every
// JavaScript \s removed from the whole key.
func CacheKey(name, id string) string {
	key := "cachable:" + name + ":[" + id + "]"
	return strings.Map(func(r rune) rune {
		if isJSWhitespace(r) {
			return -1
		}
		return r
	}, key)
}

// ByProject returns the project's rules. An empty list is a valid cached
// answer. A Redis error is returned: the TypeScript read does not catch it.
func (s *RuleStore) ByProject(ctx context.Context, projectID string) ([]Rule, error) {
	key := CacheKey(rulesCacheName, projectID)
	if rules, ok := s.local.Get(key); ok {
		return rules, nil
	}
	raw, found, err := s.redis.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if found && raw != "" {
		if rules, ok, err := parseCachedRules(raw); err != nil {
			return nil, err
		} else if ok {
			s.local.Add(key, rules)
			return rules, nil
		}
	}
	rules, err := s.load(ctx, projectID)
	if err != nil {
		return nil, err
	}
	s.local.Add(key, rules)
	return rules, nil
}

// parseCachedRules reads a cached value. ok is false for what the cache
// treats as a miss (unparseable, null, an empty object or string). Any other
// non-array value is cached by the TypeScript and then breaks rules.some.
func parseCachedRules(raw string) ([]Rule, bool, error) {
	parsed, err := jsjson.Parse([]byte(raw))
	if err != nil {
		return nil, false, nil
	}
	switch parsed.Kind {
	case jsjson.Array:
		rules, err := rulesFromJSON(parsed)
		return rules, err == nil, err
	case jsjson.Null:
		return nil, false, nil
	case jsjson.String:
		if parsed.Str == "" {
			return nil, false, nil
		}
	case jsjson.Object:
		if parsed.Obj.Len() == 0 {
			return nil, false, nil
		}
	}
	return nil, false, typeError("rules.some is not a function")
}

func rulesFromJSON(list *jsjson.Value) ([]Rule, error) {
	rules := make([]Rule, 0, len(list.Arr))
	for _, item := range list.Arr {
		if !item.IsObject() {
			return nil, typeError("cached notification rule is not an object")
		}
		rule := Rule{
			ID:          jsjson.JSString(item.Get("id")),
			Name:        jsjson.JSString(item.Get("name")),
			SendToApp:   isTruthy(item.Get("sendToApp")),
			SendToEmail: isTruthy(item.Get("sendToEmail")),
			Config:      item.Get("config"),
		}
		if template := item.Get("template"); template != nil && template.Kind == jsjson.String {
			rule.Template = template.Str
		}
		if integrations := item.Get("integrations"); integrations != nil && integrations.Kind == jsjson.Array {
			for _, integration := range integrations.Arr {
				rule.IntegrationIDs = append(rule.IntegrationIDs, jsjson.JSString(integration.Get("id")))
			}
		} else {
			return nil, typeError("rule.integrations is not iterable")
		}
		rules = append(rules, rule)
	}
	return rules, nil
}

func (s *RuleStore) load(ctx context.Context, projectID string) ([]Rule, error) {
	rows, err := s.db.Query(ctx, `SELECT id::text, name, "sendToApp", "sendToEmail", config::text, template
FROM notification_rules WHERE "projectId" = $1`, projectID)
	if err != nil {
		return nil, fmt.Errorf("notify: read rules: %w", err)
	}
	defer rows.Close()
	var rules []Rule
	var ids []string
	for rows.Next() {
		var rule Rule
		var config string
		var template *string
		if err := rows.Scan(&rule.ID, &rule.Name, &rule.SendToApp, &rule.SendToEmail, &config, &template); err != nil {
			return nil, fmt.Errorf("notify: read rules: %w", err)
		}
		if rule.Config, err = jsjson.Parse([]byte(config)); err != nil {
			return nil, fmt.Errorf("notify: rule %s config: %w", rule.ID, err)
		}
		if template != nil {
			rule.Template = *template
		}
		rules = append(rules, rule)
		ids = append(ids, rule.ID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("notify: read rules: %w", err)
	}
	rows.Close()
	if len(rules) == 0 {
		return []Rule{}, nil
	}
	if err := s.attachIntegrations(ctx, rules, ids); err != nil {
		return nil, err
	}
	return rules, nil
}

func (s *RuleStore) attachIntegrations(ctx context.Context, rules []Rule, ruleIDs []string) error {
	rows, err := s.db.Query(ctx, `SELECT i.id::text, t."B"::text
FROM integrations i INNER JOIN "_IntegrationToNotificationRule" t ON t."A" = i.id
WHERE t."B"::text = ANY($1)`, ruleIDs)
	if err != nil {
		return fmt.Errorf("notify: read rule integrations: %w", err)
	}
	defer rows.Close()
	byRule := map[string][]string{}
	for rows.Next() {
		var integrationID, ruleID string
		if err := rows.Scan(&integrationID, &ruleID); err != nil {
			return fmt.Errorf("notify: read rule integrations: %w", err)
		}
		byRule[ruleID] = append(byRule[ruleID], integrationID)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("notify: read rule integrations: %w", err)
	}
	for i := range rules {
		rules[i].IntegrationIDs = byRule[rules[i].ID]
	}
	return nil
}

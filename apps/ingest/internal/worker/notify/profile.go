package notify

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"openpanel/ingest/internal/jsjson"
)

const (
	profileCachePrefix = "profile-cache:"
	// profileQuery is profileByIdQuery with ClickHouse's own parameter binding.
	profileQuery = `SELECT id, first_name, last_name, email, avatar, properties, project_id, is_external, created_at, last_seen_at, groups
FROM profiles WHERE id = {p1:String} AND project_id = {p2:String} ORDER BY last_seen_at DESC LIMIT 1 FORMAT JSON`
	clickHouseTimeout = 10 * time.Second
)

// ProfileQuerier fetches the newest profiles row for an id as the JSON object
// ClickHouse's JSON format returns, or nil when there is none.
type ProfileQuerier interface {
	ProfileRow(ctx context.Context, projectID, profileID string) (*jsjson.Value, error)
}

// ProfileStore is getProfileById: the profile cache the flush fills, then
// ClickHouse.
type ProfileStore struct {
	redis Cache
	ch    ProfileQuerier
}

// NewProfileStore builds the store.
func NewProfileStore(redis Cache, ch ProfileQuerier) *ProfileStore {
	return &ProfileStore{redis: redis, ch: ch}
}

// ByID returns the transformed profile (transformProfile's shape and key
// order), or nil when the profile is unknown.
func (s *ProfileStore) ByID(ctx context.Context, profileID, projectID string) (*jsjson.Value, error) {
	if profileID == "" || projectID == "" {
		return nil, nil
	}
	raw, found, err := s.redis.Get(ctx, profileCachePrefix+projectID+":"+profileID)
	if err != nil {
		return nil, err
	}
	if found && raw != "" {
		if cached, err := jsjson.Parse([]byte(raw)); err == nil && isTruthy(cached) {
			return transformProfile(cached)
		}
	}
	row, err := s.ch.ProfileRow(ctx, projectID, profileID)
	if err != nil || row == nil {
		return nil, err
	}
	return transformProfile(row)
}

// transformProfile is transformProfile: snake_case row to the camelCase
// profile, properties un-dotted, dates as ISO strings (they are Dates in
// TypeScript, see isDatePath).
func transformProfile(row *jsjson.Value) (*jsjson.Value, error) {
	if !row.IsObject() {
		return nil, typeError("profile is not an object")
	}
	createdAtText := row.Get("created_at")
	if createdAtText == nil || createdAtText.Kind != jsjson.String {
		return nil, typeError("date.replace is not a function")
	}
	createdAt := clickHouseDateToJS(createdAtText.Str)
	lastSeenAt := createdAt
	if last := row.Get("last_seen_at"); isTruthy(last) {
		if last.Kind != jsjson.String {
			return nil, typeError("date.replace is not a function")
		}
		lastSeenAt = clickHouseDateToJS(last.Str)
	}
	properties, err := toObject(row.Get("properties"))
	if err != nil {
		return nil, err
	}
	groups := row.Get("groups")
	if groups == nil || groups.Kind == jsjson.Null {
		groups = &jsjson.Value{Kind: jsjson.Array}
	}

	out := jsjson.NewObj()
	setDefined(out, "firstName", row.Get("first_name"))
	setDefined(out, "lastName", row.Get("last_name"))
	setDefined(out, "isExternal", row.Get("is_external"))
	out.Set("properties", properties)
	out.Set("createdAt", createdAt)
	out.Set("lastSeenAt", lastSeenAt)
	setDefined(out, "projectId", row.Get("project_id"))
	setDefined(out, "id", row.Get("id"))
	setDefined(out, "email", row.Get("email"))
	setDefined(out, "avatar", row.Get("avatar"))
	out.Set("groups", groups)
	return jsjson.NewObject(out), nil
}

// setDefined skips an undefined member, as JSON.stringify would.
func setDefined(obj *jsjson.Obj, key string, value *jsjson.Value) {
	if value != nil {
		obj.Set(key, value)
	}
}

// clickHouseDateToJS is convertClickhouseDateToJs followed by toISOString:
// the text read as UTC. An invalid date has no ISO form and is a null.
func clickHouseDateToJS(text string) *jsjson.Value {
	text = strings.Replace(text, " ", "T", 1) + "Z"
	for _, layout := range []string{"2006-01-02T15:04:05.999999999Z", "2006-01-02T15:04:05Z"} {
		if parsed, err := time.Parse(layout, text); err == nil {
			return jsjson.NewString(parsed.UTC().Format("2006-01-02T15:04:05.000Z"))
		}
	}
	return jsjson.NewNull()
}

// toObject is toObject: dotted keys become nested objects, and a segment that
// holds a non-object is replaced by one.
func toObject(properties *jsjson.Value) (*jsjson.Value, error) {
	if properties == nil || properties.Kind == jsjson.Null {
		return nil, typeError("Cannot convert undefined or null to object")
	}
	result := jsjson.NewObj()
	if !properties.IsObject() {
		return jsjson.NewObject(result), nil
	}
	for _, key := range properties.Obj.Keys() {
		segments := splitPath(key)
		leaf := segments[len(segments)-1]
		node := result
		for _, segment := range segments[:len(segments)-1] {
			if child := node.Get(segment); child.IsObject() {
				node = child.Obj
				continue
			}
			next := jsjson.NewObj()
			node.Set(segment, jsjson.NewObject(next))
			node = next
		}
		node.Set(leaf, properties.Obj.Get(key))
	}
	return jsjson.NewObject(result), nil
}

// ClickHouseHTTP runs the profile query over ClickHouse's HTTP interface.
type ClickHouseHTTP struct {
	// URL is the server, for example http://localhost:8123.
	URL      string
	Database string
	User     string
	Password string
	Client   *http.Client
}

// NewClickHouseHTTP reads the connection from a URL such as
// http://user:password@host:8123/database, as CLICKHOUSE_URL spells it.
func NewClickHouseHTTP(rawURL string) (*ClickHouseHTTP, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("notify: CLICKHOUSE_URL: %w", err)
	}
	client := &ClickHouseHTTP{Database: strings.Trim(parsed.Path, "/")}
	if parsed.User != nil {
		client.User = parsed.User.Username()
		client.Password, _ = parsed.User.Password()
	}
	parsed.User, parsed.Path, parsed.RawQuery = nil, "", ""
	client.URL = parsed.String()
	return client, nil
}

// ProfileRow implements ProfileQuerier.
func (c *ClickHouseHTTP) ProfileRow(ctx context.Context, projectID, profileID string) (*jsjson.Value, error) {
	body, err := c.run(ctx, profileQuery, map[string]string{"p1": profileID, "p2": projectID})
	if err != nil {
		return nil, err
	}
	parsed, err := jsjson.Parse(body)
	if err != nil {
		return nil, fmt.Errorf("notify: clickhouse response: %w", err)
	}
	data := parsed.Get("data")
	if data == nil || data.Kind != jsjson.Array || len(data.Arr) == 0 {
		return nil, nil
	}
	return data.Arr[0], nil
}

// queryParamEscaper is what the JavaScript ClickHouse client does to a String
// query parameter: ClickHouse reads the value back with these escapes.
var queryParamEscaper = strings.NewReplacer("\\", "\\\\", "\t", "\\t", "\n", "\\n", "\r", "\\r", "'", "\\'")

// run POSTs a statement with its String parameters and returns the body.
func (c *ClickHouseHTTP) run(ctx context.Context, statement string, params map[string]string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, clickHouseTimeout)
	defer cancel()
	query := url.Values{}
	for name, value := range params {
		query.Set("param_"+name, queryParamEscaper.Replace(value))
	}
	if c.Database != "" {
		query.Set("database", c.Database)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.URL, "/")+"/?"+query.Encode(), bytes.NewBufferString(statement))
	if err != nil {
		return nil, fmt.Errorf("notify: clickhouse request: %w", err)
	}
	if c.User != "" {
		request.SetBasicAuth(c.User, c.Password)
	}
	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("notify: clickhouse query: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("notify: clickhouse response: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("notify: clickhouse status %d: %s", response.StatusCode, bytes.TrimSpace(body))
	}
	return body, nil
}

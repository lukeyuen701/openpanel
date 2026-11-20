package clientauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/reqheaders"
)

var clientIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// Request is what the ingest tier authenticates.
type Request struct {
	Headers  reqheaders.Headers
	ClientIP string
	// Body is the body the route validated: the schema's output on /track,
	// the raw JSON on /event, nil on GET.
	Body *jsjson.Value
	// Origin and its host as OriginHost answered.
	Origin     string
	HasOrigin  bool
	OriginHost string
}

// Outcome is validateIngestRequest's answer. A refusal carries the 401 text.
type Outcome struct {
	Client         *Client
	SecretVerified bool
	Refusal        string
}

// OK reports whether the client may ingest.
func (o Outcome) OK() bool { return o.Refusal == "" }

// Authenticator runs the ingest tier.
type Authenticator struct {
	Store    *Store
	Verifier *Verifier
}

func firstTruthy(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func bodyString(body *jsjson.Value, key string) string {
	if v := body.Get(key); v != nil && v.Kind == jsjson.String {
		return v.Str
	}
	return ""
}

// Authenticate is validateIngestRequest. An error is an infrastructure
// failure (Postgres down), which the API answers with a 500.
func (a *Authenticator) Authenticate(ctx context.Context, req Request) (Outcome, error) {
	clientID := firstTruthy(req.Headers.Value("openpanel-client-id"), req.Headers.Value("mixan-client-id"), bodyString(req.Body, "clientId"))
	secret := firstTruthy(req.Headers.Value("openpanel-client-secret"), req.Headers.Value("mixan-client-secret"), bodyString(req.Body, "clientSecret"))

	if clientID == "" {
		return Outcome{Refusal: "Ingestion: Missing client id"}, nil
	}
	if !clientIDPattern.MatchString(clientID) {
		return Outcome{Refusal: "Ingestion: Client ID must be a valid UUIDv4"}, nil
	}
	client, err := a.Store.ClientByID(ctx, clientID)
	if err != nil {
		return Outcome{}, err
	}
	if client == nil {
		return Outcome{Refusal: "Ingestion: Invalid client id"}, nil
	}
	if client.Project == nil {
		return Outcome{Refusal: "Ingestion: Client has no project"}, nil
	}
	verified := a.Verifier.Verify(ctx, clientID, secret, client.Secret)
	refuse := func(message string) (Outcome, error) {
		return Outcome{Client: client, SecretVerified: verified, Refusal: message}, nil
	}

	filters, err := parseFilters(client.Project.Filters)
	if err != nil {
		// The API calls .filter on the column and throws on anything else.
		return Outcome{}, fmt.Errorf("clientauth: project %s: %w", client.Project.ID, err)
	}
	for _, filter := range filters {
		if filter.kind == "ip" && filter.ip != nil && *filter.ip == req.ClientIP {
			return refuse("Ingestion: IP address is blocked by project filter")
		}
	}
	profileIDs := submittedProfileIDs(req.Body)
	for _, filter := range filters {
		if filter.kind != "profile_id" {
			continue
		}
		for _, id := range profileIDs {
			if id == filter.profileID {
				return refuse("Ingestion: Profile id is blocked by project filter")
			}
		}
	}

	revenue := req.Body.Path("payload", "properties", "__revenue")
	if revenue == nil || revenue.Kind == jsjson.Null {
		revenue = req.Body.Path("properties", "__revenue")
	}
	if revenue != nil && !(client.Project.AllowUnsafeRevenueTracking || verified) {
		return refuse("Ingestion: Revenue tracking is not allowed without a client secret")
	}

	if client.IgnoreCorsAndSecret {
		return Outcome{Client: client, SecretVerified: verified}, nil
	}
	// An empty cors list is still a list (truthy in JavaScript): it allows nothing.
	if client.Project.Cors != nil && IsOriginAllowed(client.Project.Cors, req.Origin, req.HasOrigin, req.OriginHost) {
		return Outcome{Client: client, SecretVerified: verified}, nil
	}
	if verified {
		return Outcome{Client: client, SecretVerified: verified}, nil
	}
	return refuse("Ingestion: Invalid cors or secret")
}

type filter struct {
	kind      string
	ip        *string
	profileID string
}

var errFiltersNotArray = errors.New("filters is not an array")

// parseFilters reads the project's filters JSON. profileId compares as
// String(filter.profileId) does.
func parseFilters(raw json.RawMessage) ([]filter, error) {
	if len(raw) == 0 {
		return nil, errFiltersNotArray
	}
	value, err := jsjson.Parse(raw)
	if err != nil || value.Kind != jsjson.Array {
		return nil, errFiltersNotArray
	}
	out := make([]filter, 0, len(value.Arr))
	for _, item := range value.Arr {
		kind := item.Get("type")
		if kind == nil || kind.Kind != jsjson.String {
			continue
		}
		f := filter{kind: kind.Str}
		if ip := item.Get("ip"); ip != nil && ip.Kind == jsjson.String {
			f.ip = &ip.Str
		}
		f.profileID = jsjson.JSString(item.Get("profileId"))
		out = append(out, f)
	}
	return out, nil
}

// submittedProfileIDs is every id the request could be stored under, as the
// string the pipeline stores.
func submittedProfileIDs(body *jsjson.Value) []string {
	candidates := []*jsjson.Value{
		body.Path("payload", "properties", "__identify", "profileId"),
		body.Path("payload", "profileId"),
		body.Path("profileId"),
	}
	var out []string
	for _, candidate := range candidates {
		if candidate != nil && (candidate.Kind == jsjson.String || candidate.Kind == jsjson.Number) {
			out = append(out, jsjson.JSString(candidate))
		}
	}
	return out
}

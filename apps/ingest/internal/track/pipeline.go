package track

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"openpanel/ingest/internal/bots"
	"openpanel/ingest/internal/geo"
	"openpanel/ingest/internal/identity"
	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/jsregex"
	"openpanel/ingest/internal/lookups"
	"openpanel/ingest/internal/reqheaders"
	"openpanel/ingest/internal/useragent"
)

const (
	fallbackUserAgent          = "unknown/1.0"
	maxOverrideDeviceIDLength  = 64
	partitionKeyFallbackLength = 8
	nanoidAlphabet             = "useandom-26T198340PX75pxJACKVERYMINDBUSHWOLF_GQZbfghjklqvwyzrict"
)

// queuePayloadHeaders ship with every event, in this order.
var queuePayloadHeaders = []string{"user-agent", "openpanel-sdk-name", "openpanel-sdk-version", "openpanel-client-id", "request-id"}

// GeoResolver is the MaxMind lookup.
type GeoResolver interface {
	Location(ip string) geo.Location
	ASN(ip string) geo.ASN
}

// SaltSource is getSalts.
type SaltSource interface {
	Get(ctx context.Context) (lookups.Salts, error)
}

// EventProducer writes one event to Kafka.
type EventProducer interface {
	Produce(ctx context.Context, key, value []byte) error
}

// Pipeline is ingestTrack's track branch and ingestLegacyEvent, after the
// transport authenticated the client.
type Pipeline struct {
	UA        *useragent.Parser
	Bots      *bots.Detector
	Geo       GeoResolver
	Salts     SaltSource
	Sessions  identity.SessionReader
	TimeoutMs int64
	Producer  EventProducer
	Logger    *slog.Logger
	NewUUID   func() string
}

// Input is one authenticated event.
type Input struct {
	ProjectID      string
	ClientIP       string
	Headers        reqheaders.Headers
	SecretVerified bool
	ArrivalMs      int64
}

// Identity is what /track answers with.
type Identity struct {
	DeviceID  string
	SessionID string
}

// StringHeaders is getStringHeaders: the whitelisted headers, an empty value
// left out as undefined is.
func StringHeaders(h reqheaders.Headers) *jsjson.Obj {
	out := jsjson.NewObj()
	for _, name := range queuePayloadHeaders {
		if value := h.Value(name); value != "" {
			out.Set(name, jsjson.NewString(value))
		}
	}
	return out
}

// OverrideDeviceID is sanitizeOverrideDeviceId over properties.__deviceId.
func OverrideDeviceID(properties *jsjson.Value) string {
	raw := properties.Get("__deviceId")
	if raw == nil || raw.Kind != jsjson.String {
		return ""
	}
	trimmed := trimJS(raw.Str)
	if trimmed == "" || jsregex.LenUTF16(trimmed) > maxOverrideDeviceIDLength {
		return ""
	}
	return trimmed
}

// userAgent is headerValue(headers, 'user-agent') ?? 'unknown/1.0': only an
// absent header falls back, an empty one stays empty.
func userAgent(h reqheaders.Headers) string {
	if value, ok := h.Get("user-agent"); ok {
		return value
	}
	return fallbackUserAgent
}

func (p *Pipeline) resolveDevice(ctx context.Context, salts lookups.Salts, projectID, ip, ua, override string, eventMs int64) (identity.Result, error) {
	resolver := identity.Resolver{
		Sessions:  p.Sessions,
		TimeoutMs: p.TimeoutMs,
		OnError: func(err error) {
			p.Logger.ErrorContext(ctx, "Error resolving session for device id", slog.Any("err", err))
		},
	}
	if override != "" {
		return resolver.Resolve(ctx, projectID, []string{override}, eventMs)
	}
	if ua == "" {
		return identity.Result{}, nil
	}
	current := identity.DeviceID(salts.Current, ua, ip, projectID)
	previous := identity.DeviceID(salts.Previous, ua, ip, projectID)
	return resolver.Resolve(ctx, projectID, []string{current, previous}, eventMs)
}

// Track runs a schema-validated track body (the output of ParseTrackBody).
// The transport has already refused bodies this cannot run natively.
func (p *Pipeline) Track(ctx context.Context, in Input, body *jsjson.Value) (Identity, error) {
	payload := body.Get("payload").Obj
	propertiesValue := payload.Get("properties")

	ts, _ := ResolveTimestamp(in.ArrivalMs, propertiesValue.Get("__timestamp"))
	ip := in.ClientIP
	if override := propertiesValue.Get("__ip"); override != nil && override.Kind == jsjson.String && override.Str != "" {
		ip = override.Str
	}
	ua := userAgent(in.Headers)
	headers := StringHeaders(in.Headers)

	if profileID := payload.Get("profileId"); truthy(profileID) {
		payload.Set("profileId", jsjson.NewString(jsjson.JSString(profileID)))
	}
	override := OverrideDeviceID(propertiesValue)

	location := p.Geo.Location(ip)
	asn := p.Geo.ASN(ip)
	salts, err := p.Salts.Get(ctx)
	if err != nil {
		return Identity{}, err
	}
	device, err := p.resolveDevice(ctx, salts, in.ProjectID, ip, ua, override, ts.Ms)
	if err != nil {
		return Identity{}, err
	}

	var properties *jsjson.Obj
	if propertiesValue != nil {
		properties = propertiesValue.Obj
	}
	uaHeader := ""
	if value := headers.Get("user-agent"); value != nil {
		uaHeader = value.Str
	}
	uaInfo := p.UA.Parse(uaHeader, useragent.PropertyOverrides{Properties: properties})
	if marked := ApplyBotSuspicion(properties, asn, in.Headers, in.SecretVerified, uaInfo.IsServer); marked != nil {
		payload.Set("properties", jsjson.NewObject(marked))
	}

	groupKey := device.DeviceID
	if uaInfo.IsServer {
		groupKey = ""
		if profileID := payload.Get("profileId"); truthy(profileID) {
			groupKey = in.ProjectID + ":" + jsjson.JSString(profileID)
		}
	}

	event := jsjson.NewObj()
	for _, key := range payload.Keys() {
		event.Set(key, payload.Get(key))
	}
	if groups := payload.Get("groups"); groups == nil || groups.Kind == jsjson.Null {
		event.Set("groups", &jsjson.Value{Kind: jsjson.Array, Arr: []*jsjson.Value{}})
	}
	event.Set("timestamp", jsjson.NewNumber(float64(ts.Ms)))
	event.Set("isTimestampFromThePast", jsjson.NewBool(ts.FromPast))

	message := jsjson.NewObj()
	message.Set("id", jsjson.NewString(p.NewUUID()))
	message.Set("projectId", jsjson.NewString(in.ProjectID))
	message.Set("headers", jsjson.NewObject(headers))
	message.Set("event", jsjson.NewObject(event))
	message.Set("uaInfo", uaInfo.JSON())
	message.Set("geo", location.JSON())
	message.Set("deviceId", jsjson.NewString(device.DeviceID))
	message.Set("sessionId", jsjson.NewString(device.SessionID))

	if groupKey == "" {
		groupKey = NewPartitionKey()
	}
	if err := p.Producer.Produce(ctx, []byte(groupKey), jsjson.Stringify(jsjson.NewObject(message))); err != nil {
		return Identity{}, err
	}
	return Identity{DeviceID: device.DeviceID, SessionID: device.SessionID}, nil
}

// truthy is JavaScript truthiness for a JSON value; nil is undefined.
func truthy(v *jsjson.Value) bool {
	if v == nil {
		return false
	}
	switch v.Kind {
	case jsjson.Null:
		return false
	case jsjson.Bool:
		return v.Bool
	case jsjson.Number:
		return v.Num != 0 && v.Num == v.Num
	case jsjson.String:
		return v.Str != ""
	}
	return true
}

// NewPartitionKey is generateId(): eight characters of nanoid's alphabet,
// for an event with no device to key on.
func NewPartitionKey() string {
	b := make([]byte, partitionKeyFallbackLength)
	limit := big.NewInt(int64(len(nanoidAlphabet)))
	for i := range b {
		n, err := rand.Int(rand.Reader, limit)
		if err != nil {
			panic(fmt.Sprintf("track: random source failed: %v", err))
		}
		b[i] = nanoidAlphabet[n.Int64()]
	}
	return string(b)
}

// Legacy is ingestLegacyEvent for a JSON object body whose name is a
// non-empty string and whose properties, if any, are an object; the
// transport hands anything else to the Bun API.
func (p *Pipeline) Legacy(ctx context.Context, in Input, body *jsjson.Value) error {
	name := body.Get("name")
	if name == nil || name.Kind != jsjson.String || trimJS(name.Str) == "" {
		return nil
	}
	propertiesValue := body.Get("properties")
	ts, _ := ResolveTimestamp(in.ArrivalMs, propertiesValue.Get("__timestamp"))
	ip := in.ClientIP
	ua := userAgent(in.Headers)
	headers := StringHeaders(in.Headers)

	salts, err := p.Salts.Get(ctx)
	if err != nil {
		return err
	}
	location := p.Geo.Location(ip)
	asn := p.Geo.ASN(ip)
	// The legacy route resolves the session against the clock, not the
	// back-dated event time.
	device, err := p.resolveDevice(ctx, salts, in.ProjectID, ip, ua, "", time.Now().UnixMilli())
	if err != nil {
		return err
	}

	var properties *jsjson.Obj
	if propertiesValue != nil {
		properties = propertiesValue.Obj
	}
	uaInfo := p.UA.Parse(ua, useragent.PropertyOverrides{Properties: properties})
	if marked := ApplyBotSuspicion(properties, asn, in.Headers, in.SecretVerified, uaInfo.IsServer); marked != nil {
		properties = marked
	}

	groupKey := device.DeviceID
	if uaInfo.IsServer {
		profileID := body.Get("profileId")
		suffix := NewPartitionKey()
		if profileID != nil && profileID.Kind != jsjson.Null {
			suffix = jsjson.JSString(profileID)
		}
		groupKey = in.ProjectID + ":" + suffix
	}

	event := jsjson.NewObj()
	event.Set("name", name)
	if profileID := body.Get("profileId"); profileID != nil && (profileID.Kind == jsjson.String || profileID.Kind == jsjson.Number) {
		event.Set("profileId", jsjson.NewString(jsjson.JSString(profileID)))
	}
	if properties != nil {
		event.Set("properties", jsjson.NewObject(properties))
	}
	event.Set("groups", &jsjson.Value{Kind: jsjson.Array, Arr: []*jsjson.Value{}})
	event.Set("timestamp", jsjson.NewNumber(float64(ts.Ms)))
	event.Set("isTimestampFromThePast", jsjson.NewBool(ts.FromPast))

	message := jsjson.NewObj()
	message.Set("id", jsjson.NewString(p.NewUUID()))
	message.Set("projectId", jsjson.NewString(in.ProjectID))
	message.Set("headers", jsjson.NewObject(headers))
	message.Set("event", jsjson.NewObject(event))
	message.Set("uaInfo", uaInfo.JSON())
	message.Set("geo", location.JSON())
	message.Set("deviceId", jsjson.NewString(device.DeviceID))
	message.Set("sessionId", jsjson.NewString(device.SessionID))
	return p.Producer.Produce(ctx, []byte(groupKey), jsjson.Stringify(jsjson.NewObject(message)))
}

// DeviceIdentity is fetchDeviceIdentity's answer.
type DeviceIdentity struct {
	DeviceID  string
	SessionID string
	Message   string
}

// DeviceLookup is GET /track/device-id after auth. ip and ua are non-empty;
// the transport answers their absence itself.
func (p *Pipeline) DeviceLookup(ctx context.Context, projectID, ip, ua string, nowMs int64) (DeviceIdentity, error) {
	salts, err := p.Salts.Get(ctx)
	if err != nil {
		return DeviceIdentity{}, err
	}
	current := identity.DeviceID(salts.Current, ua, ip, projectID)
	previous := identity.DeviceID(salts.Previous, ua, ip, projectID)
	sessions, err := p.Sessions.ExistingSessions(ctx, projectID, []string{current, previous})
	if err != nil {
		p.Logger.ErrorContext(ctx, "Error getting session end GET /track/device-id", slog.Any("err", err))
		sessions = nil
	}
	if len(sessions) > 0 && sessions[0] != nil && sessions[0].IsLive(nowMs, p.TimeoutMs) {
		return DeviceIdentity{DeviceID: current, SessionID: sessions[0].ID, Message: "current session exists for this device id"}, nil
	}
	if len(sessions) > 1 && sessions[1] != nil && sessions[1].IsLive(nowMs, p.TimeoutMs) {
		return DeviceIdentity{DeviceID: previous, SessionID: sessions[1].ID, Message: "previous session exists for this device id"}, nil
	}
	return DeviceIdentity{DeviceID: current, Message: "No session exists for this device id"}, nil
}

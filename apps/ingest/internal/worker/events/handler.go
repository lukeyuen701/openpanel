package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/worker/bullmq"
	"openpanel/ingest/internal/worker/consumer"
	"openpanel/ingest/internal/worker/jsport"
	"openpanel/ingest/internal/worker/wire"
)

const (
	// sessionStartOffsetMs is how far before the event session_start is
	// stamped, so it sorts first.
	sessionStartOffsetMs = 100

	sessionEndJobIDPrefix  = "sessionEnd:v2:"
	sessionEndAttempts     = 3
	sessionEndBackoffMs    = 200
	sessionEndBackoffType  = "exponential"
	sessionsQueueName      = "sessions"
	sessionEndSourceMetric = "boundary"

	headerSDKName    = "openpanel-sdk-name"
	headerSDKVersion = "openpanel-sdk-version"
)

var (
	globalProperties = [...]string{"__path", "__referrer", "__timestamp", "__revenue"}
	sessionUTMOrder  = [...]string{"utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term"}

	errNoHeaders = fmt.Errorf("%w: Cannot read properties of undefined (reading 'openpanel-sdk-name')", jsport.ErrTypeError)
)

// HandlerConfig wires a Handler.
type HandlerConfig struct {
	Events   *EventBuffer
	Sessions *SessionBuffer
	Profiles *ProfileBuffer
	AI       *AIEventBuffer
	Projects Projects
	Notifier Notifier
	Queue    SessionQueue
	Clock    Clock
	IDs      IDs
	Metrics  Metrics
	Logger   *slog.Logger
}

// Handler is incomingEvent for one Kafka record: it implements consumer.Handler.
type Handler struct {
	HandlerConfig
}

// NewHandler returns a Handler; nil Metrics and Logger count and log nothing.
func NewHandler(cfg HandlerConfig) *Handler {
	if cfg.Metrics == nil {
		cfg.Metrics = NopMetrics{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	return &Handler{HandlerConfig: cfg}
}

// scope is what one message carries through the path.
type scope struct {
	requestID string
	logger    *slog.Logger
}

// Handle implements consumer.Handler.
func (h *Handler) Handle(ctx context.Context, in wire.IncomingEventPayload, delivery consumer.Delivery) error {
	headers, hasHeaders := in.HeaderMap()
	requestID, err := wire.ResolveRequestID(headers[wire.RequestIDHeader], h.IDs)
	if err != nil {
		return err
	}
	if !hasHeaders {
		return errNoHeaders
	}
	logger := h.Logger.With(slog.String("requestId", requestID),
		slog.Int("kafkaPartition", int(delivery.Partition)), slog.Int64("kafkaOffset", delivery.Offset))
	return h.incomingEvent(ctx, scope{requestID: requestID, logger: logger}, in, headers)
}

// incomingEvent is incomingEvent: the event, then the AI row that follows it.
func (h *Handler) incomingEvent(ctx context.Context, sc scope, in wire.IncomingEventPayload, headers map[string]json.RawMessage) error {
	ai, err := parseRaw(in.AI)
	if err != nil {
		return err
	}
	document, err := h.ingestIncomingEvent(ctx, sc, in, headers, ai)
	if err != nil {
		return err
	}
	if isTruthy(ai) && document != nil {
		return h.storeAIEvent(ctx, in.ProjectID, ai, document)
	}
	return nil
}

// storeAIEvent writes the ai_events row after the event row, copying the
// identity and session the event resolved to.
func (h *Handler) storeAIEvent(ctx context.Context, projectID string, draft *jsjson.Value, document *jsjson.Obj) error {
	content, err := h.Projects.GetAiStoreContent(ctx, projectID)
	if err != nil {
		return err
	}
	storeContent := content == nil || *content
	h.AI.Add(ctx, buildAIEventRow(draft, document, storeContent))
	return nil
}

// ingestIncomingEvent is ingestIncomingEvent. The returned document is the
// event row, nil when the event was excluded.
func (h *Handler) ingestIncomingEvent(ctx context.Context, sc scope, in wire.IncomingEventPayload, headers map[string]json.RawMessage, ai *jsjson.Value) (*jsjson.Obj, error) {
	base, err := h.deriveBaseEvent(in, headers)
	if err != nil {
		return nil, err
	}
	eventID := optString(in.ID)

	if in.UAInfo.IsServer || in.Event.IsTimestampFromThePast || isTruthy(ai) {
		return h.serverBranch(ctx, sc, in, base, eventID)
	}

	excluded, err := h.Projects.IsExcluded(ctx, in.ProjectID, base.value())
	if err != nil {
		return nil, err
	}
	if excluded {
		sc.logger.Info("Skipping session_start and event (excluded by project filter)", slog.String("event", in.Event.Name), slog.String("projectId", in.ProjectID))
		return nil, nil
	}

	result := h.Sessions.Ingest(ctx, base)
	if result != nil && result.Kind == SessionBoundary {
		if err := h.enqueueSessionEnd(ctx, sc, base, result.Closed); err != nil {
			sc.logger.Error("Error enqueueing session_end on boundary", slog.Any("err", err),
				slog.String("deviceId", in.DeviceID), slog.String("sessionId", stringOf(get(result.Closed, "id"))))
		} else {
			h.Metrics.SessionEndEnqueued(sessionEndSourceMetric)
		}
	}

	if result != nil && (result.Kind == SessionNew || result.Kind == SessionBoundary) {
		h.Metrics.SessionStarted(result.Kind)
		start := base.copyOf()
		start.obj.Set("name", str(nameSessionStart))
		start.setCreatedAt(offsetDate(base.createdAt, -sessionStartOffsetMs))
		// No id: session_start is a second, derived row and must not share the
		// producer-minted id of the event that opened the session.
		if _, err := h.createEventAndNotify(ctx, sc, start, in.ProjectID); err != nil {
			sc.logger.Error("Error creating session start event", slog.Any("err", err))
			return nil, err
		}
	}

	final := base.copyOf()
	if result != nil {
		mergeSessionAttribution(final.obj, result.Current)
		final.obj.Set("properties", jsjson.NewObject(inheritSessionUTM(get(base.obj, "properties"), result.Current)))
	}
	final.obj.Set("id", eventID)
	return h.createEventAndNotify(ctx, sc, final, in.ProjectID)
}

// offsetDate is new Date(date.getTime() + ms).
func offsetDate(date jsport.Date, ms int64) jsport.Date {
	current, ok := date.Millis()
	if !ok {
		return date
	}
	return jsport.DateFromMillis(float64(current + ms))
}

// mergeSessionAttribution is merge(baseEvent, reject(isEmpty|isNil)({referrer,
// referrerName, referrerType} of the session)): the session's value wins when
// it is not empty.
func mergeSessionAttribution(payload *jsjson.Obj, session *jsjson.Obj) {
	for _, pair := range [...]struct{ payloadKey, sessionKey string }{
		{"referrer", "referrer"}, {"referrerName", "referrer_name"}, {"referrerType", "referrer_type"},
	} {
		value := get(session, pair.sessionKey)
		if isNullish(value) || isEmptyValue(value) {
			continue
		}
		payload.Set(pair.payloadKey, value)
	}
}

// inheritSessionUTM gives an event the UTM tags of its session when its own
// query has none; the event's own tags win.
func inheritSessionUTM(properties *jsjson.Value, session *jsjson.Obj) *jsjson.Obj {
	out := shallowCopy(asObject(properties))
	query := jsjson.NewObj()
	spreadInto(query, get(out, "__query"))
	for _, key := range sessionUTMOrder {
		if !isTruthy(get(query, key)) && isTruthy(get(session, key)) {
			query.Set(key, get(session, key))
		}
	}
	out.Set("__query", jsjson.NewObject(query))
	return out
}

// serverBranch is branch A: a server, past or AI event rides the session of
// its profile if there is one and never writes session state.
func (h *Handler) serverBranch(ctx context.Context, sc scope, in wire.IncomingEventPayload, base *eventPayload, eventID *jsjson.Value) (*jsjson.Obj, error) {
	var session *jsjson.Obj
	profileID := stringOf(get(base.obj, "profileId"))
	if profileID != "" && !in.Event.IsTimestampFromThePast {
		found, err := h.Sessions.GetByProfile(ctx, in.ProjectID, profileID)
		if err != nil {
			return nil, err
		}
		if isTruthy(found) {
			session = asObject(found)
		}
	}

	payload := base.copyOf()
	o := payload.obj
	if session != nil {
		o.Set("deviceId", orElse(get(session, "device_id"), get(base.obj, "deviceId")))
		o.Set("sessionId", orElse(get(session, "id"), get(base.obj, "sessionId")))
		o.Set("properties", jsjson.NewObject(inheritSessionUTM(get(base.obj, "properties"), session)))
	}
	// Without a session these three are undefined, not the event's own values.
	o.Set("referrer", undefinedIfNull(get(session, "referrer")))
	o.Set("referrerName", undefinedIfNull(get(session, "referrer_name")))
	o.Set("referrerType", undefinedIfNull(get(session, "referrer_type")))
	for _, pair := range [...]struct{ payloadKey, sessionKey string }{
		{"path", "exit_path"}, {"origin", "exit_origin"},
		{"os", "os"}, {"osVersion", "os_version"}, {"browserVersion", "browser_version"},
		{"browser", "browser"}, {"device", "device"}, {"brand", "brand"}, {"model", "model"},
		{"city", "city"}, {"country", "country"}, {"region", "region"},
		{"longitude", "longitude"}, {"latitude", "latitude"},
	} {
		o.Set(pair.payloadKey, orDefault(get(session, pair.sessionKey), get(base.obj, pair.payloadKey)))
	}
	o.Set("id", eventID)
	return h.createEventAndNotify(ctx, sc, payload, in.ProjectID)
}

// createEventAndNotify is createEventAndNotify: the project filter again, the
// event row and the notification check together, then the first-event mark.
func (h *Handler) createEventAndNotify(ctx context.Context, sc scope, payload *eventPayload, projectID string) (*jsjson.Obj, error) {
	excluded, err := h.Projects.IsExcluded(ctx, projectID, payload.value())
	if err != nil {
		return nil, err
	}
	if excluded {
		sc.logger.Info("Event excluded by project filter", slog.String("event", stringOf(get(payload.obj, "name"))), slog.String("projectId", projectID))
		return nil, nil
	}

	// createEvent's synchronous prefix runs first, so the notification check
	// sees the payload with its profile id already filled in. When the prefix
	// throws, TypeScript has also started the check; Go does not, because a
	// failed message is redelivered and checked again.
	prepared, err := h.prepareEvent(payload, true)
	if err != nil {
		return nil, err
	}

	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		h.writeProfile(ctx, prepared)
	}()
	go func() {
		defer wait.Done()
		// A failing check never fails the event.
		if err := h.Notifier.Check(ctx, sc.requestID, payload.value()); err != nil {
			sc.logger.Warn("notification check failed", slog.Any("err", err))
		}
	}()
	wait.Wait()
	// Only after the event is accepted: recording the first event before a
	// failed createEvent would claim data that was never persisted.
	if err := h.Projects.MarkFirstEvent(ctx, projectID); err != nil {
		sc.logger.Debug("markFirstEvent failed", slog.Any("err", err))
	}
	return prepared.row, nil
}

// enqueueSessionEnd adds the session-end job of the session a boundary closed;
// its id makes a second add of the same session a no-op.
func (h *Handler) enqueueSessionEnd(ctx context.Context, sc scope, base *eventPayload, closed *jsjson.Obj) error {
	event := shallowCopy(base.obj)
	event.Set("projectId", get(closed, "project_id"))
	event.Set("deviceId", get(closed, "device_id"))
	event.Set("sessionId", get(closed, "id"))
	event.Set("profileId", orElse(get(closed, "profile_id"), get(base.obj, "profileId")))

	for _, key := range [...]string{"id", "project_id", "device_id", "ended_at"} {
		if v := get(closed, key); v == nil || v.Kind != jsjson.String {
			return fmt.Errorf("session end snapshot.%s is not a string", key)
		}
	}
	for _, key := range [...]string{"projectId", "deviceId"} {
		if v := get(event, key); v == nil || v.Kind != jsjson.String {
			return fmt.Errorf("session end event.%s is not a string", key)
		}
	}
	job := jsjson.NewObj()
	job.Set("event", jsjson.NewObject(event))
	job.Set("snapshot", jsjson.NewObject(closed))
	body, err := stringify(jsjson.NewObject(job))
	if err != nil {
		return err
	}
	_, err = h.Queue.Add(ctx, sessionsQueueName, bullmq.SessionJob, bullmq.Envelope([]byte(body), sc.requestID), bullmq.AddOptions{
		JobID:       sessionEndJobIDPrefix + stringOf(get(closed, "id")),
		Attempts:    sessionEndAttempts,
		AttemptsSet: true,
		Backoff:     bullmq.Backoff{Type: sessionEndBackoffType, DelayMs: sessionEndBackoffMs},
	})
	return err
}

// deriveBaseEvent is the first half of ingestIncomingEvent: every derived
// field of baseEvent, in baseEvent's key order.
func (h *Handler) deriveBaseEvent(in wire.IncomingEventPayload, headers map[string]json.RawMessage) (*eventPayload, error) {
	propsValue, err := parseRaw(in.Event.Properties)
	if err != nil {
		return nil, err
	}
	properties := jsjson.NewObj()
	if !isNullish(propsValue) {
		spreadInto(properties, propsValue)
	}
	getProperty := func(name string) *jsjson.Value {
		value := properties.Get(name)
		if !isTruthy(value) {
			value = properties.Get(strings.Replace(name, "__", "", 1))
		}
		return undefinedIfNull(value)
	}

	profileValue, err := parseRaw(in.Event.ProfileID)
	if err != nil {
		return nil, err
	}
	profileID := ""
	if isTruthy(profileValue) {
		profileID = stringOf(profileValue)
	}
	createdAt, err := dateFromTimestamp(in.Event.Timestamp)
	if err != nil {
		return nil, err
	}

	url := getProperty("__path")
	parsed, err := jsport.ParsePathValue(url)
	if err != nil {
		return nil, err
	}
	referrerValue := getProperty("__referrer")
	var referrer *jsport.Referrer
	if !jsport.IsSameDomainValue(referrerValue, url) {
		parsedReferrer, err := jsport.ParseReferrerValue(referrerValue)
		if err != nil {
			return nil, err
		}
		referrer = &parsedReferrer
	}
	utmReferrer, err := jsport.GetReferrerWithQuery(parsed.Query)
	if err != nil {
		return nil, err
	}

	// A client may send its own __query (first-touch UTM tags it persisted);
	// the URL's query wins over it.
	clientQuery := properties.Get("__query")
	eventProperties := shallowCopy(properties)
	if parsed.Hash != "" {
		eventProperties.Set("__hash", str(parsed.Hash))
	} else {
		eventProperties.Set("__hash", nil)
	}
	if isTruthy(clientQuery) || parsed.Query != nil {
		query := jsjson.NewObj()
		spreadInto(query, clientQuery)
		if parsed.Query != nil {
			spreadInto(query, jsjson.NewObject(parsed.Query))
		}
		eventProperties.Set("__query", jsjson.NewObject(query))
	} else {
		eventProperties.Set("__query", nil)
	}
	for _, key := range globalProperties {
		eventProperties.Delete(key)
	}

	sdkName, err := headerValue(headers, headerSDKName)
	if err != nil {
		return nil, err
	}
	sdkVersion, err := headerValue(headers, headerSDKVersion)
	if err != nil {
		return nil, err
	}

	var groups *jsjson.Value
	groupsValue, err := parseRaw(in.Event.Groups)
	if err != nil {
		return nil, err
	}
	groups = orDefault(groupsValue, &jsjson.Value{Kind: jsjson.Array})

	referrerURL := ""
	if referrer != nil {
		referrerURL = referrer.URL
	}
	var referrerName *jsjson.Value
	switch {
	case utmReferrer != nil && utmReferrer.Name != "":
		referrerName = str(utmReferrer.Name)
	case referrer != nil && referrer.Name != "":
		referrerName = str(referrer.Name)
	case referrer != nil:
		referrerName = str(referrer.URL)
	}
	referrerType := ""
	switch {
	case utmReferrer != nil && utmReferrer.Type != "":
		referrerType = utmReferrer.Type
	case referrer != nil:
		referrerType = referrer.Type
	}

	revenue, err := deriveRevenue(in.Event.Name, propsValue, properties)
	if err != nil {
		return nil, err
	}

	o := jsjson.NewObj()
	o.Set("name", str(in.Event.Name))
	o.Set("profileId", str(profileID))
	o.Set("projectId", str(in.ProjectID))
	o.Set("deviceId", str(in.DeviceID))
	o.Set("sessionId", str(in.SessionID))
	o.Set("properties", jsjson.NewObject(eventProperties))
	o.Set("groups", groups)
	o.Set("createdAt", dateValue(createdAt))
	o.Set("duration", num(0))
	o.Set("sdkName", sdkName)
	o.Set("sdkVersion", sdkVersion)
	o.Set("city", optString(in.Geo.City))
	o.Set("country", optString(in.Geo.Country))
	o.Set("region", optString(in.Geo.Region))
	o.Set("longitude", optNumber(in.Geo.Longitude))
	o.Set("latitude", optNumber(in.Geo.Latitude))
	o.Set("path", str(parsed.Path))
	o.Set("origin", str(parsed.Origin))
	o.Set("referrer", str(referrerURL))
	o.Set("referrerName", referrerName)
	o.Set("referrerType", str(referrerType))
	o.Set("os", optString(in.UAInfo.OS))
	o.Set("osVersion", optString(in.UAInfo.OSVersion))
	o.Set("browser", optString(in.UAInfo.Browser))
	o.Set("browserVersion", optString(in.UAInfo.BrowserVersion))
	o.Set("device", optString(in.UAInfo.Device))
	o.Set("brand", optString(in.UAInfo.Brand))
	o.Set("model", optString(in.UAInfo.Model))
	o.Set("revenue", revenue)
	return &eventPayload{obj: o, createdAt: createdAt}, nil
}

// headerValue is headers[name]: a parsed JSON member, nil when absent.
func headerValue(headers map[string]json.RawMessage, name string) (*jsjson.Value, error) {
	raw, ok := headers[name]
	if !ok {
		return nil, nil
	}
	return parseRaw(raw)
}

// deriveRevenue is the revenue member of baseEvent: only a revenue event with
// a __revenue key, parsed as parseRevenue does.
func deriveRevenue(name string, propsValue *jsjson.Value, properties *jsjson.Obj) (*jsjson.Value, error) {
	if name != nameRevenue {
		return nil, nil
	}
	if propsValue != nil && (propsValue.Kind == jsjson.String || propsValue.Kind == jsjson.Number || propsValue.Kind == jsjson.Bool) {
		return nil, fmt.Errorf("%w: Cannot use 'in' operator to search for '__revenue' in a primitive", jsport.ErrTypeError)
	}
	if !properties.Has("__revenue") {
		return nil, nil
	}
	revenue := properties.Get("__revenue")
	switch {
	case !isTruthy(revenue):
		return nil, nil
	case revenue.Kind == jsjson.Number:
		return revenue, nil
	case revenue.Kind == jsjson.String:
		parsed := jsport.ParseFloat(revenue.Str)
		if parsed != parsed {
			return nil, nil
		}
		return num(parsed), nil
	default:
		return nil, nil
	}
}

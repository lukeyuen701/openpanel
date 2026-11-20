// Package server is the HTTP edge: the three ingest routes handled natively
// and a reverse proxy to the Bun API for everything else.
//
// Every decision to hand a request to the Bun API is taken before the first
// side effect (the dedupe lock, the auth caches, Kafka), so a forwarded
// request reaches the API exactly as the client sent it and the API's own
// dedupe check does not see it twice.
package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/http/httputil"
	"strconv"
	"strings"
	"time"

	"openpanel/ingest/internal/bots"
	"openpanel/ingest/internal/clientauth"
	"openpanel/ingest/internal/config"
	"openpanel/ingest/internal/dedupe"
	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/lookups"
	"openpanel/ingest/internal/reqheaders"
	"openpanel/ingest/internal/track"
)

const (
	statusWoundDown   = http.StatusAccepted
	contentTypeJSON   = "application/json;charset=utf-8"
	contentTypeText   = "text/plain;charset=utf-8"
	internalErrorText = "Internal server error"
)

// Server serves the ingest routes.
type Server struct {
	Config   config.Config
	Auth     *clientauth.Authenticator
	Dedupe   dedupe.Checker
	Orgs     *lookups.OrganizationStore
	Pipeline *track.Pipeline
	Bots     *bots.Detector
	Logger   *slog.Logger
	Metrics  *Metrics

	proxy *httputil.ReverseProxy
}

// Handler builds the routing.
func (s *Server) Handler() http.Handler {
	s.proxy = &httputil.ReverseProxy{
		// Rewrite (not Director) adds no X-Forwarded-For: the API reads the
		// client address from the headers the client and edge sent, and an
		// extra hop must not change what it reads.
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(s.Config.UpstreamURL)
			r.Out.Host = r.In.Host
		},
		FlushInterval: -1,
		// The client's own Accept-Encoding reaches the API unchanged; Go's
		// transport would otherwise add gzip and decode the answer itself.
		Transport: &http.Transport{
			Proxy:               nil,
			DisableCompression:  true,
			MaxIdleConnsPerHost: 256,
			IdleConnTimeout:     90 * time.Second,
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			s.Logger.ErrorContext(r.Context(), "forward to the Bun API failed", slog.Any("err", err), slog.String("url", r.URL.Path))
			w.WriteHeader(http.StatusBadGateway)
		},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /track", s.instrument("track", s.handleTrack))
	mux.HandleFunc("GET /track/device-id", s.instrument("device-id", s.handleDeviceID))
	mux.HandleFunc("POST /event", s.instrument("event", s.handleLegacyEvent))
	mux.HandleFunc("/", s.instrument("other", func(w http.ResponseWriter, r *http.Request) {
		s.forward(w, r, nil, "route")
	}))
	return mux
}

// request carries what every native handler derives first.
type request struct {
	arrival   time.Time
	headers   reqheaders.Headers
	origin    string
	hasOrigin bool
	host      string
	clientIP  string
	clientID  string
}

func (s *Server) newRequest(r *http.Request) (*request, string) {
	headers := reqheaders.FromHTTP(r.Header)
	if !asciiHeaders(r.Header) {
		return nil, "header-encoding"
	}
	req := &request{arrival: time.Now(), headers: headers}
	req.origin, req.hasOrigin = headers.Get("origin")
	if req.hasOrigin && req.origin != "" {
		host, modelled := clientauth.OriginHost(req.origin)
		if !modelled {
			return nil, "origin"
		}
		req.host = host
	}
	req.clientIP, _ = reqheaders.ClientIP(headers, s.Config.IPHeaderOrder)
	req.clientID = headers.Value("openpanel-client-id")
	return req, ""
}

// asciiHeaders reports whether every header value is printable ASCII. The
// API decodes other bytes its own way, so such requests go to it.
func asciiHeaders(h http.Header) bool {
	for _, values := range h {
		for _, value := range values {
			for i := 0; i < len(value); i++ {
				if value[i] >= 0x7F || (value[i] < 0x20 && value[i] != '\t') {
					return false
				}
			}
		}
	}
	return true
}

func isJSON(r *http.Request) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	return err == nil && mediaType == "application/json"
}

// errBodyRead is a body the client stopped sending.
var errBodyRead = errors.New("server: request body ended early")

// readBody reads a body small enough to handle natively. nil, nil means a
// body to forward unread (chunked, empty or large).
func (s *Server) readBody(r *http.Request) ([]byte, error) {
	if r.ContentLength <= 0 || r.ContentLength > s.Config.MaxNativeBodyBytes {
		return nil, nil
	}
	body := make([]byte, r.ContentLength)
	if _, err := io.ReadFull(r.Body, body); err != nil {
		return nil, errBodyRead
	}
	return body, nil
}

func (s *Server) forward(w http.ResponseWriter, r *http.Request, body []byte, reason string) {
	s.Metrics.Forwarded.WithLabelValues(reason).Inc()
	if body != nil {
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.ContentLength = int64(len(body))
	}
	setOutcome(r, "forwarded:"+reason)
	s.proxy.ServeHTTP(w, r)
}

// corsHeaders is what @elysiajs/cors adds to a response on an open path.
func corsHeaders(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	h.Set("Access-Control-Allow-Credentials", "true")
	h.Set("Vary", "Origin")
	origin := r.Header.Get("Origin")
	if origin == "" {
		origin = "*"
	}
	h.Set("Access-Control-Allow-Origin", origin)
	h.Set("Access-Control-Allow-Methods", r.Method)
	names := requestHeaderNames(r)
	h.Set("Access-Control-Allow-Headers", names)
	h.Set("Access-Control-Expose-Headers", names)
}

// requestHeaderNames lists the request's header names, lowercased, host
// first as Bun lists it.
func requestHeaderNames(r *http.Request) string {
	names := []string{"host"}
	for name := range r.Header {
		names = append(names, strings.ToLower(name))
	}
	if r.ContentLength > 0 && r.Header.Get("Content-Length") == "" {
		names = append(names, "content-length")
	}
	return strings.Join(names, ", ")
}

func writeJSON(w http.ResponseWriter, r *http.Request, status int, body *jsjson.Obj) {
	corsHeaders(w, r)
	w.Header().Set("Content-Type", contentTypeJSON)
	data := jsjson.Stringify(jsjson.NewObject(body))
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

func writeText(w http.ResponseWriter, r *http.Request, status int, text string) {
	corsHeaders(w, r)
	w.Header().Set("Content-Type", contentTypeText)
	w.Header().Set("Content-Length", strconv.Itoa(len(text)))
	w.WriteHeader(status)
	_, _ = io.WriteString(w, text)
}

func object(pairs ...any) *jsjson.Obj {
	o := jsjson.NewObj()
	for i := 0; i+1 < len(pairs); i += 2 {
		key := pairs[i].(string)
		switch value := pairs[i+1].(type) {
		case string:
			o.Set(key, jsjson.NewString(value))
		case bool:
			o.Set(key, jsjson.NewBool(value))
		case int:
			o.Set(key, jsjson.NewNumber(float64(value)))
		case *jsjson.Obj:
			o.Set(key, jsjson.NewObject(value))
		}
	}
	return o
}

// internalError is the API's 500: plain text in production, the JSON
// envelope otherwise.
func (s *Server) internalError(w http.ResponseWriter, r *http.Request, err error) {
	s.Logger.ErrorContext(r.Context(), "internal server error", slog.Any("err", err), slog.String("url", r.URL.Path))
	setOutcome(r, "error")
	if s.Config.Production {
		writeText(w, r, http.StatusInternalServerError, internalErrorText)
		return
	}
	writeJSON(w, r, http.StatusInternalServerError, object("status", http.StatusInternalServerError, "error", "Error", "message", err.Error()))
}

// checkDuplicate is isDuplicateIngestRequest for a request that is not a
// replay. payload is the body the route validated, nil on GET.
func (s *Server) checkDuplicate(ctx context.Context, req *request, payload *jsjson.Value) (bool, error) {
	if req.clientIP == "" || req.origin == "" || req.clientID == "" {
		return false, nil
	}
	return s.Dedupe.IsDuplicate(ctx, dedupe.Key(payload, req.clientIP, req.origin, req.clientID))
}

func (s *Server) authenticate(ctx context.Context, req *request, body *jsjson.Value) (clientauth.Outcome, error) {
	return s.Auth.Authenticate(ctx, clientauth.Request{
		Headers: req.headers, ClientIP: req.clientIP, Body: body,
		Origin: req.origin, HasOrigin: req.hasOrigin, OriginHost: req.host,
	})
}

// woundDown is isIngestionWoundDown: self-hosted never blocks, and a failed
// lookup lets the event through.
func (s *Server) woundDown(ctx context.Context, projectID string) bool {
	if s.Config.SelfHosted || projectID == "" {
		return false
	}
	org, err := s.Orgs.ByProject(ctx, projectID)
	if err != nil {
		s.Logger.ErrorContext(ctx, "Wind-down check failed, allowing ingestion", slog.Any("err", err), slog.String("projectId", projectID))
		return false
	}
	if org.WindDownStep == nil || !lookups.WoundDownSteps[*org.WindDownStep] {
		return false
	}
	s.Logger.InfoContext(ctx, "Ingestion blocked by wind-down", slog.String("organizationId", org.ID), slog.String("projectId", projectID), slog.String("windDownStep", *org.WindDownStep))
	return true
}

func (s *Server) refuseUnauthorized(w http.ResponseWriter, r *http.Request, message string) {
	s.Logger.WarnContext(r.Context(), "Invalid SDK request", slog.String("message", message))
	setOutcome(r, "unauthorized")
	writeText(w, r, http.StatusUnauthorized, message)
}

// handleTrack is POST /track.
func (s *Server) handleTrack(w http.ResponseWriter, r *http.Request) {
	req, reason := s.newRequest(r)
	if reason != "" {
		s.forward(w, r, nil, reason)
		return
	}
	if !isJSON(r) {
		s.forward(w, r, nil, "content-type")
		return
	}
	raw, err := s.readBody(r)
	if err != nil {
		setOutcome(r, "aborted")
		return
	}
	if raw == nil {
		s.forward(w, r, nil, "body-size")
		return
	}
	value, parseErr := jsjson.Parse(raw)
	if parseErr != nil {
		s.forward(w, r, raw, "json")
		return
	}
	body, ok := track.ParseTrackBody(value)
	if !ok {
		s.forward(w, r, raw, "type-or-schema")
		return
	}
	if reason := s.trackForwardReason(req, body); reason != "" {
		s.forward(w, r, raw, reason)
		return
	}
	ctx := r.Context()

	duplicate, err := s.checkDuplicate(ctx, req, body)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	if duplicate {
		setOutcome(r, "duplicate")
		writeJSON(w, r, http.StatusOK, object("duplicate", true))
		return
	}
	outcome, err := s.authenticate(ctx, req, body)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	if !outcome.OK() {
		s.refuseUnauthorized(w, r, outcome.Refusal)
		return
	}
	if limit := s.Config.Kafka.MaxMessageBytes; r.ContentLength > int64(limit) {
		setOutcome(r, "too-large")
		writeJSON(w, r, http.StatusRequestEntityTooLarge, object(
			"status", http.StatusRequestEntityTooLarge, "error", "Payload Too Large",
			"message", "Body is "+strconv.FormatInt(r.ContentLength, 10)+" bytes; the limit is "+strconv.Itoa(limit)+"."))
		return
	}
	projectID := *outcome.Client.ProjectID
	if s.woundDown(ctx, projectID) {
		setOutcome(r, "wound-down")
		writeJSON(w, r, statusWoundDown, object("blocked", true))
		return
	}
	produced := time.Now()
	result, err := s.Pipeline.Track(ctx, track.Input{
		ProjectID: projectID, ClientIP: req.clientIP, Headers: req.headers,
		SecretVerified: outcome.SecretVerified, ArrivalMs: req.arrival.UnixMilli(),
	}, body)
	s.Metrics.Produce.Observe(time.Since(produced).Seconds())
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	setOutcome(r, "accepted")
	writeJSON(w, r, http.StatusOK, object("deviceId", result.DeviceID, "sessionId", result.SessionID))
}

// trackForwardReason names what in a valid track body the receiver leaves to
// the Bun API, or "" when it handles the event itself.
func (s *Server) trackForwardReason(req *request, body *jsjson.Value) string {
	properties := body.Path("payload", "properties")
	if properties != nil {
		if properties.Obj.Has("__identify") {
			// A profile write, which the worker's profile buffer owns.
			return "identify"
		}
		if ip := properties.Get("__ip"); ip != nil && ip.Kind != jsjson.String && jsTruthy(ip) {
			return "ip-override"
		}
		if _, ok := track.ResolveTimestamp(req.arrival.UnixMilli(), properties.Get("__timestamp")); !ok {
			return "timestamp"
		}
	}
	if ua := req.headers.Value("user-agent"); ua != "" && s.Bots.Detect(ua) != nil {
		// A bot's event goes to the bot buffer, which the API writes.
		return "bot"
	}
	return ""
}

func jsTruthy(v *jsjson.Value) bool {
	switch v.Kind {
	case jsjson.Null:
		return false
	case jsjson.Bool:
		return v.Bool
	case jsjson.Number:
		return v.Num != 0
	case jsjson.String:
		return v.Str != ""
	}
	return true
}

// handleDeviceID is GET /track/device-id.
func (s *Server) handleDeviceID(w http.ResponseWriter, r *http.Request) {
	req, reason := s.newRequest(r)
	if reason != "" {
		s.forward(w, r, nil, reason)
		return
	}
	ctx := r.Context()
	duplicate, err := s.checkDuplicate(ctx, req, nil)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	if duplicate {
		setOutcome(r, "duplicate")
		writeJSON(w, r, http.StatusOK, object("duplicate", true))
		return
	}
	outcome, err := s.authenticate(ctx, req, nil)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	if !outcome.OK() {
		s.refuseUnauthorized(w, r, outcome.Refusal)
		return
	}
	ua := req.headers.Value("user-agent")
	if !outcome.SecretVerified && ua != "" {
		if bot := s.Bots.Detect(ua); bot != nil {
			setOutcome(r, "bot")
			writeJSON(w, r, http.StatusAccepted, object("bot", object("name", bot.Name, "type", bot.Type)))
			return
		}
	}
	projectID := *outcome.Client.ProjectID
	if s.woundDown(ctx, projectID) {
		setOutcome(r, "wound-down")
		writeJSON(w, r, statusWoundDown, object("blocked", true))
		return
	}
	if _, err := s.Pipeline.Salts.Get(ctx); err != nil {
		s.internalError(w, r, err)
		return
	}
	if req.clientIP == "" {
		writeText(w, r, http.StatusBadRequest, "Missing ip address")
		return
	}
	if ua == "" {
		writeText(w, r, http.StatusBadRequest, "Missing header: user-agent")
		return
	}
	found, err := s.Pipeline.DeviceLookup(ctx, projectID, req.clientIP, ua, time.Now().UnixMilli())
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	setOutcome(r, "accepted")
	writeJSON(w, r, http.StatusOK, object("deviceId", found.DeviceID, "sessionId", found.SessionID, "message", found.Message))
}

// handleLegacyEvent is POST /event.
func (s *Server) handleLegacyEvent(w http.ResponseWriter, r *http.Request) {
	req, reason := s.newRequest(r)
	if reason != "" {
		s.forward(w, r, nil, reason)
		return
	}
	if !isJSON(r) {
		s.forward(w, r, nil, "content-type")
		return
	}
	raw, err := s.readBody(r)
	if err != nil {
		setOutcome(r, "aborted")
		return
	}
	if raw == nil {
		s.forward(w, r, nil, "body-size")
		return
	}
	body, parseErr := jsjson.Parse(raw)
	if parseErr != nil || !body.IsObject() {
		s.forward(w, r, raw, "json")
		return
	}
	if properties := body.Get("properties"); properties != nil {
		if !properties.IsObject() {
			// A primitive here makes the API throw when it marks the event.
			s.forward(w, r, raw, "properties")
			return
		}
		if _, ok := track.ResolveTimestamp(req.arrival.UnixMilli(), properties.Get("__timestamp")); !ok {
			s.forward(w, r, raw, "timestamp")
			return
		}
	}
	if ua := req.headers.Value("user-agent"); ua != "" && s.Bots.Detect(ua) != nil {
		s.forward(w, r, raw, "bot")
		return
	}
	ctx := r.Context()
	isReplay := false
	if kind := body.Get("type"); kind != nil && kind.Kind == jsjson.String && kind.Str == "replay" {
		isReplay = true
	}
	if !isReplay {
		duplicate, err := s.checkDuplicate(ctx, req, body)
		if err != nil {
			s.internalError(w, r, err)
			return
		}
		if duplicate {
			setOutcome(r, "duplicate")
			writeJSON(w, r, http.StatusOK, object("duplicate", true))
			return
		}
	}
	outcome, err := s.authenticate(ctx, req, body)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	if !outcome.OK() {
		s.refuseUnauthorized(w, r, outcome.Refusal)
		return
	}
	s.Metrics.LegacyEvent.WithLabelValues(outcome.Client.ID).Inc()
	projectID := *outcome.Client.ProjectID
	if s.woundDown(ctx, projectID) {
		setOutcome(r, "wound-down")
		writeJSON(w, r, statusWoundDown, object("blocked", true))
		return
	}
	if err := s.Pipeline.Legacy(ctx, track.Input{
		ProjectID: projectID, ClientIP: req.clientIP, Headers: req.headers,
		SecretVerified: outcome.SecretVerified, ArrivalMs: req.arrival.UnixMilli(),
	}, body); err != nil {
		s.internalError(w, r, err)
		return
	}
	setOutcome(r, "accepted")
	writeText(w, r, http.StatusAccepted, "ok")
}

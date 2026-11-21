// Package chclient is the Go side of packages/db's ClickHouse client over the
// HTTP interface: the same URL, settings, body and failover as the Node
// client, which the buffers' flushes reach as `ch.insert`, `ch.command` and
// `chQuery`.
//
// Every attempt of a call builds its own request body from the rows the caller
// handed over, so a retry on another node can never send an empty one (the
// one-shot stream hazard of the TypeScript client, P2.10 #1).
package chclient

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"openpanel/ingest/internal/worker/clock"
	"openpanel/ingest/internal/worker/config"
)

const (
	minAttempts       = 3
	retryBaseDelay    = 200 * time.Millisecond
	retryMaxDelay     = 2 * time.Second
	idleSocketTTL     = 7 * time.Second // below the server's 10 s keep_alive_timeout
	connectTimeout    = 10 * time.Second
	attemptCapFactor  = 10
	defaultDatabase   = "default"
	defaultUsername   = "default"
	insertFormat      = "JSONEachRow"
	queryDefaultKey   = "default_format"
	queryDefaultValue = "JSON"
	userAgent         = "openpanel-go-worker"
	maxErrorBodyBytes = 64 << 10
)

// Config is what a client needs: the node list from CLICKHOUSE_URL and the
// CLICKHOUSE_* tuning.
type Config struct {
	URLs []string
	config.ClickHouseConfig
}

// FromWorkerConfig takes both from the worker's parsed environment.
func FromWorkerConfig(cfg config.Config) Config {
	return Config{URLs: cfg.ClickHouseURLs, ClickHouseConfig: cfg.ClickHouse}
}

// Deps are what a Client reads from outside. Only Logger may be nil.
type Deps struct {
	Clock  clock.Clock
	Logger *slog.Logger
	// Transport replaces the per-node HTTP transport, for tests.
	Transport http.RoundTripper
}

// Inserter is the write side the buffers depend on.
type Inserter interface {
	// Insert sends one JSONEachRow line per element of lines; the client
	// appends the newline. settings are the per-call layer.
	Insert(ctx context.Context, table string, lines []string, settings Settings) error
}

// Client round-robins calls over the nodes and retries a connection failure on
// the next one.
type Client struct {
	nodes    []*node
	picker   *picker
	defaults Settings
	compress bool
	timeout  time.Duration
	clock    clock.Clock
	logger   *slog.Logger
	cluster  bool
}

type node struct {
	label    string
	endpoint string
	database string
	auth     string
	http     *http.Client
}

// New builds a client for cfg.URLs. It makes no request.
func New(cfg Config, deps Deps) (*Client, error) {
	if len(cfg.URLs) == 0 {
		return nil, errors.New("chclient: CLICKHOUSE_URL is not set")
	}
	if deps.Clock == nil {
		return nil, errors.New("chclient: a clock is required")
	}
	logger := deps.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	client := &Client{
		defaults: clientDefaults(cfg.ClickHouseConfig),
		compress: cfg.RequestCompression,
		timeout:  cfg.RequestTimeout,
		clock:    deps.Clock,
		logger:   logger,
		cluster:  cfg.Cluster,
	}
	for _, raw := range cfg.URLs {
		node, err := newNode(raw, cfg.ClickHouseConfig, deps.Transport)
		if err != nil {
			return nil, err
		}
		client.nodes = append(client.nodes, node)
	}
	client.picker = newPicker(len(client.nodes), cfg.UnhealthyMark)
	return client, nil
}

func newNode(raw string, cfg config.ClickHouseConfig, transport http.RoundTripper) (*node, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return nil, errors.New("chclient: CLICKHOUSE_URL has an entry that is not a URL")
	}
	username, password := defaultUsername, ""
	if parsed.User != nil {
		username = parsed.User.Username()
		password, _ = parsed.User.Password()
	}
	database := strings.Trim(parsed.Path, "/")
	if transport == nil {
		transport = &http.Transport{
			Proxy:                 nil,
			DialContext:           (&net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}).DialContext,
			MaxConnsPerHost:       cfg.MaxOpenConnections,
			MaxIdleConnsPerHost:   cfg.MaxOpenConnections,
			IdleConnTimeout:       idleSocketTTL,
			ResponseHeaderTimeout: cfg.RequestTimeout,
			DisableCompression:    true,
		}
	}
	return &node{
		label:    parsed.Host,
		endpoint: (&url.URL{Scheme: parsed.Scheme, Host: parsed.Host, Path: "/"}).String(),
		database: database,
		auth:     basicAuth(username, password),
		http:     &http.Client{Transport: transport},
	}, nil
}

// Cluster reports CLICKHOUSE_CLUSTER.
func (c *Client) Cluster() bool { return c.cluster }

// Insert implements Inserter: INSERT INTO table FORMAT JSONEachRow.
func (c *Client) Insert(ctx context.Context, table string, lines []string, settings Settings) error {
	query := "INSERT INTO " + table + " FORMAT " + insertFormat
	merged := c.defaults.With(insertDefaults...).With(settings...)
	_, err := c.do(ctx, "insert", func(n *node) (*http.Request, error) {
		body, err := c.insertBody(lines)
		if err != nil {
			return nil, err
		}
		request, err := n.request(ctx, c.queryString(n, merged, nil, query), body)
		if err != nil {
			return nil, err
		}
		c.markCompressed(request)
		return request, nil
	})
	return err
}

// Command runs a statement that returns no rows (the profile backfill's
// UPDATE). Settings are the client defaults plus the per-call layer; the
// insert defaults do not apply.
func (c *Client) Command(ctx context.Context, statement string, settings Settings) error {
	return c.CommandWithParams(ctx, statement, nil, settings)
}

// CommandWithParams is Command for a statement that binds {name:Type}
// parameters (query_params of the Node client's command()).
func (c *Client) CommandWithParams(ctx context.Context, statement string, params []Param, settings Settings) error {
	merged := c.defaults.With(settings...)
	_, err := c.do(ctx, "command", func(n *node) (*http.Request, error) {
		body, err := c.encodeBody([]byte(statement))
		if err != nil {
			return nil, err
		}
		request, err := n.request(ctx, c.queryString(n, merged, params, ""), body)
		if err != nil {
			return nil, err
		}
		c.markCompressed(request)
		return request, nil
	})
	return err
}

// Param is a query parameter, value already in ClickHouse's text form (see
// FormatString and FormatStringArray); the statement refers to it as {name:Type}.
type Param struct{ Name, Value string }

// Result is a FORMAT JSON answer. Data rows stay raw so a caller decodes them
// into its own shape; 64-bit integers arrive as strings, as with the Node client.
type Result struct {
	Meta []Column          `json:"meta"`
	Data []json.RawMessage `json:"data"`
	Rows int64             `json:"rows"`
}

// Column is one entry of a result's meta.
type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// Query runs a SELECT and returns its JSON answer.
func (c *Client) Query(ctx context.Context, statement string, params []Param, settings Settings) (*Result, error) {
	merged := c.defaults.With(settings...).With(Setting{Key: queryDefaultKey, Value: queryDefaultValue})
	body, err := c.do(ctx, "query", func(n *node) (*http.Request, error) {
		return n.request(ctx, c.queryString(n, merged, params, ""), strings.NewReader(statement))
	})
	if err != nil {
		return nil, err
	}
	var result Result
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("chclient: query answer is not JSON: %w", err)
	}
	return &result, nil
}

func (c *Client) queryString(n *node, settings Settings, params []Param, query string) string {
	pairs := [][2]string{{"query_id", newQueryID()}}
	for _, param := range params {
		pairs = append(pairs, [2]string{"param_" + param.Name, param.Value})
	}
	for _, setting := range settings {
		pairs = append(pairs, [2]string{setting.Key, setting.Value})
	}
	if n.database != "" && n.database != defaultDatabase {
		pairs = append(pairs, [2]string{"database", n.database})
	}
	if query != "" {
		pairs = append(pairs, [2]string{"query", query})
	}
	return encodeQuery(pairs)
}

func (n *node) request(ctx context.Context, query string, body io.Reader) (*http.Request, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, n.endpoint+"?"+query, body)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", n.auth)
	request.Header.Set("User-Agent", userAgent)
	return request, nil
}

// insertBody serialises lines afresh: each one terminated by a newline.
func (c *Client) insertBody(lines []string) (io.Reader, error) {
	var plain bytes.Buffer
	for _, line := range lines {
		plain.WriteString(line)
		plain.WriteByte('\n')
	}
	return c.encodeBody(plain.Bytes())
}

// encodeBody gzips the body when the client compresses requests, which the
// Node client does for inserts and commands alike but not for queries.
func (c *Client) encodeBody(plain []byte) (io.Reader, error) {
	if !c.compress {
		return bytes.NewReader(plain), nil
	}
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(plain); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return &compressed, nil
}

func (c *Client) markCompressed(request *http.Request) {
	if c.compress {
		request.Header.Set("Content-Encoding", "gzip")
	}
}

// do is withRoundRobinRetry: attempts = max(3, nodes+1), a delay of
// min(200 ms * 2^attempt, 2 s) between them, the next node every time.
func (c *Client) do(ctx context.Context, operation string, build func(*node) (*http.Request, error)) ([]byte, error) {
	attempts := max(minAttempts, len(c.nodes)+1)
	var lastErr error
	for attempt := range attempts {
		index := c.picker.pick(c.clock.Now())
		target := c.nodes[index]
		body, err := c.attempt(ctx, target, build)
		if err == nil {
			if attempt > 0 {
				c.picker.markHealthy(index)
				c.logger.Info("CH operation succeeded after retry", "node", target.label, "attempt", attempt+1, "operation", operation)
			}
			return body, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return nil, err
		}
		class := Classify(err)
		if class == ClassServer || class == ClassOther {
			return nil, err
		}
		if class == ClassNodeDown {
			c.picker.markUnhealthy(index, c.clock.Now())
		}
		if attempt == attempts-1 {
			break
		}
		delay := min(retryBaseDelay<<attempt, retryMaxDelay)
		c.logger.Warn("CH operation failed; retrying on next node", "err", err, "class", class,
			"markedUnhealthy", class == ClassNodeDown, "node", target.label, "attempt", attempt+1,
			"maxAttempts", attempts, "nextDelayMs", delay.Milliseconds(), "operation", operation)
		if err := c.clock.Sleep(ctx, delay); err != nil {
			return nil, err
		}
	}
	return nil, lastErr
}

func (c *Client) attempt(ctx context.Context, target *node, build func(*node) (*http.Request, error)) ([]byte, error) {
	// A hard ceiling, beyond the response-header timeout, so a server that
	// trickles bytes cannot hold a flush (and its lock) forever.
	attemptCtx, cancel := context.WithTimeout(ctx, c.timeout*attemptCapFactor)
	defer cancel()
	request, err := build(target)
	if err != nil {
		return nil, err
	}
	response, err := target.http.Do(request.WithContext(attemptCtx))
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		raw, _ := io.ReadAll(io.LimitReader(response.Body, maxErrorBodyBytes))
		return nil, newServerError(response.StatusCode, string(raw), response.Header.Get("X-ClickHouse-Exception-Code"))
	}
	return io.ReadAll(response.Body)
}

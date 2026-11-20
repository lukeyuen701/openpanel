// Command ingest is OpenPanel's Go event receiver. It serves POST /track,
// GET /track/device-id and POST /event, producing to the same Kafka topic the
// Bun API produces to, and hands every other request to the Bun API.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"openpanel/ingest/internal/bots"
	"openpanel/ingest/internal/clientauth"
	"openpanel/ingest/internal/config"
	"openpanel/ingest/internal/dedupe"
	"openpanel/ingest/internal/geo"
	"openpanel/ingest/internal/identity"
	"openpanel/ingest/internal/kafka"
	"openpanel/ingest/internal/kv"
	"openpanel/ingest/internal/lookups"
	"openpanel/ingest/internal/server"
	"openpanel/ingest/internal/track"
	"openpanel/ingest/internal/useragent"
)

const (
	shutdownTimeout   = 15 * time.Second
	readHeaderTimeout = 10 * time.Second
	idleTimeout       = 120 * time.Second
	dependencyTimeout = 2 * time.Second
)

func main() {
	// The image has no shell or curl; its healthcheck runs the binary.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck())
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ingest:", err)
		os.Exit(1)
	}
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "trace", "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error", "fatal":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(server.LogHandler{Handler: handler}).With(slog.String("name", "ingest"))
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func run() error {
	cfg, err := config.FromEnvironment()
	if err != nil {
		return err
	}
	logger := newLogger(cfg.LogLevel)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rdb, err := kv.Open(cfg.RedisURL, cfg.RedisTimeout)
	if err != nil {
		return err
	}
	defer rdb.Close()

	pgURL, err := config.PostgresURL(cfg.DatabaseURL)
	if err != nil {
		return err
	}
	db, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		return fmt.Errorf("DATABASE_URL: %w", err)
	}
	defer db.Close()

	producer, err := kafka.New(cfg.Kafka)
	if err != nil {
		return err
	}

	uaParser, err := useragent.Default()
	if err != nil {
		return err
	}
	botDetector, err := bots.New()
	if err != nil {
		return err
	}
	geoResolver, problems := geo.Open(filepath.Join(cfg.GeoDir, "GeoLite2-City.mmdb"), filepath.Join(cfg.GeoDir, "GeoLite2-ASN.mmdb"))
	for _, problem := range problems {
		// The API runs without the databases too; every lookup is then empty.
		logger.Error("geo database unavailable", slog.Any("err", problem))
	}
	defer geoResolver.Close()

	metrics := server.NewMetrics()
	srv := &server.Server{
		Config:  cfg,
		Auth:    &clientauth.Authenticator{Store: clientauth.NewStore(rdb, db), Verifier: clientauth.NewVerifier(rdb)},
		Dedupe:  dedupe.Checker{Redis: rdb},
		Orgs:    lookups.NewOrganizationStore(rdb, db),
		Bots:    botDetector,
		Logger:  logger,
		Metrics: metrics,
		Pipeline: &track.Pipeline{
			UA:        uaParser,
			Bots:      botDetector,
			Geo:       geoResolver,
			Salts:     &lookups.SaltStore{Redis: rdb, DB: db},
			Sessions:  identity.RedisSessions{Client: rdb},
			TimeoutMs: cfg.SessionTimeout.Milliseconds(),
			Producer:  producer,
			Logger:    logger,
			NewUUID:   newUUID,
		},
	}

	public := &http.Server{
		Addr:              net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		Handler:           srv.Handler(),
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
	admin := &http.Server{
		Addr:              net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.AdminPort)),
		Handler:           adminHandler(metrics, rdb, db, producer),
		ReadHeaderTimeout: readHeaderTimeout,
	}

	errs := make(chan error, 2)
	go func() { errs <- serve(public) }()
	go func() { errs <- serve(admin) }()
	logger.Info("ingest listening",
		slog.String("addr", public.Addr), slog.String("admin", admin.Addr),
		slog.String("upstream", cfg.UpstreamURL.String()), slog.String("topic", cfg.Kafka.Topic),
		slog.String("uaParser", uaParser.TablesVersion()), slog.Bool("selfHosted", cfg.SelfHosted))

	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
	}
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	// Stop taking requests and let in-flight ones finish, then flush whatever
	// the producer still lingers on.
	shutdownErr := errors.Join(public.Shutdown(shutdownCtx), admin.Shutdown(shutdownCtx))
	return errors.Join(shutdownErr, producer.Close(shutdownCtx))
}

// healthcheck asks this container's own admin port whether it is ready.
func healthcheck() int {
	port := os.Getenv("INGEST_ADMIN_PORT")
	if port == "" {
		port = "3001"
	}
	client := http.Client{Timeout: dependencyTimeout + time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/healthz/ready")
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck:", err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "healthcheck: status", resp.StatusCode)
		return 1
	}
	return 0
}

func serve(s *http.Server) error {
	if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("listen %s: %w", s.Addr, err)
	}
	return nil
}

// adminHandler is liveness, readiness and metrics, on a port the edge does
// not route to.
func adminHandler(metrics *server.Metrics, rdb kv.Store, db *pgxpool.Pool, producer *kafka.Producer) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz/live", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /healthz/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), dependencyTimeout)
		defer cancel()
		checks := map[string]error{
			"redis": rdb.Ping(ctx),
			"db":    db.Ping(ctx),
			"kafka": producer.Ping(ctx),
		}
		status := http.StatusOK
		body := ""
		for name, err := range checks {
			state := "ok"
			if err != nil {
				state = err.Error()
				status = http.StatusServiceUnavailable
			}
			body += name + ": " + state + "\n"
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))
	// Profiling stays on the admin port, which the edge does not route to.
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	return mux
}

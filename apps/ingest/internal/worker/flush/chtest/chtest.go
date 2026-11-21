// Package chtest is what the live tests of the flush packages share: the
// WPARITY_CH gate, a throwaway ClickHouse database built from the repository's
// flat schema, and the Redis URL of the parity databases. It is imported by
// tests only.
package chtest

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const (
	// GateEnv enables every live test; its value is the ClickHouse base URL,
	// or any other non-empty value for the local default.
	GateEnv           = "WPARITY_CH"
	RedisEnv          = "WPARITY_REDIS"
	defaultClickHouse = "http://localhost:23123"
	// DefaultRedis is parity database 11 of the local Redis. Databases 11-15 are
	// the only ones live tests may use.
	DefaultRedis    = "redis://127.0.0.1:23379/11"
	databasePrefix  = "openpanel_wparity_flush_"
	schemaPath      = "test/clickhouse-schema.sql"
	statementBreak  = "\n;"
	requestDeadline = 2 * time.Minute
)

// BaseURL skips the test unless WPARITY_CH is set, and returns the ClickHouse
// base URL (no database).
func BaseURL(t *testing.T) string {
	t.Helper()
	value := os.Getenv(GateEnv)
	if value == "" {
		t.Skipf("%s is not set", GateEnv)
	}
	if strings.HasPrefix(value, "http") {
		return strings.TrimRight(value, "/")
	}
	return defaultClickHouse
}

// RedisURL is WPARITY_REDIS or DefaultRedis; the test fails on a database
// outside 11-15, which belongs to someone else.
func RedisURL(t *testing.T) string {
	t.Helper()
	raw := os.Getenv(RedisEnv)
	if raw == "" {
		raw = DefaultRedis
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("%s: %v", RedisEnv, err)
	}
	var index int
	if _, err := fmt.Sscanf(strings.TrimPrefix(parsed.Path, "/"), "%d", &index); err != nil || index < 11 || index > 15 {
		t.Fatalf("%s must name Redis database 11-15, got %q", RedisEnv, raw)
	}
	return raw
}

// RedisURLForDB is the Redis URL with another parity database.
func RedisURLForDB(t *testing.T, db int) string {
	t.Helper()
	if db < 11 || db > 15 {
		t.Fatalf("Redis database %d is not a parity database", db)
	}
	parsed, err := url.Parse(RedisURL(t))
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = fmt.Sprintf("/%d", db)
	return parsed.String()
}

// Exec runs one statement against base (and database, when not empty) and
// returns the response body.
func Exec(t *testing.T, base, database, statement string) string {
	t.Helper()
	out, err := ExecErr(base, database, statement)
	if err != nil {
		t.Fatalf("ClickHouse: %v\nstatement: %.200s", err, statement)
	}
	return out
}

// ExecErr is Exec returning the error.
func ExecErr(base, database, statement string) (string, error) {
	query := url.Values{}
	if database != "" {
		query.Set("database", database)
	}
	client := http.Client{Timeout: requestDeadline}
	response, err := client.Post(base+"/?"+query.Encode(), "text/plain", bytes.NewBufferString(statement))
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	return string(body), nil
}

// Database creates openpanel_wparity_flush_<suffix> from the flat schema
// (the code migrations flattened, which need Postgres and so cannot run
// against a database of their own), and drops it when the test ends. It
// returns the name.
func Database(t *testing.T, base, suffix string) string {
	t.Helper()
	name := databasePrefix + suffix
	Exec(t, base, "", "DROP DATABASE IF EXISTS "+name)
	Exec(t, base, "", "CREATE DATABASE "+name)
	t.Cleanup(func() {
		if _, err := ExecErr(base, "", "DROP DATABASE IF EXISTS "+name); err != nil {
			t.Errorf("dropping %s: %v", name, err)
		}
	})
	for _, statement := range schemaStatements(t) {
		Exec(t, base, name, statement)
	}
	return name
}

func schemaStatements(t *testing.T) []string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	dir := filepath.Dir(thisFile)
	for range 12 {
		raw, err := os.ReadFile(filepath.Join(dir, schemaPath))
		if err == nil {
			var lines []string
			for _, line := range strings.Split(string(raw), "\n") {
				if !strings.HasPrefix(line, "--") {
					lines = append(lines, line)
				}
			}
			var statements []string
			for _, statement := range strings.Split(strings.Join(lines, "\n"), statementBreak) {
				if statement = strings.TrimSpace(statement); statement != "" {
					statements = append(statements, statement)
				}
			}
			return statements
		}
		dir = filepath.Dir(dir)
	}
	t.Fatalf("%s not found above %s", schemaPath, thisFile)
	return nil
}

// URLWithDatabase is base with the database as the URL path, the form of CLICKHOUSE_URL.
func URLWithDatabase(base, database string) string { return base + "/" + database }

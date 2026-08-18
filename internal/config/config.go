// Package config loads worker configuration from environment variables.
//
// Every setting is read from the environment rather than from files, so this
// package intentionally does not know anything about the Docker Swarm
// secrets convention (files mounted under /run/secrets/*) used by the
// maevsi stack in production. Whatever injects the environment for this
// process (a compose "secrets" mapping, a systemd EnvironmentFile, a plain
// .env in local dev) is responsible for turning those secrets into the
// environment variables documented below and in the README.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the fully resolved worker configuration.
type Config struct {
	Temporal Temporal
	Metrics  Metrics
	Postgres Postgres
	S3       S3
	Sentry   Sentry
	Schedule Schedule
}

// Temporal holds the connection details for the self-hosted Temporal
// Server (not Temporal Cloud) this worker talks to.
type Temporal struct {
	// HostPort is the address of the Temporal frontend service, e.g.
	// "temporal-server:7233". Matches client.Options.HostPort.
	HostPort string
	// Namespace is the Temporal namespace to operate in.
	Namespace string
	// TaskQueue is the task queue this worker polls and that schedules
	// dispatch workflow tasks to.
	TaskQueue string
}

// Metrics configures the Prometheus metrics HTTP server exposed by the
// worker process, fed by the Temporal Go SDK's native metrics.
type Metrics struct {
	// Addr is the listen address for the metrics HTTP server, e.g. ":9090".
	Addr string
	// Path is the path the Prometheus handler is mounted on, e.g. "/metrics".
	Path string
}

// Postgres holds connection details for the org's existing central
// Postgres instance, reached via a dedicated role for this service
// (idiomatically named something like vibetype_role_service_temporal_worker
// on the server side; this package has no opinion on the name).
type Postgres struct {
	Host     string
	Port     int
	Database string
	User     string
	Password string
	// SSLMode is passed through to pgx verbatim (e.g. "disable", "require",
	// "verify-full"). Defaults to "require".
	SSLMode string
}

// DSN renders the connection details as a libpq-style connection string
// suitable for pgxpool.New.
func (p Postgres) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		p.Host, p.Port, p.Database, p.User, p.Password, p.SSLMode,
	)
}

// S3 holds the credentials and bucket configuration needed to reproduce
// the original `aws s3 sync /backups s3://<bucket>/backups` command.
type S3 struct {
	Bucket          string
	Prefix          string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	// Endpoint overrides the default AWS endpoint, for S3-compatible
	// object storage. Leave empty to use AWS S3 itself.
	Endpoint string
	// UsePathStyle forces path-style addressing, typically required by
	// non-AWS S3-compatible endpoints.
	UsePathStyle bool
	// SourceDir is the local directory synced to S3, e.g. "/backups".
	SourceDir string
}

// Sentry holds the Sentry Crons check-in URLs used for the two jobs, one
// per job so each keeps its own monitor slug, matching the original
// SENTRY_CRONS / SENTRY_CRONS_OUTBOX_PURGE environment variables.
//
// Either URL may be left empty, in which case check-ins for that job are
// skipped rather than treated as an error, mirroring the original setup
// where non-production environments fell back to email instead of Sentry.
type Sentry struct {
	DBBackupCheckInURL    string
	OutboxPurgeCheckInURL string
}

// Schedule holds the cadences the two Temporal Schedules are created with.
// They default to the original jobber cadences (daily / every 2 hours) but
// are configurable so they can be tightened for local testing.
type Schedule struct {
	DBBackupEvery       time.Duration
	OutboxPurgeEvery    time.Duration
	OutboxPurgeRetention time.Duration
}

// Load reads configuration from the environment, applying defaults where
// documented in the README, and returns an error describing every missing
// required variable at once.
func Load() (Config, error) {
	var errs []string

	cfg := Config{
		Temporal: Temporal{
			HostPort:  getEnvDefault("TEMPORAL_HOST_PORT", "temporal-server:7233"),
			Namespace: getEnvDefault("TEMPORAL_NAMESPACE", "default"),
			TaskQueue: getEnvDefault("TEMPORAL_TASK_QUEUE", "maevsi-jobs"),
		},
		Metrics: Metrics{
			Addr: getEnvDefault("METRICS_ADDR", ":9090"),
			Path: getEnvDefault("METRICS_PATH", "/metrics"),
		},
		Postgres: Postgres{
			Host:     getEnvDefault("POSTGRES_HOST", "localhost"),
			Port:     getEnvIntDefault("POSTGRES_PORT", 5432, &errs),
			Database: requireEnv("POSTGRES_DATABASE", &errs),
			User:     requireEnv("POSTGRES_USER", &errs),
			Password: requireEnv("POSTGRES_PASSWORD", &errs),
			SSLMode:  getEnvDefault("POSTGRES_SSLMODE", "require"),
		},
		S3: S3{
			Bucket:          requireEnv("S3_BUCKET", &errs),
			Prefix:          getEnvDefault("S3_PREFIX", "backups"),
			Region:          requireEnv("S3_REGION", &errs),
			AccessKeyID:     requireEnv("S3_ACCESS_KEY_ID", &errs),
			SecretAccessKey: requireEnv("S3_SECRET_ACCESS_KEY", &errs),
			Endpoint:        os.Getenv("S3_ENDPOINT"),
			UsePathStyle:    getEnvBoolDefault("S3_USE_PATH_STYLE", false, &errs),
			SourceDir:       getEnvDefault("BACKUP_SOURCE_DIR", "/backups"),
		},
		Sentry: Sentry{
			DBBackupCheckInURL:    os.Getenv("SENTRY_CRONS"),
			OutboxPurgeCheckInURL: os.Getenv("SENTRY_CRONS_OUTBOX_PURGE"),
		},
		Schedule: Schedule{
			DBBackupEvery:        getEnvDurationDefault("DBBACKUP_SCHEDULE_EVERY", 24*time.Hour, &errs),
			OutboxPurgeEvery:     getEnvDurationDefault("OUTBOX_PURGE_SCHEDULE_EVERY", 2*time.Hour, &errs),
			OutboxPurgeRetention: getEnvDurationDefault("OUTBOX_PURGE_RETENTION", 24*time.Hour, &errs),
		},
	}

	if len(errs) > 0 {
		return Config{}, fmt.Errorf("invalid configuration:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return cfg, nil
}

func requireEnv(key string, errs *[]string) string {
	v := os.Getenv(key)
	if v == "" {
		*errs = append(*errs, fmt.Sprintf("%s is required", key))
	}
	return v
}

func getEnvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvIntDefault(key string, def int, errs *[]string) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		*errs = append(*errs, fmt.Sprintf("%s must be an integer, got %q", key, v))
		return def
	}
	return n
}

func getEnvBoolDefault(key string, def bool, errs *[]string) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		*errs = append(*errs, fmt.Sprintf("%s must be a boolean, got %q", key, v))
		return def
	}
	return b
}

func getEnvDurationDefault(key string, def time.Duration, errs *[]string) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		*errs = append(*errs, fmt.Sprintf("%s must be a Go duration (e.g. \"24h\"), got %q", key, v))
		return def
	}
	return d
}

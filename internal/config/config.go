// Package config loads worker configuration from environment variables.
//
// Every setting is read from the environment rather than from files, so this package intentionally does not know anything about the Docker Swarm secrets convention (files mounted under /run/secrets/*) used by the maevsi stack in production.
// Whatever injects the environment for this process (a compose "secrets" mapping, a systemd EnvironmentFile, a plain .env in local dev) is responsible for turning those secrets into the environment variables documented below and in the README.
package config

import (
	"fmt"
	"net/url"
	"time"

	"github.com/caarlos0/env/v11"
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
	// HostPort is the address of the Temporal frontend service, e.g. "temporal-server:7233".
	// Matches client.Options.HostPort.
	HostPort string `env:"TEMPORAL_HOST_PORT" envDefault:"temporal-server:7233"`
	// Namespace is the Temporal namespace to operate in.
	Namespace string `env:"TEMPORAL_NAMESPACE" envDefault:"default"`
	// TaskQueue is the task queue this worker polls and that schedules
	// dispatch workflow tasks to.
	TaskQueue string `env:"TEMPORAL_TASK_QUEUE" envDefault:"maevsi-jobs"`
}

// Metrics configures the Prometheus metrics HTTP server exposed by the
// worker process, fed by the Temporal Go SDK's native metrics.
type Metrics struct {
	// Addr is the listen address for the metrics HTTP server, e.g. ":9090".
	Addr string `env:"METRICS_ADDR" envDefault:":9090"`
	// Path is the path the Prometheus handler is mounted on, e.g. "/metrics".
	Path string `env:"METRICS_PATH" envDefault:"/metrics"`
}

// Postgres holds connection details for the org's existing central
// Postgres instance, reached via a dedicated role for this service
// (idiomatically named something like vibetype_role_service_temporal_worker
// on the server side; this package has no opinion on the name).
type Postgres struct {
	Host     string `env:"POSTGRES_HOST" envDefault:"localhost"`
	Port     int    `env:"POSTGRES_PORT" envDefault:"5432"`
	Database string `env:"POSTGRES_DATABASE,required,notEmpty"`
	User     string `env:"POSTGRES_USER,required,notEmpty"`
	Password string `env:"POSTGRES_PASSWORD,required,notEmpty"`
	// SSLMode is passed through to pgx verbatim (e.g. "disable", "require", "verify-full").
	// Defaults to "require".
	SSLMode string `env:"POSTGRES_SSLMODE" envDefault:"require"`
}

// DSN renders the connection details as a libpq-style connection string
// suitable for pgxpool.New. The password is URL-encoded to handle special
// characters (spaces, @, =, etc.) that would otherwise break parsing.
func (p Postgres) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		p.Host, p.Port, p.Database, p.User, url.QueryEscape(p.Password), p.SSLMode,
	)
}

// S3 holds the credentials and bucket configuration needed to reproduce
// the original `aws s3 sync /backups s3://<bucket>/backups` command.
type S3 struct {
	Bucket          string `env:"S3_BUCKET,required,notEmpty"`
	Prefix          string `env:"S3_PREFIX" envDefault:"backups"`
	Region          string `env:"S3_REGION,required,notEmpty"`
	AccessKeyID     string `env:"S3_ACCESS_KEY_ID,required,notEmpty"`
	SecretAccessKey string `env:"S3_SECRET_ACCESS_KEY,required,notEmpty"`
	// Endpoint overrides the default AWS endpoint, for S3-compatible object storage.
	// Leave empty to use AWS S3 itself.
	Endpoint string `env:"S3_ENDPOINT"`
	// UsePathStyle forces path-style addressing, typically required by
	// non-AWS S3-compatible endpoints.
	UsePathStyle bool `env:"S3_USE_PATH_STYLE" envDefault:"false"`
	// SourceDir is the local directory synced to S3, e.g. "/backups".
	SourceDir string `env:"BACKUP_SOURCE_DIR" envDefault:"/backups"`
}

// Sentry holds the Sentry Crons check-in URLs used for the two jobs, one
// per job so each keeps its own monitor slug, matching the original
// SENTRY_CRONS / SENTRY_CRONS_OUTBOX_PURGE environment variables.
//
// Either URL may be left empty, in which case check-ins for that job are
// skipped rather than treated as an error, mirroring the original setup
// where non-production environments fell back to email instead of Sentry.
type Sentry struct {
	DBBackupCheckInURL    string `env:"SENTRY_CRONS"`
	OutboxPurgeCheckInURL string `env:"SENTRY_CRONS_OUTBOX_PURGE"`
}

// Schedule holds the cadences the two Temporal Schedules are created with.
// They default to the original jobber cadences (daily / every 2 hours) but
// are configurable so they can be tightened for local testing.
type Schedule struct {
	DBBackupEvery        time.Duration `env:"DBBACKUP_SCHEDULE_EVERY" envDefault:"24h"`
	OutboxPurgeEvery     time.Duration `env:"OUTBOX_PURGE_SCHEDULE_EVERY" envDefault:"2h"`
	OutboxPurgeRetention time.Duration `env:"OUTBOX_PURGE_RETENTION" envDefault:"24h"`
}

// Load reads configuration from the environment, applying defaults where
// documented above and in the README, and returns an error describing
// every missing or invalid variable at once (via env.Parse's aggregate
// error), rather than failing on the first one encountered.
func Load() (Config, error) {
	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}

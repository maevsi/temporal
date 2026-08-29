package config

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setEnv sets environment variables for the duration of the test and
// restores the previous values afterwards, including unsetting variables
// that were not previously set.
func setEnv(t *testing.T, kv map[string]string) {
	t.Helper()
	for k, v := range kv {
		t.Setenv(k, v)
	}
}

func requiredEnv() map[string]string {
	return map[string]string{
		"POSTGRES_DATABASE":    "vibetype",
		"POSTGRES_USER":        "vibetype_role_service_temporal_worker",
		"POSTGRES_PASSWORD":    "hunter2",
		"S3_BUCKET":            "maevsi-backups",
		"S3_REGION":            "eu-central-1",
		"S3_ACCESS_KEY_ID":     "AKIAEXAMPLE",
		"S3_SECRET_ACCESS_KEY": "secret",
	}
}

func TestLoad_Defaults(t *testing.T) {
	setEnv(t, requiredEnv())

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "temporal-server:7233", cfg.Temporal.HostPort)
	assert.Equal(t, "default", cfg.Temporal.Namespace)
	assert.Equal(t, "maevsi-jobs", cfg.Temporal.TaskQueue)
	assert.Equal(t, ":9090", cfg.Metrics.Addr)
	assert.Equal(t, "/metrics", cfg.Metrics.Path)
	assert.Equal(t, 5432, cfg.Postgres.Port)
	assert.Equal(t, "require", cfg.Postgres.SSLMode)
	assert.Equal(t, "/backups", cfg.S3.SourceDir)
	assert.Equal(t, "backups", cfg.S3.Prefix)
	assert.Equal(t, 24*time.Hour, cfg.Schedule.DBBackupEvery)
	assert.Equal(t, 2*time.Hour, cfg.Schedule.OutboxPurgeEvery)
	assert.Equal(t, 24*time.Hour, cfg.Schedule.OutboxPurgeRetention)
	assert.Empty(t, cfg.Sentry.DBBackupCheckInURL)
	assert.Empty(t, cfg.Sentry.OutboxPurgeCheckInURL)
}

func TestLoad_Overrides(t *testing.T) {
	env := requiredEnv()
	env["TEMPORAL_HOST_PORT"] = "temporal.internal:7233"
	env["TEMPORAL_NAMESPACE"] = "maevsi-prod"
	env["OUTBOX_PURGE_SCHEDULE_EVERY"] = "10m"
	env["SENTRY_CRONS"] = "https://sentry.example/api/0/cron/dbbackup/token/"
	setEnv(t, env)

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "temporal.internal:7233", cfg.Temporal.HostPort)
	assert.Equal(t, "maevsi-prod", cfg.Temporal.Namespace)
	assert.Equal(t, 10*time.Minute, cfg.Schedule.OutboxPurgeEvery)
	assert.Equal(t, "https://sentry.example/api/0/cron/dbbackup/token/", cfg.Sentry.DBBackupCheckInURL)
}

func TestLoad_MissingRequired(t *testing.T) {
	// Intentionally leave everything unset.
	cfg, err := Load()
	require.Error(t, err)
	assert.Equal(t, Config{}, cfg)
	assert.ErrorContains(t, err, "POSTGRES_DATABASE")
	assert.ErrorContains(t, err, "S3_BUCKET")
}

func TestLoad_InvalidDuration(t *testing.T) {
	env := requiredEnv()
	env["DBBACKUP_SCHEDULE_EVERY"] = "not-a-duration"
	setEnv(t, env)

	_, err := Load()
	require.Error(t, err)
	// The underlying env library's parse-error messages name the Go
	// struct field ("DBBackupEvery"), not the env var
	// ("DBBACKUP_SCHEDULE_EVERY"); missing-variable errors (see
	// TestLoad_MissingRequired) still name the env var itself.
	assert.ErrorContains(t, err, "DBBackupEvery")
}

func TestPostgres_DSN(t *testing.T) {
	p := Postgres{
		Host:     "pg.internal",
		Port:     5432,
		Database: "vibetype",
		User:     "vibetype_role_service_temporal_worker",
		Password: "hunter2",
		SSLMode:  "require",
	}
	assert.Equal(t,
		"host='pg.internal' port=5432 dbname='vibetype' user='vibetype_role_service_temporal_worker' password='hunter2' sslmode='require'",
		p.DSN(),
	)
}

// TestPostgres_DSN_RoundTripsSpecialCharacters is the check that matters here: rendering the DSN is only correct if pgx reads back exactly the password that went in.
// Percent-encoding used to be applied instead of quoting, which pgx passes through verbatim in this connection string format, so every password containing a space, "@", "=", or "+" silently authenticated with the wrong value.
func TestPostgres_DSN_RoundTripsSpecialCharacters(t *testing.T) {
	passwords := []string{
		"plain123",
		"p@ss w0rd",
		"a=b",
		"sim+ple",
		"has'quote",
		`back\slash`,
		"100%sure",
	}

	for _, password := range passwords {
		t.Run(password, func(t *testing.T) {
			p := Postgres{
				Host:     "pg.internal",
				Port:     5432,
				Database: "vibetype",
				User:     "vibetype_role_service_temporal_worker",
				Password: password,
				SSLMode:  "require",
			}

			parsed, err := pgx.ParseConfig(p.DSN())
			require.NoError(t, err)
			assert.Equal(t, password, parsed.Password)
			assert.Equal(t, "pg.internal", parsed.Host)
			assert.Equal(t, "vibetype", parsed.Database)
			assert.Equal(t, "vibetype_role_service_temporal_worker", parsed.User)
		})
	}
}

// TestLoad_EmptyS3PrefixMeansBucketRoot covers "S3_PREFIX=" being set explicitly, which is how an operator asks for uploads at the bucket root.
// env applies an envDefault to a set-but-empty variable just as it does to an unset one, so the default has to live outside the struct tag for that distinction to survive Load.
func TestLoad_EmptyS3PrefixMeansBucketRoot(t *testing.T) {
	setEnv(t, requiredEnv())
	t.Setenv("S3_PREFIX", "")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Empty(t, cfg.S3.Prefix)
}

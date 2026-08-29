// Command worker runs the maevsi Temporal worker for scheduled
// DBBackup and OutboxPurge operations.
// It:
//
//  1. loads configuration from the environment (see internal/config),
//  2. connects to the self-hosted Temporal Server, Postgres, and S3,
//  3. starts a Prometheus metrics HTTP server fed by the Temporal SDK's
//     native metrics,
//  4. ensures both Temporal Schedules exist (idempotent bootstrap),
//  5. registers the DBBackup/OutboxPurge workflows and activities, and
//  6. runs the worker until it receives SIGINT/SIGTERM.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"

	"github.com/maevsi/temporal/internal/activities"
	"github.com/maevsi/temporal/internal/config"
	"github.com/maevsi/temporal/internal/metrics"
	"github.com/maevsi/temporal/internal/postgres"
	"github.com/maevsi/temporal/internal/s3client"
	"github.com/maevsi/temporal/internal/schedule"
	"github.com/maevsi/temporal/internal/sentrycrons"
	"github.com/maevsi/temporal/internal/workflows"
)

func main() {
	healthcheck := flag.Bool("healthcheck", false, "probe the running worker's /healthz endpoint and exit 0/1 instead of starting the worker")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if *healthcheck {
		// The production image ships FROM scratch (no shell, no curl/wget),
		// so a container HEALTHCHECK has to be an exec of this same binary
		// rather than a shell command.
		// See Dockerfile.
		os.Exit(runHealthcheck())
	}

	if err := run(logger); err != nil {
		logger.Error("worker exited with error", "error", err)
		os.Exit(1)
	}
}

// defaultMetricsAddr mirrors the METRICS_ADDR default in internal/config, for the case where the probe cannot read the configuration at all.
const defaultMetricsAddr = ":9090"

// runHealthcheck probes the running worker's health endpoint and returns the process exit code.
func runHealthcheck() int {
	return probeHealth(healthcheckAddr())
}

// healthcheckAddr resolves the address the probe should connect to.
// It prefers the configured METRICS_ADDR so a non-default port is respected, but falls back to the documented default rather than failing outright, because the container HEALTHCHECK exec may not have all of the worker's env vars available.
// A host-less listen address such as ":9090" means "every interface", which is not something a client can dial, so it is resolved to loopback.
func healthcheckAddr() string {
	addr := defaultMetricsAddr
	if cfg, err := config.Load(); err == nil {
		addr = cfg.Metrics.Addr
	}

	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}
	return addr
}

// probeHealth requests the worker's health endpoint at addr and returns the process exit code.
func probeHealth(addr string) int {
	httpClient := &http.Client{Timeout: 5 * time.Second}
	resp, err := httpClient.Get(fmt.Sprintf("http://%s%s", addr, metrics.HealthPath))
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck: request failed:", err)
		return 1
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "healthcheck: unexpected status %d\n", resp.StatusCode)
		return 1
	}
	return 0
}

func run(logger *slog.Logger) error {
	ctx := context.Background()

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	metricsHandler := metrics.New("temporal_worker")
	defer func() {
		if err := metricsHandler.Close(); err != nil {
			logger.Error("close metrics handler", "error", err)
		}
	}()

	metricsCtx, stopMetrics := context.WithCancel(ctx)
	defer stopMetrics()
	metricsErrCh := make(chan error, 1)
	go func() {
		logger.Info("starting metrics server", "addr", cfg.Metrics.Addr, "path", cfg.Metrics.Path)
		metricsErrCh <- metrics.Serve(metricsCtx, cfg.Metrics.Addr, cfg.Metrics.Path, metricsHandler)
	}()

	temporalClient, err := client.Dial(client.Options{
		HostPort:       cfg.Temporal.HostPort,
		Namespace:      cfg.Temporal.Namespace,
		MetricsHandler: metricsHandler.Temporal,
		Logger:         sdklog.NewStructuredLogger(logger),
	})
	if err != nil {
		return fmt.Errorf("connect to temporal at %q: %w", cfg.Temporal.HostPort, err)
	}
	defer temporalClient.Close()

	pgPool, err := postgres.NewPool(ctx, &cfg.Postgres)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer pgPool.Close()

	s3Client, err := s3client.New(ctx, &cfg.S3)
	if err != nil {
		return fmt.Errorf("build s3 client: %w", err)
	}

	sentryDBBackup, err := sentrycrons.New(cfg.Sentry.DBBackupCheckInURL)
	if err != nil {
		return fmt.Errorf("configure sentry crons for dbbackup (SENTRY_CRONS): %w", err)
	}
	sentryOutboxPurge, err := sentrycrons.New(cfg.Sentry.OutboxPurgeCheckInURL)
	if err != nil {
		return fmt.Errorf("configure sentry crons for outbox purge (SENTRY_CRONS_OUTBOX_PURGE): %w", err)
	}

	a := &activities.Activities{
		S3:        s3Client,
		Bucket:    cfg.S3.Bucket,
		Prefix:    cfg.S3.Prefix,
		SourceDir: cfg.S3.SourceDir,

		DB:               &postgres.Executor{Pool: pgPool},
		OutboxSchema:     "vibetype_private",
		OutboxTable:      "outbox",
		DefaultRetention: cfg.Schedule.OutboxPurgeRetention,

		SentryDBBackup:    sentryDBBackup,
		SentryOutboxPurge: sentryOutboxPurge,
	}

	if err := schedule.EnsureAll(ctx, temporalClient, &cfg); err != nil {
		return fmt.Errorf("ensure schedules: %w", err)
	}

	w := worker.New(temporalClient, cfg.Temporal.TaskQueue, worker.Options{})
	w.RegisterWorkflow(workflows.DBBackupWorkflow)
	w.RegisterWorkflow(workflows.OutboxPurgeWorkflow)
	w.RegisterActivity(a)

	logger.Info("starting worker",
		"temporalHostPort", cfg.Temporal.HostPort,
		"namespace", cfg.Temporal.Namespace,
		"taskQueue", cfg.Temporal.TaskQueue,
	)

	runErrCh := make(chan error, 1)
	go func() { runErrCh <- w.Run(worker.InterruptCh()) }()

	select {
	case err := <-runErrCh:
		if err != nil {
			return fmt.Errorf("worker run: %w", err)
		}
		return nil
	case err := <-metricsErrCh:
		if err != nil {
			return fmt.Errorf("metrics server: %w", err)
		}
		return errors.New("metrics server stopped unexpectedly")
	}
}

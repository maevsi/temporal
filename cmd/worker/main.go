// Command worker runs the maevsi Temporal worker that replaces the
// jobber-based DBBackup and OutboxPurge cron jobs.
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

	"github.com/maevsi/temporal-worker-go/internal/activities"
	"github.com/maevsi/temporal-worker-go/internal/config"
	"github.com/maevsi/temporal-worker-go/internal/metrics"
	"github.com/maevsi/temporal-worker-go/internal/postgres"
	"github.com/maevsi/temporal-worker-go/internal/s3client"
	"github.com/maevsi/temporal-worker-go/internal/schedule"
	"github.com/maevsi/temporal-worker-go/internal/sentrycrons"
	"github.com/maevsi/temporal-worker-go/internal/workflows"
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

// runHealthcheck loads the same config the running worker process would
// have loaded and checks that its metrics HTTP server is responding.
func runHealthcheck() int {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck: load config:", err)
		return 1
	}

	addr := cfg.Metrics.Addr
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}

	httpClient := &http.Client{Timeout: 5 * time.Second}
	resp, err := httpClient.Get(fmt.Sprintf("http://%s/healthz", addr))
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
	defer metricsHandler.Close()

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

	pgPool, err := postgres.NewPool(ctx, cfg.Postgres)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer pgPool.Close()

	s3Client, err := s3client.New(ctx, cfg.S3)
	if err != nil {
		return fmt.Errorf("build s3 client: %w", err)
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

		SentryDBBackup:    sentrycrons.New(cfg.Sentry.DBBackupCheckInURL),
		SentryOutboxPurge: sentrycrons.New(cfg.Sentry.OutboxPurgeCheckInURL),
	}

	if err := schedule.EnsureAll(ctx, temporalClient, cfg); err != nil {
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

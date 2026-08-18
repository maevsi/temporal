// Package metrics wires the Temporal Go SDK's native metrics (worker pollers, activity/workflow execution counts and latencies, task queue backlog, etc.) to a Prometheus HTTP handler, so a Prometheus scrape target can be pointed at this process without any custom instrumentation.
// Configuring the actual Prometheus scrape job / Grafana dashboards is stack-repo work happening separately; this package only needs to make the metrics available.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/uber-go/tally/v4"
	promreporter "github.com/uber-go/tally/v4/prometheus"
	"go.temporal.io/sdk/client"
	sdktally "go.temporal.io/sdk/contrib/tally"
)

// reportingInterval controls how often tally flushes buffered metrics into the underlying Prometheus registry.
// It only affects timer/histogram bucketing latency, not scrape freshness (Prometheus scrapes read the registry directly).
const reportingInterval = time.Second

// Handler bundles the Temporal client.MetricsHandler to pass into
// client.Options and the http.Handler to serve on the metrics endpoint.
type Handler struct {
	Temporal client.MetricsHandler
	HTTP     http.Handler

	closer io.Closer
}

// Close stops the underlying tally scope's background reporting loop.
func (h *Handler) Close() error {
	if h.closer == nil {
		return nil
	}
	return h.closer.Close()
}

// New builds a Prometheus-backed metrics.Handler.
// namePrefix is applied to every emitted metric (e.g. "temporal_worker") to namespace this process's metrics from anything else sharing the same Prometheus instance.
func New(namePrefix string) *Handler {
	// Use a dedicated registry rather than prometheus.DefaultRegisterer:
	// New may run more than once in a process (e.g. once per test), and
	// the default registerer panics on a second registration of the same
	// collector.
	registry := prom.NewRegistry()
	reporter := promreporter.NewReporter(promreporter.Options{
		Registerer: registry,
		Gatherer:   registry,
	})
	scope, closer := tally.NewRootScope(tally.ScopeOptions{
		Prefix:          namePrefix,
		CachedReporter:  reporter,
		Separator:       promreporter.DefaultSeparator,
		SanitizeOptions: &sdktally.PrometheusSanitizeOptions,
	}, reportingInterval)

	return &Handler{
		Temporal: sdktally.NewMetricsHandler(sdktally.NewPrometheusNamingScope(scope)),
		HTTP:     reporter.HTTPHandler(),
		closer:   closer,
	}
}

// Serve starts an HTTP server exposing the metrics handler at path, plus a /healthz endpoint that just confirms this HTTP server itself is up.
// It blocks until ctx is canceled, then shuts the server down gracefully.
//
// /healthz is intentionally trivial (no Temporal/Postgres/S3 reachability checks): it exists so a container HEALTHCHECK can probe over HTTP even though the production image ships FROM scratch with no shell or curl, via the worker binary's own "-healthcheck" flag (see cmd/worker).
func Serve(ctx context.Context, addr, path string, h *Handler) error {
	mux := http.NewServeMux()
	mux.Handle(path, h.HTTP)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Addr: addr, Handler: mux}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("metrics: serve: %w", err)
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		_ = srv.Close()
		<-errCh
		return nil
	case err := <-errCh:
		return err
	}
}

package metrics

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_ExposesTemporalHandlerAndHTTPHandler(t *testing.T) {
	h := New("temporal_worker_test")
	defer func() { _ = h.Close() }()

	require.NotNil(t, h.Temporal)
	require.NotNil(t, h.HTTP)

	// Emit a metric through the Temporal-facing handler and confirm it
	// shows up on the Prometheus HTTP handler, proving the two are wired
	// to the same underlying registry.
	h.Temporal.WithTags(map[string]string{}).Counter("smoke_test_total").Inc(1)

	// tally's reporting loop flushes on its own interval; give it a moment.
	time.Sleep(reportingInterval + 200*time.Millisecond)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody)
	h.HTTP.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	body, err := io.ReadAll(rec.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "smoke_test_total")
}

func TestServe_ShutsDownOnContextCancel(t *testing.T) {
	h := New("temporal_worker_test_serve")
	defer func() { _ = h.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, "127.0.0.1:0", "/metrics", h)
	}()

	// Serve binds an ephemeral port here only to exercise the shutdown
	// path; a fixed test port would risk collisions across CI runs.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not shut down after context cancellation")
	}
}

func TestServe_RejectsInvalidAddr(t *testing.T) {
	h := New("temporal_worker_test_bad_addr")
	defer func() { _ = h.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := Serve(ctx, "not-a-valid-addr", "/metrics", h)
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "metrics: serve"))
}

// TestServe_RejectsHealthPathCollision covers METRICS_PATH being pointed at the health endpoint.
// http.ServeMux panics on a duplicate pattern, and Serve runs on its own goroutine in the worker, so that panic would take the process down instead of surfacing as a startup error.
func TestServe_RejectsHealthPathCollision(t *testing.T) {
	h := New("temporal_worker_test_collision")
	defer func() { _ = h.Close() }()

	err := Serve(t.Context(), "127.0.0.1:0", HealthPath, h)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "collides with the health endpoint")
}

// TestServe_RejectsPathWithoutLeadingSlash covers a METRICS_PATH that http.ServeMux reads as a host rather than a path.
// Like the HealthPath collision, the resulting panic would happen on the goroutine Serve runs on in the worker and take the process down, so it has to be caught before mux.Handle sees it.
func TestServe_RejectsPathWithoutLeadingSlash(t *testing.T) {
	h := New("temporal_worker_test_no_slash")
	defer func() { _ = h.Close() }()

	for _, path := range []string{"metrics", ""} {
		err := Serve(t.Context(), "127.0.0.1:0", path, h)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must start with a slash")
	}
}

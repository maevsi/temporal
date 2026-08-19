package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRunHealthcheck_ReturnsZeroOnHealthy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	// Override METRICS_ADDR so config.Load succeeds partially.
	t.Setenv("METRICS_ADDR", srv.Listener.Addr().String())

	// Set all required env vars so config.Load doesn't fail.
	t.Setenv("POSTGRES_DATABASE", "test")
	t.Setenv("POSTGRES_USER", "test")
	t.Setenv("POSTGRES_PASSWORD", "test")
	t.Setenv("S3_BUCKET", "test")
	t.Setenv("S3_REGION", "us-east-1")
	t.Setenv("S3_ACCESS_KEY_ID", "test")
	t.Setenv("S3_SECRET_ACCESS_KEY", "test")

	code := runHealthcheck()
	assert.Equal(t, 0, code, "healthcheck should return 0 when /healthz responds 200")
}

func TestRunHealthcheck_ReturnsOneOnUnhealthy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	t.Setenv("METRICS_ADDR", srv.Listener.Addr().String())

	// Set all required env vars so config.Load doesn't fail.
	t.Setenv("POSTGRES_DATABASE", "test")
	t.Setenv("POSTGRES_USER", "test")
	t.Setenv("POSTGRES_PASSWORD", "test")
	t.Setenv("S3_BUCKET", "test")
	t.Setenv("S3_REGION", "us-east-1")
	t.Setenv("S3_ACCESS_KEY_ID", "test")
	t.Setenv("S3_SECRET_ACCESS_KEY", "test")

	code := runHealthcheck()
	assert.Equal(t, 1, code, "healthcheck should return 1 when /healthz responds non-200")
}

func TestRunHealthcheck_FallsBackToDefaultAddrOnConfigError(t *testing.T) {
	// Don't set required env vars so config.Load fails.
	// This verifies the fallback behavior: healthcheck uses :9090 and fails
	// gracefully instead of crashing on missing config.
	os.Unsetenv("POSTGRES_DATABASE")
	os.Unsetenv("POSTGRES_USER")
	os.Unsetenv("POSTGRES_PASSWORD")
	os.Unsetenv("S3_BUCKET")
	os.Unsetenv("S3_REGION")
	os.Unsetenv("S3_ACCESS_KEY_ID")
	os.Unsetenv("S3_SECRET_ACCESS_KEY")

	code := runHealthcheck()
	// Should return 1 because nothing is listening on 127.0.0.1:9090,
	// not because config.Load failed.
	assert.Equal(t, 1, code, "healthcheck should return 1 when no server is running, not crash on missing config")
}

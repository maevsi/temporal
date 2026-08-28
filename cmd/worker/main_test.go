package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// requiredEnv lists the variables config.Load refuses to start without.
// Tests set them via t.Setenv so the values are restored afterwards; setting one to the empty string still trips the "notEmpty" tag and makes Load fail, which is how the config-error path is exercised without leaking an unset variable into later tests.
var requiredEnv = map[string]string{
	"POSTGRES_DATABASE":    "test",
	"POSTGRES_USER":        "test",
	"POSTGRES_PASSWORD":    "test",
	"S3_BUCKET":            "test",
	"S3_REGION":            "us-east-1",
	"S3_ACCESS_KEY_ID":     "test",
	"S3_SECRET_ACCESS_KEY": "test",
}

func setRequiredEnv(t *testing.T, value func(string) string) {
	t.Helper()
	for k, v := range requiredEnv {
		t.Setenv(k, value(v))
	}
}

func valid(v string) string { return v }
func blank(string) string   { return "" }

func TestRunHealthcheck_ReturnsZeroOnHealthy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	setRequiredEnv(t, valid)
	t.Setenv("METRICS_ADDR", srv.Listener.Addr().String())

	assert.Equal(t, 0, runHealthcheck(), "healthcheck should return 0 when /healthz responds 200")
}

func TestRunHealthcheck_ReturnsOneOnUnhealthy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	setRequiredEnv(t, valid)
	t.Setenv("METRICS_ADDR", srv.Listener.Addr().String())

	assert.Equal(t, 1, runHealthcheck(), "healthcheck should return 1 when /healthz responds non-200")
}

// TestRunHealthcheck_FallsBackToDefaultAddrOnConfigError covers the container HEALTHCHECK exec, which does not necessarily inherit the worker's environment.
// A missing config must not stop the probe from reaching the worker, so this serves /healthz on the default address and asserts the probe still finds it.
func TestRunHealthcheck_FallsBackToDefaultAddrOnConfigError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:9090")
	if err != nil {
		t.Skipf("default metrics port is already in use on this machine: %v", err)
	}

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
		}
	}))
	require.NoError(t, srv.Listener.Close())
	srv.Listener = listener
	srv.Start()
	defer srv.Close()

	// Blank out the required variables so config.Load fails and METRICS_ADDR is never consulted.
	setRequiredEnv(t, blank)
	t.Setenv("METRICS_ADDR", "")

	assert.Equal(t, 0, runHealthcheck(), "healthcheck should fall back to the default address instead of failing on missing config")
}

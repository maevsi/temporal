package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
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

func TestHealthcheckAddr_UsesConfiguredAddr(t *testing.T) {
	setRequiredEnv(t, valid)
	t.Setenv("METRICS_ADDR", "10.0.0.5:9999")

	assert.Equal(t, "10.0.0.5:9999", healthcheckAddr())
}

// TestHealthcheckAddr_ResolvesHostlessAddrToLoopback covers the default listen address form, which names a port but no host and so cannot be dialed as-is.
func TestHealthcheckAddr_ResolvesHostlessAddrToLoopback(t *testing.T) {
	setRequiredEnv(t, valid)
	t.Setenv("METRICS_ADDR", ":9191")

	assert.Equal(t, "127.0.0.1:9191", healthcheckAddr())
}

// TestHealthcheckAddr_FallsBackOnConfigError covers the container HEALTHCHECK exec, which does not necessarily inherit the worker's environment.
// A missing config must not stop the probe from reaching a worker listening on the default address.
func TestHealthcheckAddr_FallsBackOnConfigError(t *testing.T) {
	setRequiredEnv(t, blank)

	assert.Equal(t, "127.0.0.1:9090", healthcheckAddr())
}

// TestHealthcheckAddr_FallsBackOnEmptyAddr covers METRICS_ADDR being present but blank.
// The fallback comes from the envDefault on Metrics.Addr, which env applies to a set-but-empty variable just as it does to an unset one, so Load can never hand back the blank address that would produce the unusable URL "http:///healthz".
func TestHealthcheckAddr_FallsBackOnEmptyAddr(t *testing.T) {
	setRequiredEnv(t, valid)
	t.Setenv("METRICS_ADDR", "")

	assert.Equal(t, "127.0.0.1:9090", healthcheckAddr())
}

func TestProbeHealth_ReturnsZeroOnHealthy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	assert.Equal(t, 0, probeHealth(srv.Listener.Addr().String()), "healthcheck should return 0 when the health endpoint responds 200")
}

func TestProbeHealth_ReturnsOneOnUnhealthy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	assert.Equal(t, 1, probeHealth(srv.Listener.Addr().String()), "healthcheck should return 1 when the health endpoint responds non-200")
}

// TestProbeHealth_ReturnsOneWhenUnreachable covers the case the container HEALTHCHECK actually exists for: the worker is not serving at all.
func TestProbeHealth_ReturnsOneWhenUnreachable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	addr := srv.Listener.Addr().String()
	srv.Close()

	assert.Equal(t, 1, probeHealth(addr), "healthcheck should return 1 when nothing is listening")
}

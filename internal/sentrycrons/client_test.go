package sentrycrons

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckIn_SendsStatus(t *testing.T) {
	var gotStatus string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotStatus = r.URL.Query().Get("status")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := New(srv.URL)
	err := c.CheckIn(context.Background(), StatusInProgress)
	require.NoError(t, err)
	assert.Equal(t, "in_progress", gotStatus)
}

func TestCheckIn_PreservesExistingQuery(t *testing.T) {
	var gotQuery url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := New(srv.URL + "?env=production")
	err := c.CheckIn(context.Background(), StatusOK)
	require.NoError(t, err)
	assert.Equal(t, "production", gotQuery.Get("env"))
	assert.Equal(t, "ok", gotQuery.Get("status"))
}

func TestCheckIn_NonOKStatusIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := New(srv.URL)
	err := c.CheckIn(context.Background(), StatusError)
	require.Error(t, err)
}

func TestCheckIn_UnconfiguredIsNoOp(t *testing.T) {
	var c *Client
	c = New("")
	err := c.CheckIn(context.Background(), StatusOK)
	require.NoError(t, err)
	assert.False(t, c.Configured())
}

func TestCheckIn_NilClientIsNoOp(t *testing.T) {
	var c *Client
	err := c.CheckIn(context.Background(), StatusOK)
	require.NoError(t, err)
}

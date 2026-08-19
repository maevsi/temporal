// Package sentrycrons implements the check-in protocol for Sentry Crons
// "ping" style monitors: a plain HTTP GET to a per-monitor URL with a
// status query parameter, the same mechanism the original jobber-based
// shell script sinks used via curl.
package sentrycrons

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// Status is the check-in status reported to Sentry Crons.
type Status string

const (
	// StatusInProgress marks the start of a monitored run.
	StatusInProgress Status = "in_progress"
	// StatusOK marks a monitored run as having completed successfully.
	StatusOK Status = "ok"
	// StatusError marks a monitored run as having failed.
	StatusError Status = "error"
)

// Client sends check-ins for a single Sentry Crons monitor.
type Client struct {
	// parsedURL is the pre-parsed check-in URL with existing query
	// parameters preserved, minus "status" which is set per-call.
	parsedURL  *url.URL
	httpClient *http.Client
}

// New returns a Client for the given check-in URL.
// An empty URL is valid and produces a Client whose CheckIn calls are no-ops; this matches environments (e.g. local development) that historically relied on email notifications instead of Sentry.
func New(checkInURL string) *Client {
	if checkInURL == "" {
		return &Client{httpClient: &http.Client{Timeout: 10 * time.Second}}
	}

	u, err := url.Parse(checkInURL)
	if err != nil {
		// Store nil parsedURL; CheckIn will fall back to raw string.
		return &Client{httpClient: &http.Client{Timeout: 10 * time.Second}}
	}

	q := u.Query()
	q.Set("status", "")
	u.RawQuery = q.Encode()

	return &Client{
		parsedURL:  u,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// Configured reports whether this Client has a check-in URL and will
// actually perform requests.
func (c *Client) Configured() bool {
	return c != nil && c.parsedURL != nil
}

// CheckIn reports the given status to Sentry Crons.
// It is a no-op (nil error) if the client has no check-in URL configured, so callers do not need to branch on configuration before calling it.
func (c *Client) CheckIn(ctx context.Context, status Status) error {
	if !c.Configured() {
		return nil
	}

	u := *c.parsedURL
	q := u.Query()
	q.Set("status", string(status))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return fmt.Errorf("sentrycrons: build check-in request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("sentrycrons: check-in request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("sentrycrons: check-in returned status %d", resp.StatusCode)
	}
	return nil
}

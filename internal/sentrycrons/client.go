// Package sentrycrons implements the check-in protocol for Sentry Crons
// "ping" style monitors: a plain HTTP GET to a per-monitor URL with a
// status query parameter.
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
	// parsedURL is the pre-parsed check-in URL.
	// Existing query parameters are preserved; "status" is added per call in CheckIn.
	parsedURL  *url.URL
	httpClient *http.Client
}

// New returns a Client for the given check-in URL.
// An empty URL is valid and produces a Client whose CheckIn calls are no-ops; this matches environments (e.g. local development) that historically relied on email notifications instead of Sentry.
//
// A non-empty URL that cannot be used is an error rather than a silently unconfigured Client.
// Monitoring that quietly disables itself on a typo is worse than none at all: the workflows treat check-in failures as best-effort and only log them, so nothing downstream would ever report the mistake and the monitor would simply never fire.
func New(checkInURL string) (*Client, error) {
	if checkInURL == "" {
		return &Client{httpClient: &http.Client{Timeout: 10 * time.Second}}, nil
	}

	u, err := url.Parse(checkInURL)
	if err != nil {
		return nil, fmt.Errorf("sentrycrons: parse check-in URL: %w", err)
	}
	// url.Parse accepts almost anything, so the scheme has to be checked separately.
	// Without it a value like "sentry.io/api/0/cron/..." parses fine and only fails much later, inside a check-in that nobody sees fail.
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("sentrycrons: check-in URL must be http or https, got %q", checkInURL)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("sentrycrons: check-in URL has no host: %q", checkInURL)
	}

	return &Client{
		parsedURL:  u,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}, nil
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

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), http.NoBody)
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

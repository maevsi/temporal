package activities

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/activity"

	"github.com/maevsi/temporal-worker-go/internal/sentrycrons"
)

// SentryCheckIn sends a single Sentry Crons check-in for the given job and status.
// It is called from both workflows at start (in_progress) and at completion (ok / error), the same three-call pattern the original shell-script sinks used (SENTRY_CRONS / SENTRY_CRONS_OUTBOX_PURGE).
//
// It is intentionally a thin activity: workflows treat check-in failures
// as best-effort (logged, not propagated) so a Sentry outage never fails
// the underlying backup or purge.
func (a *Activities) SentryCheckIn(ctx context.Context, input SentryCheckInInput) error {
	logger := activity.GetLogger(ctx)

	client, err := a.sentryClientFor(input.Job)
	if err != nil {
		return err
	}

	if err := client.CheckIn(ctx, input.Status); err != nil {
		return fmt.Errorf("sentry crons check-in for job %q: %w", input.Job, err)
	}
	logger.Debug("sentry crons check-in sent", "job", input.Job, "status", input.Status)
	return nil
}

// sentryClientFor maps a Job to its configured Sentry Crons client.
// Keeping the mapping in one place means adding a third migrated job only requires a new case here plus a new field on Activities.
func (a *Activities) sentryClientFor(job Job) (*sentrycrons.Client, error) {
	switch job {
	case JobDBBackup:
		return a.SentryDBBackup, nil
	case JobOutboxPurge:
		return a.SentryOutboxPurge, nil
	default:
		return nil, fmt.Errorf("sentrycheckin: unknown job %q", job)
	}
}

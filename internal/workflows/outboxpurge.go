package workflows

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/maevsi/temporal/internal/activities"
	"github.com/maevsi/temporal/internal/sentrycrons"
)

const (
	outboxPurgeActivityTimeout = 2 * time.Minute
	outboxPurgeMaxAttempts     = 5
)

// OutboxPurgeRunTimeout bounds a single OutboxPurgeWorkflow run, derived from the activity's retry budget for the same reason as DBBackupRunTimeout.
const OutboxPurgeRunTimeout = outboxPurgeActivityTimeout*outboxPurgeMaxAttempts + 5*time.Minute

// OutboxPurgeWorkflow orchestrates the OutboxPurge activity:
//
//	DELETE FROM vibetype_private.outbox WHERE created_at < now() - interval '24 hours'
//
// It executes the DELETE directly over a dedicated pgx connection pool.
// The activity itself is short (a single DELETE), so timeouts here are tight; ConfigError failures are marked non-retryable since retrying a misconfigured schema/table name can't help.
func OutboxPurgeWorkflow(ctx workflow.Context) (activities.OutboxPurgeResult, error) {
	checkIn(ctx, activities.JobOutboxPurge, sentrycrons.StatusInProgress)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: outboxPurgeActivityTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        5 * time.Second,
			BackoffCoefficient:     2.0,
			MaximumInterval:        time.Minute,
			MaximumAttempts:        outboxPurgeMaxAttempts,
			NonRetryableErrorTypes: []string{activities.ErrTypeConfig},
		},
	})

	var a *activities.Activities
	var result activities.OutboxPurgeResult
	purgeErr := workflow.ExecuteActivity(ctx, a.OutboxPurge, activities.OutboxPurgeInput{}).Get(ctx, &result)

	status := sentrycrons.StatusOK
	if purgeErr != nil {
		status = sentrycrons.StatusError
	}
	checkInFinal(ctx, activities.JobOutboxPurge, status)

	return result, purgeErr
}

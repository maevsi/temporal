package workflows

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/maevsi/temporal-worker-go/internal/activities"
	"github.com/maevsi/temporal-worker-go/internal/sentrycrons"
)

// OutboxPurgeWorkflow orchestrates the OutboxPurge activity, replacing the
// jobber job that ran
//
//	DELETE FROM vibetype_private.outbox WHERE created_at < now() - interval '24 hours'
//
// every two hours. Unlike the jobber job (which was blocked because that
// image had no psql client), this executes the DELETE directly over a
// dedicated pgx connection pool.
//
// The activity itself is short (a single DELETE), so timeouts here are
// tight; ConfigError failures are marked non-retryable since retrying a
// misconfigured schema/table name can't help.
func OutboxPurgeWorkflow(ctx workflow.Context) (activities.OutboxPurgeResult, error) {
	checkIn(ctx, activities.JobOutboxPurge, sentrycrons.StatusInProgress)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        5 * time.Second,
			BackoffCoefficient:     2.0,
			MaximumInterval:        time.Minute,
			MaximumAttempts:        5,
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
	checkIn(ctx, activities.JobOutboxPurge, status)

	return result, purgeErr
}

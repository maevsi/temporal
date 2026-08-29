// Package workflows implements the two Temporal Workflows for DBBackup and OutboxPurge.
// Workflows only orchestrate: all real work (S3 upload, Postgres DELETE, Sentry HTTP calls) lives in internal/activities, reached here purely through method-expression references (see the package doc comment on activities.Activities) so this package never links against the AWS/pgx/HTTP dependencies those activities use.
package workflows

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/maevsi/temporal/internal/activities"
	"github.com/maevsi/temporal/internal/sentrycrons"
)

// checkInActivityOptions are deliberately short and cheap to retry: a Sentry Crons check-in should never be the reason a workflow run takes long or ties up worker capacity.
func checkInActivityOptions(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    10 * time.Second,
			MaximumAttempts:    3,
		},
	})
}

// checkInFinal sends the terminal check-in for a run on a disconnected context so a cancelled run still reports its outcome.
// On the workflow's own context ExecuteActivity short-circuits with a CanceledError without ever scheduling the activity, which would leave the Sentry monitor stuck in StatusInProgress until its own max-runtime alert fired.
func checkInFinal(ctx workflow.Context, job activities.Job, status sentrycrons.Status) {
	disconnected, cancel := workflow.NewDisconnectedContext(ctx)
	defer cancel()
	checkIn(disconnected, job, status)
}

// checkIn sends a Sentry Crons check-in and treats failure as best-effort: alerting must never fail the underlying backup or purge, so the error is logged rather than returned.
func checkIn(ctx workflow.Context, job activities.Job, status sentrycrons.Status) {
	var a *activities.Activities
	err := workflow.ExecuteActivity(checkInActivityOptions(ctx), a.SentryCheckIn, activities.SentryCheckInInput{
		Job:    job,
		Status: status,
	}).Get(ctx, nil)
	if err != nil {
		workflow.GetLogger(ctx).Warn("sentry crons check-in failed", "job", job, "status", status, "error", err)
	}
}

const (
	dbBackupActivityTimeout = 30 * time.Minute
	dbBackupMaxAttempts     = 3
)

// DBBackupRunTimeout bounds a single DBBackupWorkflow run.
// It is derived from the activity's own retry budget (every attempt at its full StartToCloseTimeout, plus headroom for the backoff between attempts) rather than from the schedule cadence: a run timeout tied to the interval would silently truncate runs as soon as the cadence is tightened, which the Schedule config is explicitly meant to allow.
const DBBackupRunTimeout = dbBackupActivityTimeout*dbBackupMaxAttempts + 10*time.Minute

// DBBackupWorkflow orchestrates the DBBackup activity.
// A single sync of a bucket's worth of database backups can legitimately take a while, so the activity gets a generous StartToCloseTimeout and a HeartbeatTimeout so a dead worker is detected well before that timeout expires; ConfigError failures (bad bucket, missing source dir) are marked non-retryable since retrying them can't help.
// The HeartbeatTimeout has to clear the longest gap the activity can leave between two heartbeats, which on the multipart upload path is one completed 8 MiB part: two minutes leaves room for a slow link without letting a dead worker go unnoticed for long.
func DBBackupWorkflow(ctx workflow.Context) (activities.DBBackupResult, error) {
	checkIn(ctx, activities.JobDBBackup, sentrycrons.StatusInProgress)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: dbBackupActivityTimeout,
		HeartbeatTimeout:    2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        30 * time.Second,
			BackoffCoefficient:     2.0,
			MaximumInterval:        5 * time.Minute,
			MaximumAttempts:        dbBackupMaxAttempts,
			NonRetryableErrorTypes: []string{activities.ErrTypeConfig},
		},
	})

	var a *activities.Activities
	var result activities.DBBackupResult
	backupErr := workflow.ExecuteActivity(ctx, a.DBBackup, activities.DBBackupInput{}).Get(ctx, &result)

	status := sentrycrons.StatusOK
	if backupErr != nil {
		status = sentrycrons.StatusError
	}
	checkInFinal(ctx, activities.JobDBBackup, status)

	return result, backupErr
}

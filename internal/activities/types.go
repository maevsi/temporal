package activities

import (
	"time"

	"github.com/maevsi/temporal/internal/sentrycrons"
)

// Job identifies which workflow a Sentry Crons check-in belongs to.
type Job string

const (
	// JobDBBackup is the DBBackup workflow.
	JobDBBackup Job = "dbbackup"
	// JobOutboxPurge is the OutboxPurge workflow.
	JobOutboxPurge Job = "outbox_purge"
)

// ErrTypeConfig is the ApplicationError type used for configuration problems (bad bucket name, unreachable source directory, ...) that will never succeed on retry.
// Pair with temporal.RetryPolicy.NonRetryableErrorTypes.
const ErrTypeConfig = "ConfigError"

// DBBackupInput is the input to the DBBackup activity.
// Empty today; kept as a named struct so new parameters (e.g. an explicit source override) can be added without breaking the activity's serialized signature.
type DBBackupInput struct{}

// DBBackupResult summarizes the outcome of a DBBackup activity run.
type DBBackupResult struct {
	FilesUploaded int
	FilesSkipped  int
	BytesUploaded int64
	Duration      time.Duration
}

// OutboxPurgeInput is the input to the OutboxPurge activity.
type OutboxPurgeInput struct {
	// Retention overrides the configured retention period for this run.
	// Zero means "use the Activities' configured default".
	Retention time.Duration
}

// OutboxPurgeResult summarizes the outcome of an OutboxPurge activity run.
type OutboxPurgeResult struct {
	RowsDeleted int64
}

// SentryCheckInInput is the input to the SentryCheckIn activity.
type SentryCheckInInput struct {
	Job    Job
	Status sentrycrons.Status
}

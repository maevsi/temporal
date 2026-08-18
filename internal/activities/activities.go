// Package activities implements the Temporal Activities backing the two migrated jobber jobs (DBBackup, OutboxPurge) plus a small SentryCheckIn activity shared by both workflows.
//
// Activities are methods on Activities so their real dependencies (an S3 client, a Postgres executor, Sentry Crons clients) are injected once at worker startup instead of being read from globals.
// Workflows never import this package's dependencies directly; they reference activities by the ActivityName* constants and the Input/Result types in types.go (see internal/workflows).
package activities

import (
	"time"

	"github.com/maevsi/temporal-worker-go/internal/sentrycrons"
)

// Activities bundles the dependencies needed by every activity in this package.
// A zero-value *Activities is also used, untyped, purely as a method-expression source when workflows register activity references (see internal/workflows), and that usage never calls through the pointer.
type Activities struct {
	// S3 / DBBackup dependencies.
	S3        S3API
	Bucket    string
	Prefix    string
	SourceDir string

	// Postgres / OutboxPurge dependencies.
	DB               DBExecutor
	OutboxSchema     string
	OutboxTable      string
	DefaultRetention time.Duration

	// Sentry Crons dependencies, one client per job so each keeps its own
	// monitor slug (mirrors SENTRY_CRONS / SENTRY_CRONS_OUTBOX_PURGE).
	SentryDBBackup    *sentrycrons.Client
	SentryOutboxPurge *sentrycrons.Client
}

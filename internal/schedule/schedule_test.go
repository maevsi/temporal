package schedule

import (
	"fmt"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maevsi/temporal/internal/config"
)

func testConfig() config.Config {
	return config.Config{
		Temporal: config.Temporal{TaskQueue: "maevsi-jobs"},
		Schedule: config.Schedule{
			DBBackupEvery:    24 * time.Hour,
			OutboxPurgeEvery: 2 * time.Hour,
		},
	}
}

func TestDBBackupSchedule_UsesIntervalNotCron(t *testing.T) {
	cfg := testConfig()
	opts := dbBackupSchedule(&cfg)

	assert.Equal(t, IDDBBackup, opts.ID)
	assert.Empty(t, opts.Spec.CronExpressions, "schedule must use native Intervals, not cron strings")
	require.Len(t, opts.Spec.Intervals, 1)
	assert.Equal(t, 24*time.Hour, opts.Spec.Intervals[0].Every)
	assert.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, opts.Overlap)

	action, ok := opts.Action.(*client.ScheduleWorkflowAction)
	require.True(t, ok)
	assert.Equal(t, "maevsi-jobs", action.TaskQueue)
}

func TestOutboxPurgeSchedule_UsesIntervalNotCron(t *testing.T) {
	cfg := testConfig()
	opts := outboxPurgeSchedule(&cfg)

	assert.Equal(t, IDOutboxPurge, opts.ID)
	assert.Empty(t, opts.Spec.CronExpressions)
	require.Len(t, opts.Spec.Intervals, 1)
	assert.Equal(t, 2*time.Hour, opts.Spec.Intervals[0].Every)
	assert.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, opts.Overlap)
}

// TestIsAlreadyExists covers the concurrent-bootstrap path in ensure: two replicas starting together race between Describe and Create, and the loser must treat the conflict as success rather than failing startup.
// This previously used grpc's status.Code, which reports codes.Unknown for every serviceerror the SDK returns, so the guard never matched.
func TestIsAlreadyExists(t *testing.T) {
	t.Run("matches the SDK's conflict errors", func(t *testing.T) {
		assert.True(t, isAlreadyExists(serviceerror.NewAlreadyExists("schedule already exists")))
		assert.True(t, isAlreadyExists(serviceerror.NewWorkflowExecutionAlreadyStarted("already started", "id", "run-id")))
	})

	t.Run("sees through wrapping", func(t *testing.T) {
		wrapped := fmt.Errorf("schedule: create %q: %w", IDDBBackup, serviceerror.NewAlreadyExists("dup"))
		assert.True(t, isAlreadyExists(wrapped))
	})

	t.Run("does not match unrelated errors", func(t *testing.T) {
		assert.False(t, isAlreadyExists(serviceerror.NewNotFound("no such schedule")))
		assert.False(t, isAlreadyExists(serviceerror.NewUnavailable("frontend down")))
		assert.False(t, isAlreadyExists(fmt.Errorf("plain error")))
	})
}

// TestIsNotFound guards the other branch of ensure: anything other than a NotFound from Describe must surface as a startup error rather than being mistaken for "schedule missing, create it".
func TestIsNotFound(t *testing.T) {
	assert.True(t, isNotFound(serviceerror.NewNotFound("no such schedule")))
	assert.True(t, isNotFound(fmt.Errorf("wrapped: %w", serviceerror.NewNotFound("no such schedule"))))
	assert.False(t, isNotFound(serviceerror.NewUnavailable("frontend down")))
	assert.False(t, isNotFound(serviceerror.NewAlreadyExists("dup")))
}

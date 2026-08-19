package schedule

import (
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maevsi/temporal-worker-go/internal/config"
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

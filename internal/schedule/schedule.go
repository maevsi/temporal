// Package schedule creates the Temporal Schedules that replace jobber's
// crontab entirely: cadences live in Temporal's native Schedule API
// (interval-based, not cron strings baked into workflow code) so they are
// visible and editable via `temporal schedule` / the Temporal UI without a
// worker code change.
package schedule

import (
	"context"
	"errors"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/maevsi/temporal-worker-go/internal/config"
	"github.com/maevsi/temporal-worker-go/internal/workflows"
)

// Schedule IDs are stable business identifiers; changing them creates new
// Schedules rather than updating existing ones on the next deploy.
const (
	IDDBBackup    = "dbbackup-daily"
	IDOutboxPurge = "outbox-purge-every-2h"
)

// EnsureAll idempotently creates both Schedules if they don't already exist.
// It never modifies an existing Schedule (e.g. one paused or re-tuned by an operator via the Temporal CLI/UI), matching the bootstrap-once semantics appropriate for a worker that starts on every deploy.
func EnsureAll(ctx context.Context, c client.Client, cfg *config.Config) error {
	specs := []*client.ScheduleOptions{
		dbBackupSchedule(cfg),
		outboxPurgeSchedule(cfg),
	}
	for _, opts := range specs {
		if err := ensure(ctx, c, opts); err != nil {
			return err
		}
	}
	return nil
}

func ensure(ctx context.Context, c client.Client, opts *client.ScheduleOptions) error {
	handle := c.ScheduleClient().GetHandle(ctx, opts.ID)
	if _, err := handle.Describe(ctx); err == nil {
		// Already exists; leave whatever an operator has configured alone.
		return nil
	} else if !isNotFound(err) {
		return fmt.Errorf("schedule: describe %q: %w", opts.ID, err)
	}

	if _, err := c.ScheduleClient().Create(ctx, *opts); err != nil {
		if status.Code(err) == codes.AlreadyExists {
			// Another replica created it between our Describe and Create; safe to ignore.
			return nil
		}
		return fmt.Errorf("schedule: create %q: %w", opts.ID, err)
	}
	return nil
}

func isNotFound(err error) bool {
	var nf *serviceerror.NotFound
	return errors.As(err, &nf)
}

func dbBackupSchedule(cfg *config.Config) *client.ScheduleOptions {
	return &client.ScheduleOptions{
		ID: IDDBBackup,
		Spec: client.ScheduleSpec{
			Intervals: []client.ScheduleIntervalSpec{{Every: cfg.Schedule.DBBackupEvery}},
		},
		Action: &client.ScheduleWorkflowAction{
			ID:                 IDDBBackup,
			Workflow:           workflows.DBBackupWorkflow,
			TaskQueue:          cfg.Temporal.TaskQueue,
			WorkflowRunTimeout: cfg.Schedule.DBBackupEvery,
		},
		Overlap:        enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		PauseOnFailure: false,
	}
}

func outboxPurgeSchedule(cfg *config.Config) *client.ScheduleOptions {
	return &client.ScheduleOptions{
		ID: IDOutboxPurge,
		Spec: client.ScheduleSpec{
			Intervals: []client.ScheduleIntervalSpec{{Every: cfg.Schedule.OutboxPurgeEvery}},
		},
		Action: &client.ScheduleWorkflowAction{
			ID:                 IDOutboxPurge,
			Workflow:           workflows.OutboxPurgeWorkflow,
			TaskQueue:          cfg.Temporal.TaskQueue,
			WorkflowRunTimeout: cfg.Schedule.OutboxPurgeEvery,
		},
		Overlap:        enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		PauseOnFailure: false,
	}
}

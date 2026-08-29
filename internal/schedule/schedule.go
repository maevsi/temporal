// Package schedule creates the Temporal Schedules for the worker's
// cron jobs: cadences live in Temporal's native Schedule API
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

	"github.com/maevsi/temporal/internal/config"
	"github.com/maevsi/temporal/internal/workflows"
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
		if isAlreadyExists(err) {
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

// isAlreadyExists reports whether err says the Schedule was created by someone else first, which is what a second worker replica sees when both bootstrap at the same time.
// The check has to go through errors.As on the SDK's own error types rather than grpc's status.Code: the SDK converts gRPC status errors into serviceerror values, and those do not implement GRPCStatus(), so status.Code would report codes.Unknown for every one of them.
func isAlreadyExists(err error) bool {
	var ae *serviceerror.AlreadyExists
	if errors.As(err, &ae) {
		return true
	}
	var started *serviceerror.WorkflowExecutionAlreadyStarted
	return errors.As(err, &started)
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
			WorkflowRunTimeout: workflows.DBBackupRunTimeout,
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
			WorkflowRunTimeout: workflows.OutboxPurgeRunTimeout,
		},
		Overlap:        enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		PauseOnFailure: false,
	}
}

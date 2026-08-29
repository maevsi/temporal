package workflows

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/maevsi/temporal/internal/activities"
	"github.com/maevsi/temporal/internal/sentrycrons"
)

type OutboxPurgeWorkflowSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func TestOutboxPurgeWorkflowSuite(t *testing.T) {
	suite.Run(t, new(OutboxPurgeWorkflowSuite))
}

func (s *OutboxPurgeWorkflowSuite) TestSuccess_ChecksInProgressThenOK() {
	env := s.NewTestWorkflowEnvironment()
	a := activityRef()

	var statuses []sentrycrons.Status
	env.OnActivity(a.SentryCheckIn, mock.Anything, mock.MatchedBy(func(in activities.SentryCheckInInput) bool {
		return in.Job == activities.JobOutboxPurge
	})).Return(func(_ context.Context, in activities.SentryCheckInInput) error {
		statuses = append(statuses, in.Status)
		return nil
	})

	want := activities.OutboxPurgeResult{RowsDeleted: 7}
	env.OnActivity(a.OutboxPurge, mock.Anything, activities.OutboxPurgeInput{}).Return(want, nil)

	env.ExecuteWorkflow(OutboxPurgeWorkflow)

	s.Require().True(env.IsWorkflowCompleted())
	s.Require().NoError(env.GetWorkflowError())

	var got activities.OutboxPurgeResult
	s.Require().NoError(env.GetWorkflowResult(&got))
	s.Equal(want, got)
	s.Equal([]sentrycrons.Status{sentrycrons.StatusInProgress, sentrycrons.StatusOK}, statuses)
}

func (s *OutboxPurgeWorkflowSuite) TestFailure_ChecksInProgressThenError() {
	env := s.NewTestWorkflowEnvironment()
	a := activityRef()

	var statuses []sentrycrons.Status
	env.OnActivity(a.SentryCheckIn, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.SentryCheckInInput) error {
		statuses = append(statuses, in.Status)
		return nil
	})

	env.OnActivity(a.OutboxPurge, mock.Anything, activities.OutboxPurgeInput{}).Return(
		activities.OutboxPurgeResult{},
		temporal.NewApplicationError("db unreachable", "PostgresError"),
	)

	env.ExecuteWorkflow(OutboxPurgeWorkflow)

	s.Require().True(env.IsWorkflowCompleted())
	s.Require().Error(env.GetWorkflowError())
	s.Equal([]sentrycrons.Status{sentrycrons.StatusInProgress, sentrycrons.StatusError}, statuses)
}

// TestCancellation_StillChecksInError covers a run cancelled while the activity is in flight.
// The terminal check-in has to go out on a disconnected context: on the workflow's own context ExecuteActivity short-circuits with a CanceledError without scheduling anything, and the Sentry monitor would sit in StatusInProgress until its own max-runtime alert fired.
func (s *OutboxPurgeWorkflowSuite) TestCancellation_StillChecksInError() {
	env := s.NewTestWorkflowEnvironment()
	a := activityRef()

	var statuses []sentrycrons.Status
	env.OnActivity(a.SentryCheckIn, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.SentryCheckInInput) error {
		statuses = append(statuses, in.Status)
		return nil
	})

	env.OnActivity(a.OutboxPurge, mock.Anything, activities.OutboxPurgeInput{}).Return(
		func(ctx context.Context, _ activities.OutboxPurgeInput) (activities.OutboxPurgeResult, error) {
			<-ctx.Done()
			return activities.OutboxPurgeResult{}, ctx.Err()
		})

	env.RegisterDelayedCallback(env.CancelWorkflow, time.Second)
	env.ExecuteWorkflow(OutboxPurgeWorkflow)

	s.Require().True(env.IsWorkflowCompleted())
	s.Require().Error(env.GetWorkflowError())
	s.Equal([]sentrycrons.Status{sentrycrons.StatusInProgress, sentrycrons.StatusError}, statuses)
}

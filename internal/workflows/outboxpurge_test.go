package workflows

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/maevsi/temporal-worker-go/internal/activities"
	"github.com/maevsi/temporal-worker-go/internal/sentrycrons"
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

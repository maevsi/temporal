package workflows

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/maevsi/temporal-worker-go/internal/activities"
	"github.com/maevsi/temporal-worker-go/internal/sentrycrons"
)

type DBBackupWorkflowSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func TestDBBackupWorkflowSuite(t *testing.T) {
	suite.Run(t, new(DBBackupWorkflowSuite))
}

// activityRef mirrors the nil-receiver method-expression the production
// workflow code uses to reference activities (see checkIn / DBBackupWorkflow),
// so OnActivity registers against the exact same function value.
func activityRef() *activities.Activities {
	var a *activities.Activities
	return a
}

func (s *DBBackupWorkflowSuite) TestSuccess_ChecksInProgressThenOK() {
	env := s.NewTestWorkflowEnvironment()
	a := activityRef()

	var statuses []sentrycrons.Status
	env.OnActivity(a.SentryCheckIn, mock.Anything, mock.MatchedBy(func(in activities.SentryCheckInInput) bool {
		return in.Job == activities.JobDBBackup
	})).Return(func(_ context.Context, in activities.SentryCheckInInput) error {
		statuses = append(statuses, in.Status)
		return nil
	})

	want := activities.DBBackupResult{FilesUploaded: 3, BytesUploaded: 1024}
	env.OnActivity(a.DBBackup, mock.Anything, activities.DBBackupInput{}).Return(want, nil)

	env.ExecuteWorkflow(DBBackupWorkflow)

	s.Require().True(env.IsWorkflowCompleted())
	s.Require().NoError(env.GetWorkflowError())

	var got activities.DBBackupResult
	s.Require().NoError(env.GetWorkflowResult(&got))
	s.Equal(want, got)
	s.Equal([]sentrycrons.Status{sentrycrons.StatusInProgress, sentrycrons.StatusOK}, statuses)
}

func (s *DBBackupWorkflowSuite) TestFailure_ChecksInProgressThenError() {
	env := s.NewTestWorkflowEnvironment()
	a := activityRef()

	var statuses []sentrycrons.Status
	env.OnActivity(a.SentryCheckIn, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.SentryCheckInInput) error {
		statuses = append(statuses, in.Status)
		return nil
	})

	env.OnActivity(a.DBBackup, mock.Anything, activities.DBBackupInput{}).Return(
		activities.DBBackupResult{},
		temporal.NewApplicationError("sync exploded", activities.ErrTypeConfig),
	)

	env.ExecuteWorkflow(DBBackupWorkflow)

	s.Require().True(env.IsWorkflowCompleted())
	s.Require().Error(env.GetWorkflowError())
	s.Equal([]sentrycrons.Status{sentrycrons.StatusInProgress, sentrycrons.StatusError}, statuses)
}

func (s *DBBackupWorkflowSuite) TestSentryCheckInFailureDoesNotFailWorkflow() {
	env := s.NewTestWorkflowEnvironment()
	a := activityRef()

	env.OnActivity(a.SentryCheckIn, mock.Anything, mock.Anything).Return(errors.New("sentry is down"))
	env.OnActivity(a.DBBackup, mock.Anything, activities.DBBackupInput{}).Return(activities.DBBackupResult{FilesUploaded: 1}, nil)

	env.ExecuteWorkflow(DBBackupWorkflow)

	s.Require().True(env.IsWorkflowCompleted())
	require.NoError(s.T(), env.GetWorkflowError())
}

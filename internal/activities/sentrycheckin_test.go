package activities

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/maevsi/temporal/internal/sentrycrons"
)

// newSentryClient builds a sentrycrons.Client for a test, failing the test rather than returning the error, since these tests only ever pass URLs that are known-good.
func newSentryClient(t *testing.T, checkInURL string) *sentrycrons.Client {
	t.Helper()
	c, err := sentrycrons.New(checkInURL)
	require.NoError(t, err)
	return c
}

// runCheckIn executes the SentryCheckIn activity, which (unlike DBBackup
// and OutboxPurge) returns only an error and so doesn't fit the generic
// runActivity helper used elsewhere in this package.
func runCheckIn(t *testing.T, a *Activities, in SentryCheckInInput) error {
	t.Helper()
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(a.SentryCheckIn)
	_, err := env.ExecuteActivity(a.SentryCheckIn, in)
	return err
}

func TestSentryCheckIn_RoutesToConfiguredClient(t *testing.T) {
	var gotStatus string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotStatus = r.URL.Query().Get("status")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	a := &Activities{
		SentryDBBackup:    newSentryClient(t, srv.URL),
		SentryOutboxPurge: newSentryClient(t, ""), // unconfigured, must not be used
	}

	err := runCheckIn(t, a, SentryCheckInInput{Job: JobDBBackup, Status: sentrycrons.StatusOK})
	require.NoError(t, err)
	assert.Equal(t, "ok", gotStatus)
}

func TestSentryCheckIn_UnknownJobErrors(t *testing.T) {
	a := &Activities{}
	err := runCheckIn(t, a, SentryCheckInInput{Job: "not-a-real-job", Status: sentrycrons.StatusOK})
	require.Error(t, err)
}

func TestSentryCheckIn_UnconfiguredClientIsNoOp(t *testing.T) {
	a := &Activities{SentryOutboxPurge: newSentryClient(t, "")}
	err := runCheckIn(t, a, SentryCheckInInput{Job: JobOutboxPurge, Status: sentrycrons.StatusInProgress})
	require.NoError(t, err)
}

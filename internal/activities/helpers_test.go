package activities

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
)

// assertApplicationErrorType asserts that err is (or wraps) a Temporal
// *temporal.ApplicationError with the given Type, which is what
// RetryPolicy.NonRetryableErrorTypes matches against.
func assertApplicationErrorType(t *testing.T, err error, wantType string) {
	t.Helper()
	var appErr *temporal.ApplicationError
	require.True(t, errors.As(err, &appErr), "expected a *temporal.ApplicationError, got %T: %v", err, err)
	assert.Equal(t, wantType, appErr.Type())
}

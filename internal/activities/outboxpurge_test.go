package activities

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeDB is a hand-rolled fake of the DBExecutor interface that records
// the query and args it was called with and returns a scripted result.
type fakeDB struct {
	rowsAffected int64
	err          error

	gotQuery string
	gotArgs  []any
}

func (f *fakeDB) Exec(_ context.Context, sql string, args ...any) (int64, error) {
	f.gotQuery = sql
	f.gotArgs = args
	if f.err != nil {
		return 0, f.err
	}
	return f.rowsAffected, nil
}

func TestOutboxPurge_DeletesWithDefaultRetention(t *testing.T) {
	db := &fakeDB{rowsAffected: 42}
	a := &Activities{
		DB:               db,
		OutboxSchema:     "vibetype_private",
		OutboxTable:      "outbox",
		DefaultRetention: 24 * time.Hour,
	}

	result, err := runActivity(t, a.OutboxPurge, OutboxPurgeInput{})
	require.NoError(t, err)
	assert.Equal(t, int64(42), result.RowsDeleted)
	assert.Equal(t, `DELETE FROM "vibetype_private"."outbox" WHERE created_at < now() - $1::interval`, db.gotQuery)
	require.Len(t, db.gotArgs, 1)
	assert.Equal(t, "86400 seconds", db.gotArgs[0])
}

func TestOutboxPurge_InputRetentionOverridesDefault(t *testing.T) {
	db := &fakeDB{rowsAffected: 0}
	a := &Activities{
		DB:               db,
		OutboxSchema:     "vibetype_private",
		OutboxTable:      "outbox",
		DefaultRetention: 24 * time.Hour,
	}

	_, err := runActivity(t, a.OutboxPurge, OutboxPurgeInput{Retention: 2 * time.Hour})
	require.NoError(t, err)
	require.Len(t, db.gotArgs, 1)
	assert.Equal(t, "7200 seconds", db.gotArgs[0])
}

func TestOutboxPurge_NoRetentionConfiguredIsNonRetryableConfigError(t *testing.T) {
	a := &Activities{DB: &fakeDB{}, OutboxSchema: "vibetype_private", OutboxTable: "outbox"}

	_, err := runActivity(t, a.OutboxPurge, OutboxPurgeInput{})
	require.Error(t, err)
	assertApplicationErrorType(t, err, ErrTypeConfig)
}

func TestOutboxPurge_MissingConfigIsNonRetryableConfigError(t *testing.T) {
	a := &Activities{}
	_, err := runActivity(t, a.OutboxPurge, OutboxPurgeInput{})
	require.Error(t, err)
	assertApplicationErrorType(t, err, ErrTypeConfig)
}

func TestOutboxPurge_DBErrorPropagates(t *testing.T) {
	db := &fakeDB{err: assert.AnError}
	a := &Activities{DB: db, OutboxSchema: "vibetype_private", OutboxTable: "outbox", DefaultRetention: time.Hour}

	_, err := runActivity(t, a.OutboxPurge, OutboxPurgeInput{})
	require.Error(t, err)
}

func TestPgIntervalLiteral(t *testing.T) {
	assert.Equal(t, "86400 seconds", pgIntervalLiteral(24*time.Hour))
	assert.Equal(t, "7200 seconds", pgIntervalLiteral(2*time.Hour))
	assert.Equal(t, "90 seconds", pgIntervalLiteral(90*time.Second))
}

func TestQuoteIdent(t *testing.T) {
	assert.Equal(t, `"outbox"`, quoteIdent("outbox"))
	assert.Equal(t, `"weird""name"`, quoteIdent(`weird"name`))
}

package activities

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

// OutboxPurge reproduces the original jobber command:
//
//	DELETE FROM vibetype_private.outbox WHERE created_at < now() - interval '24 hours'
//
// against the org's central Postgres instance, via the dedicated
// vibetype_role_service_* role configured for this worker. The retention
// window comes from input.Retention if set, otherwise from the Activities'
// configured default (OUTBOX_PURGE_RETENTION, defaulting to 24h).
//
// The interval is passed as a bound parameter cast to ::interval rather
// than interpolated into the query text, so it is always valid regardless
// of the configured duration's units.
func (a *Activities) OutboxPurge(ctx context.Context, input OutboxPurgeInput) (OutboxPurgeResult, error) {
	logger := activity.GetLogger(ctx)

	if a.DB == nil || a.OutboxSchema == "" || a.OutboxTable == "" {
		return OutboxPurgeResult{}, temporal.NewApplicationError(
			"OutboxPurge activity is missing required configuration (DB executor, schema, or table)",
			ErrTypeConfig,
		)
	}

	retention := input.Retention
	if retention <= 0 {
		retention = a.DefaultRetention
	}
	if retention <= 0 {
		return OutboxPurgeResult{}, temporal.NewApplicationError(
			"OutboxPurge activity has no positive retention period configured",
			ErrTypeConfig,
		)
	}

	query := fmt.Sprintf(
		`DELETE FROM %s.%s WHERE created_at < now() - $1::interval`,
		quoteIdent(a.OutboxSchema), quoteIdent(a.OutboxTable),
	)

	rows, err := a.DB.Exec(ctx, query, pgIntervalLiteral(retention))
	if err != nil {
		return OutboxPurgeResult{}, fmt.Errorf("purge outbox rows older than %s: %w", retention, err)
	}

	logger.Info("outbox purge complete", "rowsDeleted", rows, "retention", retention)
	return OutboxPurgeResult{RowsDeleted: rows}, nil
}

// pgIntervalLiteral renders a time.Duration as a Postgres interval literal
// (e.g. "3600 seconds") that is always parseable by ::interval regardless
// of the duration's magnitude or the Go string representation's units.
func pgIntervalLiteral(d time.Duration) string {
	return fmt.Sprintf("%d seconds", int64(d.Seconds()))
}

// quoteIdent double-quotes a Postgres identifier and escapes embedded
// quotes. Schema/table names come from this service's own configuration,
// not user input, but quoting them keeps the query well-formed regardless.
func quoteIdent(ident string) string {
	escaped := ""
	for _, r := range ident {
		if r == '"' {
			escaped += `""`
			continue
		}
		escaped += string(r)
	}
	return `"` + escaped + `"`
}

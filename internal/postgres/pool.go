// Package postgres wires up the connection to the org's existing central
// Postgres instance and adapts it to the small activities.DBExecutor
// interface the OutboxPurge activity depends on.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/maevsi/temporal-worker-go/internal/config"
)

// NewPool opens a connection pool using the given configuration. Callers
// own the returned pool and must Close it on shutdown.
func NewPool(ctx context.Context, cfg config.Postgres) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("postgres: create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return pool, nil
}

// Executor adapts a *pgxpool.Pool to activities.DBExecutor.
type Executor struct {
	Pool *pgxpool.Pool
}

// Exec runs sql with args and returns the number of affected rows.
func (e *Executor) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	tag, err := e.Pool.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

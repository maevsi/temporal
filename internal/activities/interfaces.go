package activities

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
)

// S3API is the subset of the AWS S3 client used by the DBBackup activity: transfermanager.S3APIClient already covers both the HeadObject existence/size check and everything needed to upload a file, transparently using a multipart upload above S3's single-PutObject limit.
// It is satisfied directly by *s3.Client (see internal/s3client), and is small enough to fake by hand in tests without pulling in a generated mock of the whole S3 SDK surface.
type S3API interface {
	transfermanager.S3APIClient
}

// DBExecutor is the subset of a Postgres connection used by the OutboxPurge activity: a single parameterized statement that reports the number of affected rows.
// It is satisfied by *postgres.Executor (see internal/postgres) and is trivial to fake in tests without spinning up a real Postgres instance.
type DBExecutor interface {
	Exec(ctx context.Context, sql string, args ...any) (rowsAffected int64, err error)
}

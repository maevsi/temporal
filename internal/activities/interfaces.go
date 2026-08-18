package activities

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3API is the subset of the AWS S3 client used by the DBBackup activity: a per-key existence/size check, a single-request upload for files under S3's 5 GiB PutObject limit, and the multipart upload operations used above that limit.
// It is satisfied directly by *s3.Client (see internal/s3client), and is small enough to fake by hand in tests without pulling in a generated mock of the whole S3 SDK surface.
type S3API interface {
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

// DBExecutor is the subset of a Postgres connection used by the OutboxPurge activity: a single parameterized statement that reports the number of affected rows.
// It is satisfied by *postgres.Executor (see internal/postgres) and is trivial to fake in tests without spinning up a real Postgres instance.
type DBExecutor interface {
	Exec(ctx context.Context, sql string, args ...any) (rowsAffected int64, err error)
}

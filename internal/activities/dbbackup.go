package activities

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

// DBBackup reproduces the original `aws s3 sync /backups s3://<bucket>/backups` jobber command: it walks SourceDir and uploads every file that is missing from the bucket or whose size differs from what is already there, mirroring the default (no --delete) behavior of `aws s3 sync`.
//
// Unlike the shell command, matching is done by comparing file size via HeadObject rather than the CLI's size+mtime heuristic; this is a deliberate simplification documented in the README.
// Files larger than S3's single-PUT limit (5 GiB) are not supported; a multipart upload via aws-sdk-go-v2/feature/s3/manager would be a natural follow-up if backups grow past that.
func (a *Activities) DBBackup(ctx context.Context, _ DBBackupInput) (DBBackupResult, error) {
	logger := activity.GetLogger(ctx)
	start := time.Now()

	if a.S3 == nil || a.Bucket == "" || a.SourceDir == "" {
		return DBBackupResult{}, temporal.NewApplicationError(
			"DBBackup activity is missing required configuration (S3 client, bucket, or source directory)",
			ErrTypeConfig,
		)
	}

	info, err := os.Stat(a.SourceDir)
	if err != nil {
		return DBBackupResult{}, temporal.NewApplicationError(
			fmt.Sprintf("backup source directory %q is not accessible: %v", a.SourceDir, err),
			ErrTypeConfig,
		)
	}
	if !info.IsDir() {
		return DBBackupResult{}, temporal.NewApplicationError(
			fmt.Sprintf("backup source %q is not a directory", a.SourceDir),
			ErrTypeConfig,
		)
	}

	result := DBBackupResult{}

	err = filepath.WalkDir(a.SourceDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}

		// Long syncs must heartbeat so the Temporal server can detect a
		// dead worker via HeartbeatTimeout instead of waiting out the
		// full StartToCloseTimeout.
		activity.RecordHeartbeat(ctx, path)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rel, err := filepath.Rel(a.SourceDir, path)
		if err != nil {
			return fmt.Errorf("resolve relative path for %q: %w", path, err)
		}
		key := strings.TrimPrefix(strings.TrimSuffix(a.Prefix, "/")+"/"+filepath.ToSlash(rel), "/")

		fileInfo, err := d.Info()
		if err != nil {
			return fmt.Errorf("stat %q: %w", path, err)
		}

		upload, err := a.needsUpload(ctx, key, fileInfo.Size())
		if err != nil {
			return err
		}
		if !upload {
			result.FilesSkipped++
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open %q: %w", path, err)
		}
		defer f.Close()

		if _, err := a.S3.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &a.Bucket,
			Key:    &key,
			Body:   f,
		}); err != nil {
			return fmt.Errorf("upload %q to s3://%s/%s: %w", path, a.Bucket, key, err)
		}

		logger.Debug("uploaded backup file", "path", path, "key", key, "bytes", fileInfo.Size())
		result.FilesUploaded++
		result.BytesUploaded += fileInfo.Size()
		return nil
	})
	if err != nil {
		return result, fmt.Errorf("dbbackup sync failed after uploading %d file(s): %w", result.FilesUploaded, err)
	}

	result.Duration = time.Since(start)
	logger.Info("dbbackup sync complete",
		"filesUploaded", result.FilesUploaded,
		"filesSkipped", result.FilesSkipped,
		"bytesUploaded", result.BytesUploaded,
		"duration", result.Duration,
	)
	return result, nil
}

// needsUpload reports whether the local file at the given size should be
// uploaded to key, based on whether the object exists in S3 and, if so,
// whether its size differs from the local file.
func (a *Activities) needsUpload(ctx context.Context, key string, localSize int64) (bool, error) {
	head, err := a.S3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &a.Bucket,
		Key:    &key,
	})
	if err != nil {
		if isNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("head s3://%s/%s: %w", a.Bucket, key, err)
	}
	if head.ContentLength == nil || *head.ContentLength != localSize {
		return true, nil
	}
	return false, nil
}

// isNotFound reports whether err represents an S3 404 response, whether or
// not the SDK managed to decode it into a typed NotFound error (HeadObject
// responses have no body to decode a typed error from, so this commonly
// surfaces only as an HTTP status).
func isNotFound(err error) bool {
	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) {
		return respErr.HTTPStatusCode() == 404
	}
	return false
}

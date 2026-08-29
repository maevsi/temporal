package activities

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

// DBBackup walks SourceDir and uploads every file that is missing from the bucket or whose size differs from what is already there, mirroring the default (no --delete) behavior of `aws s3 sync`.
//
// Matching is done by comparing file size via HeadObject rather than the CLI's size+mtime heuristic: DB dump files get a fresh mtime on every regeneration regardless of whether their content changed, so an mtime check would force a re-upload on every single run and defeat the point of diffing. This is a deliberate simplification documented in the README.
// Uploads go through the AWS SDK's S3 transfer manager (feature/s3/transfermanager), which automatically switches to a multipart upload for files above its MultipartUploadThreshold (16 MiB by default, so in practice every real dump file takes the multipart path).
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
	uploader := newUploader(a.S3, func(bytesTransferred, totalBytes int64) {
		activity.RecordHeartbeat(ctx, bytesTransferred, totalBytes)
	})

	err = filepath.WalkDir(a.SourceDir, func(filePath string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}

		// Long syncs must heartbeat so the Temporal server can detect a dead worker via HeartbeatTimeout instead of waiting out the full StartToCloseTimeout.
		// This marks progress through the directory; newUploader keeps the heartbeats coming during a single file's transfer.
		activity.RecordHeartbeat(ctx, filePath)

		rel, err := filepath.Rel(a.SourceDir, filePath)
		if err != nil {
			return fmt.Errorf("resolve relative path for %q: %w", filePath, err)
		}
		// path.Join collapses the prefix's own slashes, so a stray "backups/" or "/backups" in S3_PREFIX cannot produce a doubled or leading separator in the key.
		key := strings.TrimPrefix(path.Join(a.Prefix, filepath.ToSlash(rel)), "/")

		fileInfo, err := d.Info()
		if err != nil {
			return fmt.Errorf("stat %q: %w", filePath, err)
		}

		upload, err := a.needsUpload(ctx, key, fileInfo.Size())
		if err != nil {
			return err
		}
		if !upload {
			result.FilesSkipped++
			return nil
		}

		f, err := os.Open(filePath)
		if err != nil {
			return fmt.Errorf("open %q: %w", filePath, err)
		}
		// This defer runs when the walk callback returns for this file, not at the end of the whole walk, so descriptors do not pile up across a large source directory.
		// The close error is dropped because the file is only ever read from, where closing cannot lose data.
		defer func() { _ = f.Close() }()

		if _, err := uploader.UploadObject(ctx, &transfermanager.UploadObjectInput{
			Bucket: &a.Bucket,
			Key:    &key,
			Body:   f,
		}); err != nil {
			return fmt.Errorf("upload %q to s3://%s/%s: %w", filePath, a.Bucket, key, err)
		}

		logger.Debug("uploaded backup file", "path", filePath, "key", key, "bytes", fileInfo.Size())
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

// progressHeartbeat adapts the transfer manager's byte-progress hook to a plain callback.
// The transfer manager invokes progress hooks synchronously from the goroutines running the transfer, so beat must be cheap and safe to call concurrently.
type progressHeartbeat struct {
	beat func(bytesTransferred, totalBytes int64)
}

// OnObjectBytesTransferred implements transfermanager.ObjectBytesTransferredListener.
func (h progressHeartbeat) OnObjectBytesTransferred(_ context.Context, event *transfermanager.ObjectBytesTransferredEvent) {
	h.beat(event.BytesTransferred, event.TotalBytes)
}

// newUploader builds the transfer manager DBBackup uploads through, wired so transfer progress reaches beat.
// Heartbeating once per file before its upload starts is not enough on its own: transferring a single large dump easily outlasts the workflow's HeartbeatTimeout, and the Temporal server would then time the activity out mid-upload and retry it from scratch on every attempt, so the file would never finish uploading no matter how many attempts it got.
// On the multipart path the hook fires as each part completes; below the multipart threshold it fires only once, after the upload, which is fine because a file that small transfers well inside the timeout.
func newUploader(client S3API, beat func(bytesTransferred, totalBytes int64)) *transfermanager.Client {
	return transfermanager.New(client, func(o *transfermanager.Options) {
		o.ObjectProgressListeners.Register(progressHeartbeat{beat: beat})
	})
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

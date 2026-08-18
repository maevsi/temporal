package activities

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

// putObjectSizeLimit is S3's hard cap on a single PutObject request; larger files must go through a multipart upload instead.
// A var rather than a const so tests can lower it to exercise the multipart path without uploading gigabytes of fixture data.
var putObjectSizeLimit int64 = 5 * 1024 * 1024 * 1024 // 5 GiB

// multipartChunkSize is the size of each part in a multipart upload.
// S3 requires every part but the last to be at least 5 MiB and allows at most 10,000 parts per upload, so 100 MiB supports files up to roughly 976 GiB without tuning.
// A var for the same testability reason as putObjectSizeLimit.
var multipartChunkSize int64 = 100 * 1024 * 1024 // 100 MiB

// DBBackup reproduces the original `aws s3 sync /backups s3://<bucket>/backups` jobber command: it walks SourceDir and uploads every file that is missing from the bucket or whose size differs from what is already there, mirroring the default (no --delete) behavior of `aws s3 sync`.
//
// Unlike the shell command, matching is done by comparing file size via HeadObject rather than the CLI's size+mtime heuristic; this is a deliberate simplification documented in the README.
// Files larger than S3's single-PUT limit (5 GiB) are uploaded via S3's multipart upload API instead of a single PutObject.
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

		if fileInfo.Size() > putObjectSizeLimit {
			if err := a.multipartUpload(ctx, key, f, fileInfo.Size()); err != nil {
				return fmt.Errorf("multipart upload %q to s3://%s/%s: %w", path, a.Bucket, key, err)
			}
		} else if _, err := a.S3.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &a.Bucket,
			Key:    &key,
			Body:   f,
		}); err != nil {
			_ = f.Close()
			return fmt.Errorf("upload %q to s3://%s/%s: %w", path, a.Bucket, key, err)
		}

		if err := f.Close(); err != nil {
			return fmt.Errorf("close %q: %w", path, err)
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

// multipartUpload uploads r (of the given total size) to key using S3's multipart upload API, required for files larger than putObjectSizeLimit.
// It aborts the upload on any failure so S3 doesn't keep billing storage for an incomplete upload.
func (a *Activities) multipartUpload(ctx context.Context, key string, r io.Reader, size int64) error {
	created, err := a.S3.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: &a.Bucket,
		Key:    &key,
	})
	if err != nil {
		return fmt.Errorf("create multipart upload: %w", err)
	}
	uploadID := created.UploadId

	parts, err := a.uploadParts(ctx, key, *uploadID, r, size)
	if err != nil {
		if _, abortErr := a.S3.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   &a.Bucket,
			Key:      &key,
			UploadId: uploadID,
		}); abortErr != nil {
			return fmt.Errorf("%w (also failed to abort multipart upload: %v)", err, abortErr)
		}
		return err
	}

	if _, err := a.S3.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          &a.Bucket,
		Key:             &key,
		UploadId:        uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}); err != nil {
		return fmt.Errorf("complete multipart upload: %w", err)
	}
	return nil
}

// uploadParts uploads r in multipartChunkSize chunks, returning the completed part list CompleteMultipartUpload needs.
func (a *Activities) uploadParts(ctx context.Context, key, uploadID string, r io.Reader, size int64) ([]types.CompletedPart, error) {
	var parts []types.CompletedPart
	remaining := size
	for partNumber := int32(1); remaining > 0; partNumber++ {
		n := min(remaining, multipartChunkSize)

		// Long uploads must heartbeat so the Temporal server can detect a
		// dead worker via HeartbeatTimeout instead of waiting out the
		// full StartToCloseTimeout.
		activity.RecordHeartbeat(ctx, fmt.Sprintf("%s part %d", key, partNumber))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// pn is a fresh variable per iteration: PartNumber below is
		// stored by pointer in the returned parts slice, and taking
		// &partNumber directly would alias the same loop variable
		// across every part.
		pn := partNumber
		out, err := a.S3.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:        &a.Bucket,
			Key:           &key,
			UploadId:      &uploadID,
			PartNumber:    &pn,
			Body:          io.LimitReader(r, n),
			ContentLength: &n,
		})
		if err != nil {
			return nil, fmt.Errorf("upload part %d: %w", partNumber, err)
		}

		parts = append(parts, types.CompletedPart{ETag: out.ETag, PartNumber: &pn})
		remaining -= n
	}
	return parts, nil
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

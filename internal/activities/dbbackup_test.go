package activities

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

// multipartUploadThreshold mirrors transfermanager's unexported defaultMultipartUploadThreshold.
// It is 16 MiB, not S3's 5 GiB single-PutObject limit, so any fixture meant to exercise the path real dump files take has to clear it.
const multipartUploadThreshold = 16 * 1024 * 1024

// fakeS3 is a hand-rolled fake of the S3API interface, keyed by S3 object key, that lets tests assert on exactly what the activity would have uploaded without touching real AWS infrastructure.
//
// Both upload paths are implemented, because both are reachable: transfermanager switches to a multipart upload above 16 MiB, which is well under the size of a real database dump, so the multipart path is the one production almost always takes.
// The parts are reassembled the way S3 itself does, in the order the caller lists them at CompleteMultipartUpload, so a test can compare the resulting object against the bytes on disk.
type fakeS3 struct {
	// mu guards every field below: the transfer manager uploads parts from several goroutines at once.
	mu sync.Mutex

	// existing simulates objects already present in the bucket, keyed by
	// object key, valued by their content length.
	existing map[string]int64
	// uploaded records the final body of every completed upload, keyed by key, whether it arrived as one PutObject or as reassembled multipart parts.
	uploaded map[string][]byte
	// parts holds the parts of in-flight multipart uploads, keyed by upload ID and then part number.
	parts map[string]map[int32][]byte
	// multipartUploads counts CreateMultipartUpload calls, so a test can assert which upload path a fixture actually took.
	multipartUploads int
	// aborted counts AbortMultipartUpload calls, which the transfer manager issues when a multipart upload fails partway through.
	aborted int

	headErr error
	putErr  error
	partErr error
}

func newFakeS3() *fakeS3 {
	return &fakeS3{
		existing: map[string]int64{},
		uploaded: map[string][]byte{},
		parts:    map[string]map[int32][]byte{},
	}
}

func (f *fakeS3) CreateMultipartUpload(_ context.Context, params *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.multipartUploads++
	uploadID := fmt.Sprintf("upload-%d", f.multipartUploads)
	f.parts[uploadID] = map[int32][]byte{}
	return &s3.CreateMultipartUploadOutput{Bucket: params.Bucket, Key: params.Key, UploadId: &uploadID}, nil
}

func (f *fakeS3) UploadPart(_ context.Context, params *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	if f.partErr != nil {
		return nil, f.partErr
	}
	body, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.parts[*params.UploadId][*params.PartNumber] = body
	etag := fmt.Sprintf("etag-%d", *params.PartNumber)
	return &s3.UploadPartOutput{ETag: &etag}, nil
}

func (f *fakeS3) CompleteMultipartUpload(_ context.Context, params *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var object []byte
	for _, part := range params.MultipartUpload.Parts {
		object = append(object, f.parts[*params.UploadId][*part.PartNumber]...)
	}
	delete(f.parts, *params.UploadId)
	f.uploaded[*params.Key] = object
	return &s3.CompleteMultipartUploadOutput{Bucket: params.Bucket, Key: params.Key}, nil
}

func (f *fakeS3) AbortMultipartUpload(_ context.Context, params *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.aborted++
	delete(f.parts, *params.UploadId)
	return &s3.AbortMultipartUploadOutput{}, nil
}

func (f *fakeS3) GetObject(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	panic("not expected: DBBackup never reads objects back")
}

func (f *fakeS3) ListObjectsV2(_ context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	panic("not expected: DBBackup never lists objects")
}

func (f *fakeS3) HeadObject(_ context.Context, params *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if f.headErr != nil {
		return nil, f.headErr
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	size, ok := f.existing[*params.Key]
	if !ok {
		return nil, &awshttp.ResponseError{
			ResponseError: &smithyhttp.ResponseError{
				Response: &smithyhttp.Response{Response: &http.Response{StatusCode: 404}},
			},
		}
	}
	return &s3.HeadObjectOutput{ContentLength: &size}, nil
}

func (f *fakeS3) PutObject(_ context.Context, params *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if f.putErr != nil {
		return nil, f.putErr
	}
	body, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.uploaded[*params.Key] = body
	return &s3.PutObjectOutput{}, nil
}

func writeFile(t *testing.T, dir, rel, content string) {
	t.Helper()
	path := filepath.Join(dir, rel)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}

func runActivity[TIn, TOut any](t *testing.T, fn func(context.Context, TIn) (TOut, error), in TIn) (TOut, error) {
	t.Helper()
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(fn)
	val, err := env.ExecuteActivity(fn, in)
	var out TOut
	// Only the success path carries a result worth decoding, and a decode failure there would otherwise surface as a confusing assertion against a zero value.
	if err == nil && val != nil {
		require.NoError(t, val.Get(&out), "decode activity result")
	}
	return out, err
}

func TestDBBackup_UploadsNewAndChangedFiles(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "dump.sql", "new file, not in bucket yet")
	writeFile(t, dir, "nested/dump2.sql", "changed content")
	writeFile(t, dir, "unchanged.sql", "same everywhere")

	fake := newFakeS3()
	fake.existing["backups/nested/dump2.sql"] = 3 // wrong size -> re-upload
	fake.existing["backups/unchanged.sql"] = int64(len("same everywhere"))

	a := &Activities{
		S3:        fake,
		Bucket:    "maevsi-backups",
		Prefix:    "backups",
		SourceDir: dir,
	}

	result, err := runActivity(t, a.DBBackup, DBBackupInput{})
	require.NoError(t, err)

	assert.Equal(t, 2, result.FilesUploaded)
	assert.Equal(t, 1, result.FilesSkipped)
	assert.Equal(t, []byte("new file, not in bucket yet"), fake.uploaded["backups/dump.sql"])
	assert.Equal(t, []byte("changed content"), fake.uploaded["backups/nested/dump2.sql"])
	assert.NotContains(t, fake.uploaded, "backups/unchanged.sql")
}

func TestDBBackup_SkipsUnchangedFiles(t *testing.T) {
	dir := t.TempDir()
	content := "identical content"
	writeFile(t, dir, "dump.sql", content)

	fake := newFakeS3()
	fake.existing["backups/dump.sql"] = int64(len(content))

	a := &Activities{S3: fake, Bucket: "b", Prefix: "backups", SourceDir: dir}

	result, err := runActivity(t, a.DBBackup, DBBackupInput{})
	require.NoError(t, err)
	assert.Equal(t, 0, result.FilesUploaded)
	assert.Equal(t, 1, result.FilesSkipped)
}

func TestDBBackup_MissingSourceDirIsNonRetryableConfigError(t *testing.T) {
	a := &Activities{S3: newFakeS3(), Bucket: "b", Prefix: "backups", SourceDir: "/does/not/exist"}

	_, err := runActivity(t, a.DBBackup, DBBackupInput{})
	require.Error(t, err)
	assertApplicationErrorType(t, err, ErrTypeConfig)
}

func TestDBBackup_MissingConfigIsNonRetryableConfigError(t *testing.T) {
	a := &Activities{}
	_, err := runActivity(t, a.DBBackup, DBBackupInput{})
	require.Error(t, err)
	assertApplicationErrorType(t, err, ErrTypeConfig)
}

func TestDBBackup_PutObjectErrorFailsActivity(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "dump.sql", "content")

	fake := newFakeS3()
	fake.putErr = assert.AnError

	a := &Activities{S3: fake, Bucket: "b", Prefix: "backups", SourceDir: dir}
	_, err := runActivity(t, a.DBBackup, DBBackupInput{})
	require.Error(t, err)
}

// writeLargeFile writes a file of the given size and returns its content, so a test can compare what ended up in the bucket against what was on disk.
// The byte pattern uses a prime stride so parts reassembled in the wrong order show up as a mismatch instead of lining up by coincidence.
func writeLargeFile(t *testing.T, dir, rel string, size int) []byte {
	t.Helper()
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i % 251)
	}
	path := filepath.Join(dir, rel)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, content, 0o644))
	return content
}

// TestDBBackup_UploadsLargeFileViaMultipart covers the upload path a real dump file takes.
// Anything over 16 MiB is split into parts and reassembled by S3 rather than sent as one PutObject, so this asserts the object that ends up in the bucket is byte-for-byte the file on disk.
func TestDBBackup_UploadsLargeFileViaMultipart(t *testing.T) {
	dir := t.TempDir()
	content := writeLargeFile(t, dir, "dump.sql", multipartUploadThreshold+1)

	fake := newFakeS3()
	a := &Activities{S3: fake, Bucket: "b", Prefix: "backups", SourceDir: dir}

	result, err := runActivity(t, a.DBBackup, DBBackupInput{})
	require.NoError(t, err)

	assert.Equal(t, 1, result.FilesUploaded)
	assert.Equal(t, int64(len(content)), result.BytesUploaded)
	assert.Equal(t, 1, fake.multipartUploads, "a file over the threshold must go through a multipart upload")
	assert.Zero(t, fake.aborted)
	assert.True(t, bytes.Equal(content, fake.uploaded["backups/dump.sql"]), "the reassembled object must match the file on disk")
}

// TestNewUploader_ReportsProgressDuringTransfer pins the reason DBBackup wires a progress listener into the transfer manager at all.
// Heartbeating once per file before its upload starts leaves the whole transfer of a large dump silent, so the Temporal server would time the activity out mid-upload and retry it from scratch forever; progress has to arrive while the transfer is still running, not only once it has finished.
func TestNewUploader_ReportsProgressDuringTransfer(t *testing.T) {
	content := make([]byte, multipartUploadThreshold+1)
	fake := newFakeS3()

	var mu sync.Mutex
	var progress []int64
	uploader := newUploader(fake, func(bytesTransferred, _ int64) {
		mu.Lock()
		defer mu.Unlock()
		progress = append(progress, bytesTransferred)
	})

	bucket, key := "b", "backups/dump.sql"
	_, err := uploader.UploadObject(t.Context(), &transfermanager.UploadObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(content),
	})
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.Greater(t, len(progress), 1, "progress must be reported per part, not once at the end of the transfer")
	assert.Equal(t, int64(len(content)), slices.Max(progress), "the final progress report must account for the whole object")
}

// TestDBBackup_KeyBuildingNormalizesPrefix guards the S3 key layout against a prefix that carries its own separators, which would otherwise produce keys like "backups//dump.sql".
func TestDBBackup_KeyBuildingNormalizesPrefix(t *testing.T) {
	for _, prefix := range []string{"backups", "backups/", "/backups", "backups//"} {
		t.Run(prefix, func(t *testing.T) {
			dir := t.TempDir()
			writeFile(t, dir, "nested/dump.sql", "content")

			fake := newFakeS3()
			a := &Activities{S3: fake, Bucket: "b", Prefix: prefix, SourceDir: dir}

			_, err := runActivity(t, a.DBBackup, DBBackupInput{})
			require.NoError(t, err)
			assert.Contains(t, fake.uploaded, "backups/nested/dump.sql")
		})
	}
}

// TestDBBackup_EmptyPrefixUploadsAtBucketRoot covers S3_PREFIX being unset, where keys must not pick up a leading separator.
func TestDBBackup_EmptyPrefixUploadsAtBucketRoot(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "dump.sql", "content")

	fake := newFakeS3()
	a := &Activities{S3: fake, Bucket: "b", SourceDir: dir}

	_, err := runActivity(t, a.DBBackup, DBBackupInput{})
	require.NoError(t, err)
	assert.Contains(t, fake.uploaded, "dump.sql")
}

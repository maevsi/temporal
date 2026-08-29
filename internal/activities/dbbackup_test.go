package activities

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

// fakeS3 is a hand-rolled fake of the S3API interface, keyed by S3 object
// key, that lets tests assert on exactly what the activity would have
// uploaded without touching real AWS infrastructure.
//
// It only implements HeadObject and PutObject meaningfully: every test
// fixture is a few bytes, far under transfermanager's multipart threshold,
// so uploads always take the single-PutObject path. The multipart methods
// below exist only to satisfy transfermanager.S3APIClient at compile time
// and are never expected to be called; multipart chunking itself is
// AWS SDK behavior, not this package's, so it isn't re-tested here.
type fakeS3 struct {
	// existing simulates objects already present in the bucket, keyed by
	// object key, valued by their content length.
	existing map[string]int64
	// uploaded records the body of every PutObject call, keyed by key.
	uploaded map[string][]byte

	headErr error
	putErr  error
}

func newFakeS3() *fakeS3 {
	return &fakeS3{
		existing: map[string]int64{},
		uploaded: map[string][]byte{},
	}
}

func (f *fakeS3) CreateMultipartUpload(_ context.Context, _ *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	panic("not expected: test fixtures are always under the multipart threshold")
}

func (f *fakeS3) UploadPart(_ context.Context, _ *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	panic("not expected: test fixtures are always under the multipart threshold")
}

func (f *fakeS3) CompleteMultipartUpload(_ context.Context, _ *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	panic("not expected: test fixtures are always under the multipart threshold")
}

func (f *fakeS3) AbortMultipartUpload(_ context.Context, _ *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	panic("not expected: test fixtures are always under the multipart threshold")
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
	if val != nil {
		_ = val.Get(&out)
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

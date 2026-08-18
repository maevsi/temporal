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
type fakeS3 struct {
	// existing simulates objects already present in the bucket, keyed by
	// object key, valued by their content length.
	existing map[string]int64
	// uploaded records the bodies of every PutObject call, keyed by key.
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

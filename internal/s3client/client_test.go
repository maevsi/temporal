package s3client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/maevsi/temporal-worker-go/internal/config"
)

func TestNew_ReturnsClientWithValidConfig(t *testing.T) {
	cfg := config.S3{
		Region:          "eu-central-1",
		AccessKeyID:     "AKIAEXAMPLE",
		SecretAccessKey: "secret",
	}

	client, err := New(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestNew_AcceptsCustomEndpoint(t *testing.T) {
	cfg := config.S3{
		Region:          "us-east-1",
		AccessKeyID:     "AKIAEXAMPLE",
		SecretAccessKey: "secret",
		Endpoint:        "https://minio.local:9000",
		UsePathStyle:    true,
	}

	client, err := New(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, client)
}

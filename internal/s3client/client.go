// Package s3client constructs the AWS S3 client used by the DBBackup
// activity from this worker's own config, rather than the default AWS SDK
// credential chain, since credentials are supplied explicitly via env vars
// per this repo's configuration convention (see internal/config).
package s3client

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/maevsi/temporal-worker-go/internal/config"
)

// New builds an *s3.Client (which satisfies activities.S3API) from the given configuration.
// Endpoint/UsePathStyle support S3-compatible object storage in addition to AWS S3 itself.
func New(ctx context.Context, cfg *config.S3) (*s3.Client, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("s3client: load AWS config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.UsePathStyle
	})
	return client, nil
}

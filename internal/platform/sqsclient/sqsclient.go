// Package sqsclient builds the shared *sqs.Client used by every SQS-facing
// adapter (the consumer now, the outbox publisher in Fase 11).
package sqsclient

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/beaglexv/backend-challenge-go/internal/platform/config"
)

// New builds an SQS client from cfg. When AccessKeyID is set, credentials
// are pinned to it (LocalStack's static test credentials); left empty, the
// AWS SDK's own default credential chain resolves them (IAM role, shared
// config, its own environment variables) — the shape a real deployment
// wants. Endpoint, when set, overrides the service endpoint (LocalStack);
// left empty, requests go to the real AWS endpoint for Region.
func New(ctx context.Context, cfg config.SQS) (*sqs.Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
	}
	if cfg.AccessKeyID != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("sqsclient: load AWS config: %w", err)
	}

	client := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})
	return client, nil
}

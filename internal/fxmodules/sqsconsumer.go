package fxmodules

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/beaglexv/backend-challenge-go/internal/adapters/sqsconsumer"
	"github.com/beaglexv/backend-challenge-go/internal/application/processwagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/platform/config"
	"github.com/beaglexv/backend-challenge-go/internal/platform/sqsclient"
)

// SQSConsumerModule provides the *sqs.Client and the wager-transactions
// consumer, and registers its lifecycle: OnStart launches the poll loop,
// OnStop stops it (bounded by fx.StopTimeout, same as every other
// component's graceful shutdown).
var SQSConsumerModule = fx.Module("sqsconsumer",
	fx.Provide(newSQSClient),
	fx.Invoke(registerConsumer),
)

func newSQSClient(cfg *config.Config) (*sqs.Client, error) {
	return sqsclient.New(context.Background(), cfg.SQS)
}

func registerConsumer(lc fx.Lifecycle, client *sqs.Client, cfg *config.Config, processor *processwagertransaction.Service, logger *zap.Logger) {
	consumer := sqsconsumer.New(client, sqsconsumer.Config{
		QueueURL: cfg.SQS.WagerTransactionsQueueURL,
	}, processor, logger)

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			consumer.Start()
			logger.Info("sqs consumer started", zap.String("queueUrl", cfg.SQS.WagerTransactionsQueueURL))
			return nil
		},
		OnStop: func(ctx context.Context) error {
			return consumer.Stop(ctx)
		},
	})
}

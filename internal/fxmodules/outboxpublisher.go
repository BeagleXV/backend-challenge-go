package fxmodules

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/beaglexv/backend-challenge-go/internal/adapters/outboxpublisher"
	"github.com/beaglexv/backend-challenge-go/internal/application/ports"
	"github.com/beaglexv/backend-challenge-go/internal/platform/config"
)

// OutboxPublisherModule provides the ports.EventPublisher (SQS-backed,
// reusing the same *sqs.Client the consumer module provides) and
// registers the publisher worker's lifecycle: OnStart launches the poll
// loop, OnStop stops it (bounded by fx.StopTimeout, same as every other
// component's graceful shutdown).
var OutboxPublisherModule = fx.Module("outboxpublisher",
	fx.Provide(asEventPublisher),
	fx.Invoke(registerOutboxPublisher),
)

func asEventPublisher(client *sqs.Client, cfg *config.Config) ports.EventPublisher {
	return outboxpublisher.NewSQSPublisher(client, cfg.SQS.EventsQueueURL)
}

func registerOutboxPublisher(lc fx.Lifecycle, uow ports.UnitOfWork, outbox ports.OutboxRepository, publisher ports.EventPublisher, logger *zap.Logger) {
	worker := outboxpublisher.New(uow, outbox, publisher, outboxpublisher.Config{}, logger)

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			worker.Start()
			logger.Info("outbox publisher started")
			return nil
		},
		OnStop: func(ctx context.Context) error {
			return worker.Stop(ctx)
		},
	})
}

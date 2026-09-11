// Package sqsconsumer implements the at-least-once SQS consumer for
// wager-transactions.fifo. It normalizes each message into the same
// processwagertransaction.Request shape the HTTP handler builds and calls
// the identical Handle — there is exactly one place, shared by both
// transports, that decides idempotency, reference resolution and the
// financial rule per kind.
package sqsconsumer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.uber.org/zap"

	"github.com/beaglexv/backend-challenge-go/internal/application/processwagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/platform/metrics"
)

// Config tunes the consumer's polling and concurrency behavior. Zero
// values are replaced with the documented defaults by New.
type Config struct {
	QueueURL string

	// MaxMessages caps how many messages a single ReceiveMessage call
	// returns (SQS itself caps this at 10). Default 10.
	MaxMessages int32
	// WaitTimeSeconds is the long-poll duration; 0 would mean short
	// polling (wasteful, noisy). Default 10.
	WaitTimeSeconds int32
	// VisibilityTimeout must comfortably exceed how long processing one
	// message normally takes — if exceeded, SQS makes the message visible
	// again even though a handler is still (or was) working on it, which
	// is the exact safety net a stuck/killed consumer relies on for safe
	// redelivery. Default 30s.
	VisibilityTimeout int32
	// Concurrency bounds how many messages this consumer processes at
	// once. Different wallets can and should proceed in parallel — the
	// per-wallet serialization the challenge requires comes from
	// GetForUpdate's row lock plus, in production, each FIFO MessageGroupId
	// (recommended: walletId) only ever having one in-flight message
	// across all consumers. Default 10.
	Concurrency int
	// MessageProcessingTimeout bounds a single message's handling — a
	// defensive ceiling against one stuck message consuming a worker slot
	// forever. Default 30s.
	MessageProcessingTimeout time.Duration
}

func (c Config) withDefaults() Config {
	if c.MaxMessages <= 0 {
		c.MaxMessages = 10
	}
	if c.WaitTimeSeconds <= 0 {
		c.WaitTimeSeconds = 10
	}
	if c.VisibilityTimeout <= 0 {
		c.VisibilityTimeout = 30
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 10
	}
	if c.MessageProcessingTimeout <= 0 {
		c.MessageProcessingTimeout = 30 * time.Second
	}
	return c
}

// sqsAPI is the subset of *sqs.Client the consumer needs — narrow enough
// to fake in tests without a real SQS/LocalStack endpoint.
type sqsAPI interface {
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

// Consumer polls wager-transactions.fifo and processes each message via
// processwagertransaction.Service.Handle.
type Consumer struct {
	client    sqsAPI
	cfg       Config
	processor *processwagertransaction.Service
	metrics   *metrics.Metrics
	logger    *zap.Logger

	cancelPoll context.CancelFunc
	wg         sync.WaitGroup
}

func New(client *sqs.Client, cfg Config, processor *processwagertransaction.Service, m *metrics.Metrics, logger *zap.Logger) *Consumer {
	return &Consumer{client: client, cfg: cfg.withDefaults(), processor: processor, metrics: m, logger: logger}
}

// Start launches the poll loop in the background and returns immediately.
// Must only be called once.
func (c *Consumer) Start() {
	pollCtx, cancel := context.WithCancel(context.Background())
	c.cancelPoll = cancel
	go c.pollLoop(pollCtx)
}

// Stop signals the poll loop to stop pulling new messages immediately,
// then waits — bounded by ctx — for in-flight message handlers to finish.
// If ctx is done first, Stop returns anyway: those handlers keep running
// in the background for whatever remains of their own
// MessageProcessingTimeout, and if the process exits before they finish,
// the messages they were working become visible again once their SQS
// visibility timeout elapses — safe redelivery, never a lost or duplicated
// movement, exactly the fallback the challenge asks for.
func (c *Consumer) Stop(ctx context.Context) error {
	if c.cancelPoll != nil {
		c.cancelPoll()
	}

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		c.logger.Warn("sqsconsumer: shutdown grace period elapsed with handlers still in flight; their messages will be safely redelivered once visibility timeout expires")
	}
	return nil
}

func (c *Consumer) pollLoop(pollCtx context.Context) {
	sem := make(chan struct{}, c.cfg.Concurrency)
	for {
		if pollCtx.Err() != nil {
			return
		}

		out, err := c.client.ReceiveMessage(pollCtx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(c.cfg.QueueURL),
			MaxNumberOfMessages: c.cfg.MaxMessages,
			WaitTimeSeconds:     c.cfg.WaitTimeSeconds,
			VisibilityTimeout:   c.cfg.VisibilityTimeout,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameMessageGroupId,
			},
		})
		if err != nil {
			if pollCtx.Err() != nil {
				return
			}
			c.logger.Error("sqsconsumer: receive failed", zap.Error(err))
			continue
		}

		for _, msg := range out.Messages {
			msg := msg
			sem <- struct{}{}
			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				defer func() { <-sem }()
				handleCtx, cancel := context.WithTimeout(context.Background(), c.cfg.MessageProcessingTimeout)
				defer cancel()
				c.handleMessage(handleCtx, msg)
			}()
		}
	}
}

func (c *Consumer) handleMessage(ctx context.Context, msg types.Message) {
	body := aws.ToString(msg.Body)
	sum := sha256.Sum256([]byte(body))
	hash := hex.EncodeToString(sum[:])

	var env envelope
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		c.metrics.RecordRetry(ctx, metrics.RetryKindSQSMessage)
		c.logger.Error("sqsconsumer: malformed message body, leaving for redrive",
			zap.Error(err), zap.String("sqsMessageId", aws.ToString(msg.MessageId)))
		return
	}

	req, err := env.toRequest(hash)
	if err != nil {
		c.metrics.RecordRetry(ctx, metrics.RetryKindSQSMessage)
		c.logger.Error("sqsconsumer: invalid envelope, leaving for redrive",
			zap.Error(err), zap.String("sqsMessageId", aws.ToString(msg.MessageId)))
		return
	}

	start := time.Now()
	result, err := c.processor.Handle(ctx, req)
	c.metrics.RecordProcessingDuration(ctx, metrics.TransportSQS, time.Since(start))
	if err != nil {
		c.metrics.RecordRetry(ctx, metrics.RetryKindSQSMessage)
		switch {
		case errors.Is(err, processwagertransaction.ErrIdempotencyConflict):
			c.metrics.RecordConcurrencyConflict(ctx, metrics.ConflictReasonIdempotencyConflict)
		case errors.Is(err, processwagertransaction.ErrExternalIDReused):
			c.metrics.RecordConcurrencyConflict(ctx, metrics.ConflictReasonExternalIDReused)
		}
		if errors.Is(err, processwagertransaction.ErrInboxHashMismatch) {
			c.logger.Error("sqsconsumer: redelivered message content changed under the same messageId, leaving for redrive",
				zap.String("messageId", env.MessageID))
			return
		}
		c.logger.Error("sqsconsumer: processing failed, leaving message for redrive",
			zap.Error(err), zap.String("messageId", env.MessageID))
		return
	}
	c.metrics.RecordWagerTransaction(ctx, string(result.Status), metrics.TransportSQS, result.IdempotentReplay)

	c.logger.Info("sqsconsumer: processed wager transaction",
		zap.String("correlationId", req.CorrelationID),
		zap.String("messageId", env.MessageID),
		zap.String("transactionId", result.TransactionID.String()),
		zap.String("walletId", req.WalletID.String()),
		zap.String("providerId", req.ProviderID),
		zap.String("status", string(result.Status)),
		zap.Bool("idempotentReplay", result.IdempotentReplay),
	)

	// Delete only after Handle's transaction committed durably — a
	// PROCESSED, REJECTED or PENDING_REFERENCE result are all equally
	// terminal for this *message* (Fase 8's inbox/PENDING_REFERENCE
	// special case covers the last one): the outcome is persisted, so
	// there is nothing left for a redelivery of this exact message to do.
	if _, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.cfg.QueueURL),
		ReceiptHandle: msg.ReceiptHandle,
	}); err != nil {
		// Safe even so: a redelivery of an already-PROCESSED message
		// replays the persisted idempotent result instead of reapplying
		// anything.
		c.logger.Error("sqsconsumer: failed to delete processed message (safe — redelivery will replay the idempotent result)",
			zap.Error(err), zap.String("messageId", env.MessageID))
	}
}

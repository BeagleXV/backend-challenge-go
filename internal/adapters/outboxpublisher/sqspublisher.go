// Package outboxpublisher implements the transactional outbox's delivery
// side: an EventPublisher that sends to SQS, and the worker that claims
// pending outbox rows and drives them through it.
package outboxpublisher

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
)

// SQSPublisher implements ports.EventPublisher by sending to a single SQS
// FIFO queue (wager-events.fifo). MessageGroupId is the aggregateId — every
// event about the same wallet/transaction stays in order relative to each
// other, matching the same per-aggregate ordering convention
// wager-transactions.fifo uses (Fase 9) — and MessageDeduplicationId is
// the eventId, so a republish after a crash between "SQS accepted it" and
// "MarkPublished committed" (Fase 11's second required interruption
// scenario) lands as a no-op within SQS's own dedup window rather than a
// guaranteed-fresh delivery. That window is a bonus, not the guarantee:
// the outbox's own published_at check is what actually prevents this
// worker from resending an already-confirmed event once its own state is
// consistent again, and a consumer of wager-events.fifo must still
// tolerate a duplicate delivery by eventId — the challenge accepts either
// "no duplicate" or "consumer tolerates duplicate by eventId", and this
// system relies on the latter for the true at-least-once edge case.
type SQSPublisher struct {
	client   *sqs.Client
	queueURL string
}

func NewSQSPublisher(client *sqs.Client, queueURL string) *SQSPublisher {
	return &SQSPublisher{client: client, queueURL: queueURL}
}

func (p *SQSPublisher) Publish(ctx context.Context, eventID, aggregateID uuid.UUID, eventType string, payload []byte) error {
	_, err := p.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(p.queueURL),
		MessageBody:            aws.String(string(payload)),
		MessageGroupId:         aws.String(aggregateID.String()),
		MessageDeduplicationId: aws.String(eventID.String()),
	})
	if err != nil {
		return fmt.Errorf("outboxpublisher: send %s (event %s): %w", eventType, eventID, err)
	}
	return nil
}

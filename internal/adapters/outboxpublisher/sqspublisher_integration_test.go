//go:build integration

// Integration test against a real, ephemeral LocalStack container
// (testcontainers-go) — not a mock SQS client. It proves SQSPublisher
// actually delivers to a real FIFO queue with the MessageGroupId/
// MessageDeduplicationId attributes it claims to set, and that
// republishing the same eventId is accepted as a duplicate rather than
// rejected — the two things a unit test against a fake client cannot
// prove.
package outboxpublisher_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	tclocalstack "github.com/testcontainers/testcontainers-go/modules/localstack"

	"github.com/beaglexv/backend-challenge-go/internal/adapters/outboxpublisher"
)

// testSQSClient starts LocalStack, provisions a FIFO queue named like
// wager-events.fifo, and returns a client pointed at it plus the queue's
// URL.
func testSQSClient(t *testing.T) (*sqs.Client, string) {
	t.Helper()
	ctx := context.Background()

	container, err := tclocalstack.Run(ctx, "localstack/localstack:3")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, container.Terminate(context.Background()))
	})

	endpoint, err := container.PortEndpoint(ctx, "4566/tcp", "http")
	require.NoError(t, err)

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("wager-events-test.fifo"),
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "false",
		},
	})
	require.NoError(t, err)

	return client, *out.QueueUrl
}

func TestSQSPublisher_Publish_DeliversWithGroupAndDeduplicationId(t *testing.T) {
	client, queueURL := testSQSClient(t)
	ctx := context.Background()

	pub := outboxpublisher.NewSQSPublisher(client, queueURL)

	eventID := uuid.New()
	aggregateID := uuid.New()
	require.NoError(t, pub.Publish(ctx, eventID, aggregateID, "WagerTransactionProcessed", []byte(`{"hello":"world"}`)))

	out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     5,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameMessageGroupId,
			types.MessageSystemAttributeNameMessageDeduplicationId,
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Messages, 1)

	msg := out.Messages[0]
	require.JSONEq(t, `{"hello":"world"}`, aws.ToString(msg.Body))
	require.Equal(t, aggregateID.String(), msg.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)])
	require.Equal(t, eventID.String(), msg.Attributes[string(types.MessageSystemAttributeNameMessageDeduplicationId)])
}

func TestSQSPublisher_Republish_SameEventId_AcceptedAsDuplicate(t *testing.T) {
	client, queueURL := testSQSClient(t)
	ctx := context.Background()

	pub := outboxpublisher.NewSQSPublisher(client, queueURL)

	eventID := uuid.New()
	aggregateID := uuid.New()

	// First publish: as if the original commit->publish path succeeded.
	require.NoError(t, pub.Publish(ctx, eventID, aggregateID, "WagerTransactionProcessed", []byte(`{"seq":1}`)))

	// Drain it, simulating the consumer having already seen it.
	drained, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 1, WaitTimeSeconds: 5,
	})
	require.NoError(t, err)
	require.Len(t, drained.Messages, 1)

	// Republish under the exact same eventId — the "interruption between
	// publication and confirmation" scenario: this call must not error
	// (SQS FIFO accepts it as a duplicate delivery within its own
	// dedup window), matching the challenge's "republicação preserva
	// eventId" requirement.
	require.NoError(t, pub.Publish(ctx, eventID, aggregateID, "WagerTransactionProcessed", []byte(`{"seq":1}`)))

	// Within SQS FIFO's own dedup window, the duplicate MessageDeduplicationId
	// means no second message is actually delivered — a bonus consistency
	// check, not this system's primary duplicate-prevention mechanism
	// (that's the outbox's published_at check, which stops this worker
	// from calling Publish again once its own state is consistent).
	redelivered, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 1, WaitTimeSeconds: 2,
	})
	require.NoError(t, err)
	require.Empty(t, redelivered.Messages, "SQS FIFO's own dedup window must have absorbed the same-eventId republish")
}

#!/bin/sh
# Runs automatically once LocalStack's SQS service is ready (mounted into
# /etc/localstack/init/ready.d/). Provisions wager-transactions-dlq.fifo
# and wager-transactions.fifo with a redrive policy pointing the second at
# the first — maxReceiveCount=5, so a message failing processing five
# times lands in the DLQ via SQS's own mechanism, never manual consumer
# logic. Also provisions wager-events.fifo, the outbox publisher's
# destination — content-based dedup off, since the publisher always sets
# an explicit MessageDeduplicationId (the eventId) itself.
set -eu

awslocal sqs create-queue \
  --queue-name wager-transactions-dlq.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=false

DLQ_URL=$(awslocal sqs get-queue-url --queue-name wager-transactions-dlq.fifo --query QueueUrl --output text)
DLQ_ARN=$(awslocal sqs get-queue-attributes --queue-url "$DLQ_URL" --attribute-names QueueArn --query Attributes.QueueArn --output text)

ATTRIBUTES=$(printf '{"FifoQueue":"true","ContentBasedDeduplication":"false","VisibilityTimeout":"30","RedrivePolicy":"{\\"deadLetterTargetArn\\":\\"%s\\",\\"maxReceiveCount\\":\\"5\\"}"}' "$DLQ_ARN")

awslocal sqs create-queue \
  --queue-name wager-transactions.fifo \
  --attributes "$ATTRIBUTES"

awslocal sqs create-queue \
  --queue-name wager-events.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=false

echo "wager-transactions.fifo, wager-transactions-dlq.fifo (redrive maxReceiveCount=5) and wager-events.fifo provisioned"

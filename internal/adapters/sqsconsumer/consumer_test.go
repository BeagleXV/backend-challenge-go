package sqsconsumer

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/beaglexv/backend-challenge-go/internal/application/apptest"
	"github.com/beaglexv/backend-challenge-go/internal/application/processwagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wallet"
	"github.com/beaglexv/backend-challenge-go/internal/platform/metrics"
)

// fakeSQS is a minimal, deterministic stand-in for *sqs.Client: one batch
// of messages is served on the first ReceiveMessage call, nothing on
// every call after — enough to drive the consumer through exactly one
// poll cycle per test without a real SQS/LocalStack endpoint. Real
// queue/redrive behavior (this is Fase 9's other deliverable) is verified
// manually against LocalStack, documented in ARCHITECTURE.md.
type fakeSQS struct {
	mu       sync.Mutex
	batches  [][]types.Message
	deleted  []string
	received int
}

func (f *fakeSQS) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.received >= len(f.batches) {
		return &sqs.ReceiveMessageOutput{}, nil
	}
	batch := f.batches[f.received]
	f.received++
	return &sqs.ReceiveMessageOutput{Messages: batch}, nil
}

func (f *fakeSQS) DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, aws.ToString(in.ReceiptHandle))
	return &sqs.DeleteMessageOutput{}, nil
}

func (f *fakeSQS) deletedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.deleted)
}

func mustMoney(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.New(amount, money.BRL)
	require.NoError(t, err)
	return m
}

func newTestConsumer(t *testing.T, sqsClient sqsAPI) (*Consumer, *apptest.WalletRepository, uuid.UUID) {
	t.Helper()
	wallets := apptest.NewWalletRepository()
	txs := apptest.NewWagerTransactionRepository()
	ledgers := apptest.NewLedgerRepository()
	inbox := apptest.NewInboxRepository()
	outbox := apptest.NewOutboxRepository()
	clock := apptest.FixedClock{T: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	ids := &apptest.SequentialIDs{}

	w, err := wallet.New(wallet.NewParams{
		ID: uuid.New(), PlayerID: uuid.New(), Currency: money.BRL,
		InitialBalance: mustMoney(t, "100.00"), Now: time.Now(),
	})
	require.NoError(t, err)
	require.NoError(t, wallets.Insert(context.Background(), w))

	processor := processwagertransaction.New(apptest.NoopUnitOfWork{}, wallets, txs, ledgers, inbox, outbox, clock, ids)

	c := &Consumer{
		client:    sqsClient,
		cfg:       Config{QueueURL: "http://localhost:4566/000000000000/wager-transactions.fifo"}.withDefaults(),
		processor: processor,
		metrics:   metrics.NewNoop(),
		logger:    zaptest.NewLogger(t),
	}
	return c, wallets, w.ID()
}

func messageFor(t *testing.T, receiptHandle, sqsMessageID string, env envelope) types.Message {
	t.Helper()
	body, err := json.Marshal(env)
	require.NoError(t, err)
	return types.Message{
		MessageId:     aws.String(sqsMessageID),
		ReceiptHandle: aws.String(receiptHandle),
		Body:          aws.String(string(body)),
	}
}

func betEnvelope(walletID uuid.UUID, messageID, externalTxID string) envelope {
	return envelope{
		MessageID:  messageID,
		Type:       "WagerTransactionRequested",
		OccurredAt: time.Now().UTC(),
		Data: envelopeData{
			ProviderID:            "provider-a",
			ExternalTransactionID: externalTxID,
			IdempotencyKey:        "provider-a:" + externalTxID,
			PlayerID:              uuid.New(),
			WalletID:              walletID,
			RoundID:               "round-1",
			GameID:                "game-1",
			Kind:                  "BET",
			Money:                 money.Money{},
		},
	}
}

func TestHandleMessage_ProcessesAndDeletesOnSuccess(t *testing.T) {
	fake := &fakeSQS{}
	c, wallets, walletID := newTestConsumer(t, fake)

	env := betEnvelope(walletID, "msg-1", "tx-1")
	env.Data.Money = mustMoney(t, "25.00")
	msg := messageFor(t, "receipt-1", "sqs-1", env)

	c.handleMessage(context.Background(), msg)

	assert.Equal(t, 1, fake.deletedCount(), "a successfully processed message must be deleted")
	w, err := wallets.GetByID(context.Background(), walletID)
	require.NoError(t, err)
	assert.Equal(t, "75.00", w.Balance().String())
}

func TestHandleMessage_MalformedBody_NotDeleted(t *testing.T) {
	fake := &fakeSQS{}
	c, _, _ := newTestConsumer(t, fake)

	msg := types.Message{
		MessageId:     aws.String("sqs-bad"),
		ReceiptHandle: aws.String("receipt-bad"),
		Body:          aws.String("not json at all"),
	}

	c.handleMessage(context.Background(), msg)

	assert.Zero(t, fake.deletedCount(), "a malformed message must be left for SQS's own redrive policy")
}

func TestHandleMessage_MissingMessageID_NotDeleted(t *testing.T) {
	fake := &fakeSQS{}
	c, _, walletID := newTestConsumer(t, fake)

	env := betEnvelope(walletID, "", "tx-1")
	env.Data.Money = mustMoney(t, "25.00")
	msg := messageFor(t, "receipt-1", "sqs-1", env)

	c.handleMessage(context.Background(), msg)

	assert.Zero(t, fake.deletedCount())
}

func TestHandleMessage_ProcessingError_NotDeleted(t *testing.T) {
	fake := &fakeSQS{}
	c, _, _ := newTestConsumer(t, fake)

	// A wallet id that doesn't exist makes Handle return ErrWalletNotFound.
	env := betEnvelope(uuid.New(), "msg-1", "tx-1")
	env.Data.Money = mustMoney(t, "25.00")
	msg := messageFor(t, "receipt-1", "sqs-1", env)

	c.handleMessage(context.Background(), msg)

	assert.Zero(t, fake.deletedCount(), "processing failure must leave the message for redrive, not delete it")
}

func TestHandleMessage_RedeliveryOfProcessedMessage_IsIdempotentAndDeleted(t *testing.T) {
	fake := &fakeSQS{}
	c, wallets, walletID := newTestConsumer(t, fake)

	env := betEnvelope(walletID, "msg-1", "tx-1")
	env.Data.Money = mustMoney(t, "25.00")
	msg := messageFor(t, "receipt-1", "sqs-1", env)

	c.handleMessage(context.Background(), msg)
	c.handleMessage(context.Background(), msg) // exact same message redelivered

	assert.Equal(t, 2, fake.deletedCount(), "both the original delivery and the redelivery are terminal for this message")
	w, err := wallets.GetByID(context.Background(), walletID)
	require.NoError(t, err)
	assert.Equal(t, "75.00", w.Balance().String(), "redelivery must not move the wallet a second time")
}

func TestHandleMessage_HashMismatchOnRedelivery_NotDeleted(t *testing.T) {
	fake := &fakeSQS{}
	c, _, walletID := newTestConsumer(t, fake)

	env := betEnvelope(walletID, "msg-1", "tx-1")
	env.Data.Money = mustMoney(t, "25.00")
	first := messageFor(t, "receipt-1", "sqs-1", env)
	c.handleMessage(context.Background(), first)
	require.Equal(t, 1, fake.deletedCount())

	// Same messageId, but the body changed underneath it — must never be
	// treated as a normal replay.
	env.Data.ExternalTransactionID = "tx-2"
	tampered := messageFor(t, "receipt-2", "sqs-1", env)
	c.handleMessage(context.Background(), tampered)

	assert.Equal(t, 1, fake.deletedCount(), "a hash-mismatched redelivery must not be deleted")
}

func TestStart_Stop_GracefulShutdown(t *testing.T) {
	fake := &fakeSQS{}
	c, wallets, walletID := newTestConsumer(t, fake)

	env := betEnvelope(walletID, "msg-1", "tx-1")
	env.Data.Money = mustMoney(t, "25.00")
	msg := messageFor(t, "receipt-1", "sqs-1", env)
	fake.batches = [][]types.Message{{msg}}

	c.Start()

	require.Eventually(t, func() bool {
		return fake.deletedCount() == 1
	}, 2*time.Second, 10*time.Millisecond, "the seeded message must be processed and deleted")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, c.Stop(ctx))

	w, err := wallets.GetByID(context.Background(), walletID)
	require.NoError(t, err)
	assert.Equal(t, "75.00", w.Balance().String())
}

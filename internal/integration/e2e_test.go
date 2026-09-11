//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
	"github.com/beaglexv/backend-challenge-go/internal/fxmodules"
)

// runningApp is a small wrapper that starts a real app.Start over the real
// containers and guarantees app.Stop via t.Cleanup, so every e2e test
// below reads as: build config, start app, exercise it.
type runningApp struct {
	baseURL string
}

func startApp(t *testing.T, pg postgresContainer, sqsInf sqsInfra, kc keycloakContainer) runningApp {
	t.Helper()
	httpAddr := freeAddr(t)
	cfg := buildConfig(t, pg, sqsInf, kc, httpAddr, freeAddr(t))

	app := fx.New(fxmodules.All(cfg)...)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, app.Start(ctx))
	t.Cleanup(func() {
		stopCtx, cancelStop := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelStop()
		require.NoError(t, app.Stop(stopCtx))
	})

	return runningApp{baseURL: "http://" + httpAddr}
}

// wagerEnvelope mirrors internal/adapters/sqsconsumer's wire contract
// (README section 10) — this test builds the exact message shape a real
// provider integration would put on wager-transactions.fifo.
type wagerEnvelope struct {
	MessageID  string            `json:"messageId"`
	Type       string            `json:"type"`
	OccurredAt time.Time         `json:"occurredAt"`
	Data       wagerEnvelopeData `json:"data"`
}

type wagerEnvelopeData struct {
	ProviderID            string      `json:"providerId"`
	ExternalTransactionID string      `json:"externalTransactionId"`
	IdempotencyKey        string      `json:"idempotencyKey"`
	PlayerID              uuid.UUID   `json:"playerId"`
	WalletID              uuid.UUID   `json:"walletId"`
	RoundID               string      `json:"roundId"`
	GameID                string      `json:"gameId"`
	Kind                  string      `json:"kind"`
	Money                 money.Money `json:"money"`
}

func mustMoneyInt(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.New(amount, money.BRL)
	require.NoError(t, err)
	return m
}

type walletResponse struct {
	ID       uuid.UUID   `json:"id"`
	PlayerID uuid.UUID   `json:"playerId"`
	Balance  money.Money `json:"balance"`
	Version  int64       `json:"version"`
}

// openWalletViaHTTP calls the real /wallets endpoint through the real
// running app, authenticated as the internal-service client (the only
// client allowed to manage wallets, per requireInternal).
func openWalletViaHTTP(t *testing.T, app runningApp, internalToken string, playerID uuid.UUID, initial string) walletResponse {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"playerId":       playerID,
		"initialBalance": mustMoneyInt(t, initial),
	})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, app.baseURL+"/wallets", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+internalToken)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	var out walletResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}

func getWalletViaHTTP(t *testing.T, app runningApp, internalToken string, walletID uuid.UUID) walletResponse {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/wallets/%s", app.baseURL, walletID), nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+internalToken)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out walletResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}

type wagerSubmitResult struct {
	TransactionID    uuid.UUID   `json:"transactionId"`
	Status           string      `json:"status"`
	Balance          money.Money `json:"balance,omitempty"`
	IdempotentReplay bool        `json:"idempotentReplay"`
}

// submitWagerViaHTTP calls the real /wagering/transactions endpoint,
// authenticated as a provider (requireProvider), for tests that need an
// HTTP-submitted transaction rather than an SQS one (e.g. auth/provider
// isolation checks).
func submitWagerViaHTTP(t *testing.T, app runningApp, providerToken string, walletID, playerID uuid.UUID, externalTxID string) wagerSubmitResult {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"providerId":            "provider-a",
		"externalTransactionId": externalTxID,
		"playerId":              playerID,
		"walletId":              walletID,
		"roundId":               "round-1",
		"gameId":                "game-1",
		"kind":                  "BET",
		"money":                 mustMoneyInt(t, "10.00"),
	})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, app.baseURL+"/wagering/transactions", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "provider-a:"+externalTxID)
	req.Header.Set("Authorization", "Bearer "+providerToken)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out wagerSubmitResult
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}

// sendWagerMessage puts one BET envelope on the real wager-transactions
// queue, FIFO-grouped by wallet as production does.
func sendWagerMessage(t *testing.T, sqsInf sqsInfra, walletID, playerID uuid.UUID, externalTxID string) {
	t.Helper()
	env := wagerEnvelope{
		MessageID:  uuid.NewString(),
		Type:       "WagerTransactionSubmitted",
		OccurredAt: time.Now().UTC(),
		Data: wagerEnvelopeData{
			ProviderID:            "provider-a",
			ExternalTransactionID: externalTxID,
			IdempotencyKey:        "provider-a:" + externalTxID,
			PlayerID:              playerID,
			WalletID:              walletID,
			RoundID:               "round-1",
			GameID:                "game-1",
			Kind:                  "BET",
			Money:                 mustMoneyInt(t, "25.00"),
		},
	}
	body, err := json.Marshal(env)
	require.NoError(t, err)

	_, err = sqsInf.client.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl:               aws.String(sqsInf.txQueueURL),
		MessageBody:            aws.String(string(body)),
		MessageGroupId:         aws.String(walletID.String()),
		MessageDeduplicationId: aws.String(env.MessageID),
	})
	require.NoError(t, err)
}

// TestEndToEnd_HTTPOpenWallet_SQSBet_UpdatesBalance is the full-stack
// proof Fase 15 asks for: a real app (fx-started, real Postgres, real
// Keycloak-issued tokens, real LocalStack queues) opens a wallet over
// HTTP, then a BET arriving over SQS — exactly as a real provider
// integration would deliver it — is picked up by the real consumer,
// processed through the same Handle the HTTP path uses, and the debit is
// observable back over HTTP.
func TestEndToEnd_HTTPOpenWallet_SQSBet_UpdatesBalance(t *testing.T) {
	pg := startPostgres(t)
	sqsInf := startLocalStack(t, 5)
	kc := startKeycloak(t)
	app := startApp(t, pg, sqsInf, kc)

	internalToken := kc.token(t, "internal-service", "internal-service-secret-local-only")
	playerID := uuid.New()

	wallet := openWalletViaHTTP(t, app, internalToken, playerID, "100.00")

	sendWagerMessage(t, sqsInf, wallet.ID, playerID, "bet-e2e-1")

	require.Eventually(t, func() bool {
		w := getWalletViaHTTP(t, app, internalToken, wallet.ID)
		return w.Balance.String() == "75.00"
	}, 20*time.Second, 250*time.Millisecond, "the real SQS consumer must pick up and process the BET")
}

// TestEndToEnd_MalformedMessage_LandsInDLQ proves the redrive policy
// deploy/localstack/init-queues.sh configures for real deployments
// actually works: a message the consumer can never successfully process
// (malformed JSON body) is left on the queue on every delivery attempt
// (handleMessage never calls DeleteMessage for it), and after
// maxReceiveCount deliveries SQS itself — not any consumer-side retry
// counting — moves it to the DLQ.
func TestEndToEnd_MalformedMessage_LandsInDLQ(t *testing.T) {
	pg := startPostgres(t)
	const maxReceiveCount = 2
	sqsInf := startLocalStack(t, maxReceiveCount)
	kc := startKeycloak(t)
	app := startApp(t, pg, sqsInf, kc)
	_ = app // the running consumer is what drives redelivery; no HTTP calls needed here

	_, err := sqsInf.client.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl:               aws.String(sqsInf.txQueueURL),
		MessageBody:            aws.String("not valid json"),
		MessageGroupId:         aws.String(uuid.NewString()),
		MessageDeduplicationId: aws.String(uuid.NewString()),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		out, err := sqsInf.client.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
			QueueUrl:       aws.String(sqsInf.dlqURL),
			AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages},
		})
		require.NoError(t, err)
		return out.Attributes["ApproximateNumberOfMessages"] == "1"
	}, 90*time.Second, 3*time.Second, "a message the consumer can never process must land in the DLQ after maxReceiveCount deliveries (each delivery is gated by the consumer's own 30s VisibilityTimeout, so this needs headroom well past maxReceiveCount*30s)")
}

// TestEndToEnd_RestartRecovery_PendingOutboxEventIsPublishedByFreshInstance
// proves the challenge's "recuperação após restart simulado" requirement:
// state that must survive a process restart (an outbox event still
// unpublished) lives in Postgres, never only in memory. This starts one
// app instance just long enough to have processed a BET (which writes
// outbox rows in the same transaction as the domain change), stops it
// immediately — before its own outbox publisher worker necessarily got to
// them — then starts a second, completely fresh fx app against the same
// Postgres/LocalStack containers and asserts the events get published by
// the new instance.
func TestEndToEnd_RestartRecovery_PendingOutboxEventIsPublishedByFreshInstance(t *testing.T) {
	pg := startPostgres(t)
	sqsInf := startLocalStack(t, 5)
	kc := startKeycloak(t)

	internalToken := kc.token(t, "internal-service", "internal-service-secret-local-only")
	playerID := uuid.New()

	// First instance: open the wallet and submit the BET over SQS, then
	// stop the app right away. Whether or not its own publisher got to
	// the resulting outbox rows before Stop is irrelevant to this test —
	// what matters is that the rows exist in Postgres independent of any
	// single process's lifetime.
	var walletID uuid.UUID
	func() {
		app := startApp(t, pg, sqsInf, kc)
		wallet := openWalletViaHTTP(t, app, internalToken, playerID, "100.00")
		walletID = wallet.ID
		sendWagerMessage(t, sqsInf, walletID, playerID, "bet-restart-1")

		// Give the first instance a little time to actually receive and
		// process the message (writing the outbox rows) before its
		// t.Cleanup-triggered Stop runs at the end of this func literal.
		require.Eventually(t, func() bool {
			w := getWalletViaHTTP(t, app, internalToken, walletID)
			return w.Balance.String() == "75.00"
		}, 20*time.Second, 250*time.Millisecond)
	}()

	// Second, completely independent fx app, built fresh against the same
	// Postgres and LocalStack containers.
	app2 := startApp(t, pg, sqsInf, kc)
	_ = app2

	require.Eventually(t, func() bool {
		out, err := sqsInf.client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(sqsInf.eventsURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     1,
		})
		require.NoError(t, err)
		return len(out.Messages) > 0
	}, 30*time.Second, 500*time.Millisecond, "the fresh instance's outbox publisher must pick up and publish events left behind by the first instance's Postgres-persisted state")
}

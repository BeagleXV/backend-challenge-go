//go:build integration

// Fase 16 — the 8 mandatory concurrency/recovery scenarios from README
// section 13. Several are already proven elsewhere and are not duplicated
// here (see the scope note in PLANO.md's Fase 16 entry for the full
// mapping): scenario 1 (50 identical concurrent BETs -> single debit) in
// postgres_integration_test.go's
// TestProcessWagerTransaction_SameBetSentFiftyTimesInParallel_SingleDebit;
// scenario 5 (consumer redelivery after commit, before message deletion)
// at the unit level in sqsconsumer's
// TestHandleMessage_RedeliveryOfProcessedMessage_IsIdempotentAndDeleted;
// scenario 6 (two outbox publishers racing the same batch) in
// TestOutboxRepository_ClaimBatch_TwoPublishersDoNotDoubleClaim; scenario 7
// (REFUND/ROLLBACK arriving before its reference) across
// resolvependingreference's TestResolve_ReferenceArrivedLate_NowProcesses
// and referenceworker's expiry tests, plus
// TestWagerTransactionRepository_PendingReferenceRetryLifecycle against
// real Postgres. This file covers what remained: scenario 2 (two BETs
// disputing a balance too small for both), scenario 3 (distinct wallets
// are not serialized against each other) and scenario 4 (>=3 real,
// independent OS processes of the actual binary, not goroutines).
package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
)

type reconciliationResult struct {
	Consistent bool `json:"consistent"`
}

// reconcileViaHTTP calls the real /wallets/{id}/reconciliation endpoint —
// the same check every scenario below closes with: stored balance must
// equal sum(credits) - sum(debits) over the wallet's own ledger.
func reconcileViaHTTP(t *testing.T, baseURL, internalToken string, walletID uuid.UUID) reconciliationResult {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/wallets/%s/reconciliation", baseURL, walletID), nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+internalToken)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out reconciliationResult
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}

func listLedgerViaHTTP(t *testing.T, baseURL, internalToken string, walletID uuid.UUID) []ledgerEntryResponse {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/wallets/%s/ledger", baseURL, walletID), nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+internalToken)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out ledgerPageResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out.Entries
}

type ledgerEntryResponse struct {
	Direction     string      `json:"direction"`
	Amount        money.Money `json:"amount"`
	BalanceBefore money.Money `json:"balanceBefore"`
	BalanceAfter  money.Money `json:"balanceAfter"`
}

type ledgerPageResponse struct {
	Entries []ledgerEntryResponse `json:"entries"`
}

// submitBetViaHTTP is submitWagerViaHTTP generalized over the bet's amount
// and idempotency key, needed to fire two *different* bets (as opposed to
// the same one twice) against the same wallet concurrently.
func submitBetViaHTTP(t *testing.T, baseURL, providerToken string, walletID, playerID uuid.UUID, externalTxID, amount string) *http.Response {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"providerId":            "provider-a",
		"externalTransactionId": externalTxID,
		"playerId":              playerID,
		"walletId":              walletID,
		"roundId":               "round-1",
		"gameId":                "game-1",
		"kind":                  "BET",
		"money":                 mustMoneyInt(t, amount),
	})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, baseURL+"/wagering/transactions", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "provider-a:"+externalTxID)
	req.Header.Set("Authorization", "Bearer "+providerToken)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

// TestConcurrency_TwoBetsDisputingInsufficientBalance_OneWinsOneRejected is
// scenario 2: two 80.00 BETs against a 100.00 balance, submitted at the
// same time. Exactly one must succeed (balance 20.00, one debit ledger
// entry); the other must be rejected for insufficient balance, leaving no
// trace in the ledger. Reconciliation must report the wallet consistent
// afterwards.
func TestConcurrency_TwoBetsDisputingInsufficientBalance_OneWinsOneRejected(t *testing.T) {
	pg := startPostgres(t)
	sqsInf := startLocalStack(t, 5)
	kc := startKeycloak(t)
	app := startApp(t, pg, sqsInf, kc)

	internalToken := kc.token(t, "internal-service", "internal-service-secret-local-only")
	providerToken := kc.token(t, "provider-a", "provider-a-secret-local-only")
	playerID := uuid.New()

	wallet := openWalletViaHTTP(t, app, internalToken, playerID, "100.00")

	const attempts = 2
	statusCodes := make([]int, attempts)
	bodies := make([][]byte, attempts)

	var wg sync.WaitGroup
	var startBarrier sync.WaitGroup
	startBarrier.Add(1)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			startBarrier.Wait() // release both requests at the same instant, no sleep involved
			resp := submitBetViaHTTP(t, app.baseURL, providerToken, wallet.ID, playerID, fmt.Sprintf("bet-dispute-%d", i), "80.00")
			defer resp.Body.Close()
			statusCodes[i] = resp.StatusCode
			buf := make([]byte, 4096)
			n, _ := resp.Body.Read(buf)
			bodies[i] = buf[:n]
		}(i)
	}
	startBarrier.Done()
	wg.Wait()

	successCount, rejectedCount := 0, 0
	for i, code := range statusCodes {
		require.Contains(t, []int{http.StatusOK, http.StatusUnprocessableEntity}, code, "attempt %d: unexpected status %d, body %s", i, code, bodies[i])
		var out wagerSubmitResult
		require.NoError(t, json.Unmarshal(bodies[i], &out))
		switch out.Status {
		case "PROCESSED":
			successCount++
		case "REJECTED":
			rejectedCount++
		}
	}
	require.Equal(t, 1, successCount, "exactly one of the two disputing BETs must be PROCESSED")
	require.Equal(t, 1, rejectedCount, "exactly one of the two disputing BETs must be REJECTED for insufficient balance")

	final := getWalletViaHTTP(t, app, internalToken, wallet.ID)
	require.Equal(t, "20.00", final.Balance.String())

	entries := listLedgerViaHTTP(t, app.baseURL, internalToken, wallet.ID)
	require.Len(t, entries, 2, "the OPENING credit plus exactly one BET debit — the rejected BET must leave no ledger entry of its own")
	require.Equal(t, "20.00", entries[len(entries)-1].BalanceAfter.String())

	rec := reconcileViaHTTP(t, app.baseURL, internalToken, wallet.ID)
	require.True(t, rec.Consistent, "stored balance must equal sum(credits) - sum(debits)")
}

// TestConcurrency_DistinctWalletsProcessedSimultaneously_HTTP is scenario
// 3's HTTP-level correctness proof: N BETs against N distinct wallets,
// fired at the same instant (a WaitGroup barrier, never a sleep) through
// the real HTTP API, must all succeed independently and each wallet must
// end up individually consistent. The actual non-serialization mechanism
// (that the pessimistic lock is scoped per-wallet-row, not table-wide) is
// proven deterministically, without any wall-clock comparison, in
// internal/adapters/postgres/postgres_integration_test.go's
// TestWalletRepository_GetForUpdate_DistinctWallets_DoNotSerializeAgainstEachOther
// — a wall-clock-based version of that proof at this HTTP layer turned out
// flaky: per-request fixed overhead here (auth token verification, JSON
// (de)serialization, HTTP round trip) is an order of magnitude larger than
// the actual lock hold time, so timing comparisons at this layer carry no
// real signal either way.
func TestConcurrency_DistinctWalletsProcessedSimultaneously_HTTP(t *testing.T) {
	pg := startPostgres(t)
	sqsInf := startLocalStack(t, 5)
	kc := startKeycloak(t)
	app := startApp(t, pg, sqsInf, kc)

	internalToken := kc.token(t, "internal-service", "internal-service-secret-local-only")
	providerToken := kc.token(t, "provider-a", "provider-a-secret-local-only")

	const count = 8
	walletIDs := make([]uuid.UUID, count)
	playerIDs := make([]uuid.UUID, count)
	for i := 0; i < count; i++ {
		playerIDs[i] = uuid.New()
		walletIDs[i] = openWalletViaHTTP(t, app, internalToken, playerIDs[i], "100.00").ID
	}

	var wg sync.WaitGroup
	var startBarrier sync.WaitGroup
	startBarrier.Add(1)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			startBarrier.Wait()
			resp := submitBetViaHTTP(t, app.baseURL, providerToken, walletIDs[i], playerIDs[i], fmt.Sprintf("bet-distinct-wallets-%d", i), "10.00")
			resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode, "wallet %d", i)
		}(i)
	}
	startBarrier.Done()
	wg.Wait()

	for i, walletID := range walletIDs {
		final := getWalletViaHTTP(t, app, internalToken, walletID)
		require.Equal(t, "90.00", final.Balance.String(), "wallet %d", i)
		rec := reconcileViaHTTP(t, app.baseURL, internalToken, walletID)
		require.True(t, rec.Consistent, "wallet %d", i)
	}
}

// apiProcess is one real OS process running the actual compiled cmd/api
// binary, not a goroutine standing in for an "instance". Multiple of these
// against the same Postgres/LocalStack/Keycloak containers is what
// scenario 4 asks for: repeating the relevant scenarios with independent
// processes.
type apiProcess struct {
	cmd     *exec.Cmd
	baseURL string
}

// buildAPIBinary compiles cmd/api once per test (not once per process:
// every apiProcess below execs the same binary) into t.TempDir().
func buildAPIBinary(t *testing.T) string {
	t.Helper()
	binPath := t.TempDir() + "/api"
	cmd := exec.Command("go", "build", "-o", binPath, "./cmd/api")
	cmd.Dir = "../.."
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "building cmd/api: %s", out)
	return binPath
}

// startAPIProcess launches one real, independent OS process of the built
// binary, wired to the same shared Postgres/LocalStack/Keycloak containers
// as every other instance in the test, and waits for its real
// /health/ready to report healthy before returning.
func startAPIProcess(t *testing.T, binPath string, pg postgresContainer, sqsInf sqsInfra, kc keycloakContainer) apiProcess {
	t.Helper()
	httpAddr := freeAddr(t)
	metricsAddr := freeAddr(t)
	cfg := buildConfig(t, pg, sqsInf, kc, httpAddr, metricsAddr)

	env := append(os.Environ(),
		"APP_ENV=test",
		"APP_HTTP_ADDR="+httpAddr,
		"METRICS_ADDR="+metricsAddr,
		"APP_SHUTDOWN_TIMEOUT=5s",
		"POSTGRES_HOST="+cfg.Postgres.Host,
		"POSTGRES_PORT="+cfg.Postgres.Port,
		"POSTGRES_DB="+cfg.Postgres.DB,
		"POSTGRES_USER="+cfg.Postgres.User,
		"POSTGRES_PASSWORD="+cfg.Postgres.Password,
		"POSTGRES_SSLMODE=disable",
		"OIDC_ISSUER_URL="+kc.issuerURL,
		"OIDC_AUDIENCE=wagering-api",
		"AWS_REGION=us-east-1",
		"AWS_ACCESS_KEY_ID=test",
		"AWS_SECRET_ACCESS_KEY=test",
		"SQS_ENDPOINT="+sqsInf.endpoint,
		"SQS_WAGER_TRANSACTIONS_QUEUE_URL="+sqsInf.txQueueURL,
		"SQS_WAGER_TRANSACTIONS_DLQ_URL="+sqsInf.dlqURL,
		"SQS_EVENTS_QUEUE_URL="+sqsInf.eventsURL,
	)

	cmd := exec.Command(binPath)
	cmd.Env = env
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())

	proc := apiProcess{cmd: cmd, baseURL: "http://" + httpAddr}
	t.Cleanup(func() {
		if cmd.Process == nil {
			return
		}
		_ = cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
		}
	})

	require.Eventually(t, func() bool {
		resp, err := http.Get(proc.baseURL + "/health/ready")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 30*time.Second, 200*time.Millisecond, "process at %s must become ready", proc.baseURL)

	return proc
}

// TestConcurrency_ThreeRealProcesses_SameBetSubmittedToEachInParallel_SingleDebit
// is scenario 4: repeating the "same bet many times in parallel -> single
// debit" guarantee (scenario 1), but this time across 3 real, independent
// OS processes of the actual binary — never goroutines simulating separate
// instances — all pointed at the same shared Postgres/LocalStack/Keycloak
// containers. The same idempotency key is submitted to all 3 processes at
// once; whichever process's transaction commits first must be the only
// one that actually debits, and the other two must observe (via
// Postgres-level idempotent replay, not in-memory state private to one
// process) the exact same PROCESSED result.
func TestConcurrency_ThreeRealProcesses_SameBetSubmittedToEachInParallel_SingleDebit(t *testing.T) {
	pg := startPostgres(t)
	sqsInf := startLocalStack(t, 5)
	kc := startKeycloak(t)

	binPath := buildAPIBinary(t)
	const instanceCount = 3
	instances := make([]apiProcess, instanceCount)
	for i := range instances {
		instances[i] = startAPIProcess(t, binPath, pg, sqsInf, kc)
	}

	internalToken := kc.token(t, "internal-service", "internal-service-secret-local-only")
	providerToken := kc.token(t, "provider-a", "provider-a-secret-local-only")
	playerID := uuid.New()

	// Open the wallet through instance 0 — any instance would do, since
	// they all share the same Postgres.
	wallet := openWalletViaHTTP(t, runningApp{baseURL: instances[0].baseURL}, internalToken, playerID, "1000.00")

	results := make([]wagerSubmitResult, instanceCount)
	statusCodes := make([]int, instanceCount)
	var wg sync.WaitGroup
	var startBarrier sync.WaitGroup
	startBarrier.Add(1)
	for i := 0; i < instanceCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			startBarrier.Wait()
			resp := submitBetViaHTTP(t, instances[i].baseURL, providerToken, wallet.ID, playerID, "bet-multiprocess-1", "25.00")
			defer resp.Body.Close()
			statusCodes[i] = resp.StatusCode
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&results[i]))
		}(i)
	}
	startBarrier.Done()
	wg.Wait()

	firstTxID := results[0].TransactionID
	for i := 0; i < instanceCount; i++ {
		require.Equal(t, http.StatusOK, statusCodes[i], "instance %d", i)
		require.Equal(t, "PROCESSED", results[i].Status, "instance %d", i)
		require.Equal(t, firstTxID, results[i].TransactionID, "instance %d must resolve to the same transaction as instance 0, proving idempotency is shared via Postgres, not process-local state", i)
		require.Equal(t, "975.00", results[i].Balance.String(), "instance %d", i)
	}

	final := getWalletViaHTTP(t, runningApp{baseURL: instances[0].baseURL}, internalToken, wallet.ID)
	require.Equal(t, "975.00", final.Balance.String())

	entries := listLedgerViaHTTP(t, instances[0].baseURL, internalToken, wallet.ID)
	require.Len(t, entries, 2, "the OPENING credit plus exactly one BET debit — 3 processes submitting the same idempotency key concurrently must still produce only one debit entry")
	require.Equal(t, "975.00", entries[len(entries)-1].BalanceAfter.String())

	rec := reconcileViaHTTP(t, instances[0].baseURL, internalToken, wallet.ID)
	require.True(t, rec.Consistent)
}

// TestConcurrency_RestartRecovery_PendingReferenceResolvedByFreshInstance
// covers scenario 8's asynchronous-continuation nuance for this system:
// there is no HTTP endpoint that accepts a request and returns before
// processing it (no 202-style async acceptance), so the only state that
// legitimately continues *after* a process boundary is a REFUND/ROLLBACK
// left PENDING_REFERENCE, awaiting the reference-worker to resolve it.
// This proves that state, and the worker resolving it, survive a restart:
// one instance receives the REFUND (its reference not arrived yet, so it
// persists as PENDING_REFERENCE in Postgres) and is stopped immediately
// afterwards; a second, entirely fresh instance is then started, the
// missing BET arrives, and the second instance's own reference-worker
// picks up and resolves the still-PENDING_REFERENCE transaction left by
// the first.
func TestConcurrency_RestartRecovery_PendingReferenceResolvedByFreshInstance(t *testing.T) {
	pg := startPostgres(t)
	sqsInf := startLocalStack(t, 5)
	kc := startKeycloak(t)

	internalToken := kc.token(t, "internal-service", "internal-service-secret-local-only")
	providerToken := kc.token(t, "provider-a", "provider-a-secret-local-only")
	playerID := uuid.New()

	var walletID uuid.UUID
	func() {
		app := startApp(t, pg, sqsInf, kc)
		wallet := openWalletViaHTTP(t, app, internalToken, playerID, "100.00")
		walletID = wallet.ID

		body, err := json.Marshal(map[string]any{
			"providerId":                     "provider-a",
			"externalTransactionId":          "refund-restart-1",
			"playerId":                       playerID,
			"walletId":                       walletID,
			"roundId":                        "round-1",
			"gameId":                         "game-1",
			"kind":                           "REFUND",
			"referenceExternalTransactionId": "bet-not-arrived-yet",
			// Must match the referenced BET's amount exactly (25.00,
			// hardcoded in sendWagerMessage below) — applyReversal rejects
			// a REFUND/ROLLBACK whose amount disagrees with its reference.
			"money": mustMoneyInt(t, "25.00"),
		})
		require.NoError(t, err)
		req, err := http.NewRequest(http.MethodPost, app.baseURL+"/wagering/transactions", bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "provider-a:refund-restart-1")
		req.Header.Set("Authorization", "Bearer "+providerToken)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusAccepted, resp.StatusCode, "PENDING_REFERENCE maps to 202 — the reference has not arrived yet")

		var out wagerSubmitResult
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
		require.Equal(t, "PENDING_REFERENCE", out.Status, "the reference has not arrived yet, so this must persist as PENDING_REFERENCE rather than fail outright")
	}()
	// app.Stop already ran via t.Cleanup inside the func literal above.

	// A fresh instance: the missing BET this REFUND references now
	// arrives over SQS, then the second instance's reference-worker must
	// resolve the transaction the first instance left PENDING_REFERENCE.
	app2 := startApp(t, pg, sqsInf, kc)
	sendWagerMessage(t, sqsInf, walletID, playerID, "bet-not-arrived-yet")

	// The reference-worker isn't event-driven on the BET's arrival — it
	// polls on a fixed interval and its very first resolution attempt for
	// a freshly-PENDING_REFERENCE transaction is only scheduled 30s out
	// (pendingReferenceBackoff(1), by design: a reference that resolves
	// almost immediately shouldn't be hammered at a tight interval). The
	// wait below has to clear that fixed floor, plus the poll interval,
	// plus real headroom — it is not tuned to a tight bound.
	require.Eventually(t, func() bool {
		w := getWalletViaHTTP(t, app2, internalToken, walletID)
		// 100.00 opening, -25.00 for the BET the message above carries
		// (sendWagerMessage always submits a 25.00 BET), +25.00 credited
		// back once the REFUND resolves against it — net back to 100.00.
		return w.Balance.String() == "100.00"
	}, 90*time.Second, 2*time.Second, "the fresh instance's reference-worker must resolve the PENDING_REFERENCE REFUND left by the first instance once its reference BET arrives")

	rec := reconcileViaHTTP(t, app2.baseURL, internalToken, walletID)
	require.True(t, rec.Consistent)
}

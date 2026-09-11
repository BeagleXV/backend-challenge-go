// Package httpapi implements the HTTP transport: chi router, middlewares
// (panic recovery, correlation id, structured request logging, OIDC
// authentication and authorization) and handlers for every route in the
// contract. It depends on the application layer's use cases and read
// ports, never the other way around.
package httpapi

import (
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/beaglexv/backend-challenge-go/internal/adapters/idp"
	"github.com/beaglexv/backend-challenge-go/internal/application/openwallet"
	"github.com/beaglexv/backend-challenge-go/internal/application/ports"
	"github.com/beaglexv/backend-challenge-go/internal/application/processwagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/application/reconciliation"
	"github.com/beaglexv/backend-challenge-go/internal/platform/config"
	"github.com/beaglexv/backend-challenge-go/internal/platform/metrics"
)

// NewRouter assembles every route in the contract. Health checks are
// public (a load balancer probing them never carries a token); every
// business route sits behind authMiddleware plus the specific
// authorization check its resource needs.
func NewRouter(
	verifier *idp.Verifier,
	pool *pgxpool.Pool,
	sqsClient *sqs.Client,
	cfg *config.Config,
	openWallet *openwallet.Service,
	processor *processwagertransaction.Service,
	reconcile *reconciliation.Service,
	wallets ports.WalletRepository,
	ledgers ports.LedgerRepository,
	txs ports.WagerTransactionRepository,
	idGen ports.IDGenerator,
	m *metrics.Metrics,
	logger *zap.Logger,
) http.Handler {
	r := chi.NewRouter()
	r.Use(recoveryMiddleware(logger))
	r.Use(correlationIDMiddleware(func() string { return uuid.New().String() }))
	r.Use(loggingMiddleware(logger))

	health := newHealthHandlers(pool, sqsClient, cfg.SQS.WagerTransactionsQueueURL)
	r.Get("/health/live", health.liveHandler)
	r.Get("/health/ready", health.readyHandler)

	walletH := newWalletHandlers(openWallet, reconcile, wallets, ledgers, idGen, m, logger)
	wageringH := newWageringHandlers(processor, txs, m, logger)

	auth := authMiddleware(verifier)

	r.Route("/wallets", func(r chi.Router) {
		r.Use(auth, requireInternal)
		r.Post("/", walletH.openWalletHandler)
		r.Get("/{walletID}", walletH.getWalletHandler)
		r.Get("/{walletID}/ledger", walletH.listLedgerHandler)
		r.Post("/{walletID}/reconciliation", walletH.reconciliationHandler)
	})

	r.Route("/wagering/transactions", func(r chi.Router) {
		r.Use(auth, requireProvider)
		r.Post("/", wageringH.submitHandler)
		r.Get("/{transactionID}", wageringH.getByIDHandler)
	})

	r.Route("/providers/{providerID}/wagering/transactions/{externalTransactionID}", func(r chi.Router) {
		r.Use(auth, requireProvider)
		r.Get("/", wageringH.getByProviderAndExternalIDHandler)
	})

	// A per-request timeout is cheap protection against a slow/stuck
	// downstream call holding a connection open indefinitely — the
	// challenge does not require full rate limiting, but does call for at
	// least a request timeout.
	return http.TimeoutHandler(r, requestTimeout, `{"error":{"code":"REQUEST_TIMEOUT","message":"request timed out"}}`)
}

const requestTimeout = 30 * time.Second

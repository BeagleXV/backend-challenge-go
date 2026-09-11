package httpapi

import (
	"context"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"
)

type healthHandlers struct {
	pool          *pgxpool.Pool
	sqsClient     *sqs.Client
	wagerQueueURL string
}

func newHealthHandlers(pool *pgxpool.Pool, sqsClient *sqs.Client, wagerQueueURL string) *healthHandlers {
	return &healthHandlers{pool: pool, sqsClient: sqsClient, wagerQueueURL: wagerQueueURL}
}

// liveHandler only reports that the process is up and serving — it never
// touches a dependency, so it can't be dragged down by one.
func (h *healthHandlers) liveHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyHandler reports whether the process's dependencies are actually
// reachable — a real check against each one, never a hardcoded 200.
func (h *healthHandlers) readyHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	checks := map[string]string{}
	ready := true

	if err := h.pool.Ping(ctx); err != nil {
		checks["postgres"] = "unavailable"
		ready = false
	} else {
		checks["postgres"] = "ok"
	}

	if _, err := h.sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(h.wagerQueueURL),
	}); err != nil {
		checks["sqs"] = "unavailable"
		ready = false
	} else {
		checks["sqs"] = "ok"
	}

	status := http.StatusOK
	if !ready {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]any{"status": readyStatusLabel(ready), "checks": checks})
}

func readyStatusLabel(ready bool) string {
	if ready {
		return "ok"
	}
	return "unavailable"
}

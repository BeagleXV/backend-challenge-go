package httpapi

import (
	"context"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type healthHandlers struct {
	pool *pgxpool.Pool
}

func newHealthHandlers(pool *pgxpool.Pool) *healthHandlers {
	return &healthHandlers{pool: pool}
}

// liveHandler only reports that the process is up and serving — it never
// touches a dependency, so it can't be dragged down by one.
func (h *healthHandlers) liveHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyHandler reports whether the process's dependencies are actually
// reachable — a real ping, never a hardcoded 200. SQS readiness joins this
// once Fase 9 adds an SQS client to check.
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

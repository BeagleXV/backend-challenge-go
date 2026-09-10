package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/beaglexv/backend-challenge-go/internal/application/ports"
)

// OutboxRepository implements ports.OutboxRepository.
type OutboxRepository struct {
	pool *pgxpool.Pool
}

func NewOutboxRepository(pool *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{pool: pool}
}

func (r *OutboxRepository) Enqueue(ctx context.Context, eventID, aggregateID uuid.UUID, eventType string, payload []byte, occurredAt time.Time) error {
	_, err := q(ctx, r.pool).Exec(ctx,
		`INSERT INTO outbox_events (event_id, aggregate_id, event_type, payload, occurred_at)
		 VALUES ($1, $2, $3, $4, $5)`,
		eventID, aggregateID, eventType, payload, occurredAt,
	)
	if err != nil {
		return mapErr(err, "outbox.Enqueue")
	}
	return nil
}

// ClaimBatch locks up to limit due, unpublished rows with
// SELECT ... FOR UPDATE SKIP LOCKED — the mechanism that lets multiple
// publisher instances (Fase 11) each grab a disjoint batch without
// blocking each other, and without ever double-claiming a row still held
// by another instance's open transaction.
func (r *OutboxRepository) ClaimBatch(ctx context.Context, limit int, lockedBy string, lockDuration time.Duration) ([]ports.OutboxRecord, error) {
	now := time.Now().UTC()
	rows, err := q(ctx, r.pool).Query(ctx,
		`SELECT event_id, aggregate_id, event_type, payload, occurred_at, attempts
		 FROM outbox_events
		 WHERE published_at IS NULL
		   AND next_attempt_at <= $1
		   AND (locked_by IS NULL OR locked_at < $2)
		 ORDER BY next_attempt_at
		 LIMIT $3
		 FOR UPDATE SKIP LOCKED`,
		now, now.Add(-lockDuration), limit,
	)
	if err != nil {
		return nil, mapErr(err, "outbox.ClaimBatch")
	}

	var claimed []ports.OutboxRecord
	for rows.Next() {
		var rec ports.OutboxRecord
		if err := rows.Scan(&rec.EventID, &rec.AggregateID, &rec.EventType, &rec.Payload, &rec.OccurredAt, &rec.Attempts); err != nil {
			rows.Close()
			return nil, mapErr(err, "outbox.ClaimBatch")
		}
		claimed = append(claimed, rec)
	}
	closeErr := rows.Err()
	rows.Close()
	if closeErr != nil {
		return nil, mapErr(closeErr, "outbox.ClaimBatch")
	}

	if len(claimed) == 0 {
		return nil, nil
	}

	ids := make([]uuid.UUID, len(claimed))
	for i, rec := range claimed {
		ids[i] = rec.EventID
	}
	_, err = q(ctx, r.pool).Exec(ctx,
		`UPDATE outbox_events
		 SET locked_by = $1, locked_at = $2, attempts = attempts + 1
		 WHERE event_id = ANY($3)`,
		lockedBy, now, ids,
	)
	if err != nil {
		return nil, mapErr(err, "outbox.ClaimBatch")
	}
	for i := range claimed {
		claimed[i].Attempts++
	}

	return claimed, nil
}

func (r *OutboxRepository) MarkPublished(ctx context.Context, eventID uuid.UUID, publishedAt time.Time) error {
	tag, err := q(ctx, r.pool).Exec(ctx,
		`UPDATE outbox_events SET published_at = $2 WHERE event_id = $1`,
		eventID, publishedAt,
	)
	if err != nil {
		return mapErr(err, "outbox.MarkPublished")
	}
	if tag.RowsAffected() == 0 {
		return mapErr(pgx.ErrNoRows, "outbox.MarkPublished")
	}
	return nil
}

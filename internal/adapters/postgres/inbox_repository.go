package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/beaglexv/backend-challenge-go/internal/application/ports"
)

// InboxRepository implements ports.InboxRepository.
type InboxRepository struct {
	pool *pgxpool.Pool
}

func NewInboxRepository(pool *pgxpool.Pool) *InboxRepository {
	return &InboxRepository{pool: pool}
}

// TryInsert relies on ON CONFLICT DO NOTHING against the table's own
// (consumer_name, message_id) primary key rather than a SELECT-then-INSERT
// — the single statement is what makes two concurrent deliveries of the
// same message race safely: exactly one INSERT succeeds, the loser sees
// zero rows affected and reports alreadyExists.
func (r *InboxRepository) TryInsert(ctx context.Context, msg ports.InboxMessage) (bool, error) {
	tag, err := q(ctx, r.pool).Exec(ctx,
		`INSERT INTO inbox_messages (consumer_name, message_id, hash, received_at)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (consumer_name, message_id) DO NOTHING`,
		msg.ConsumerName, msg.MessageID, msg.Hash, time.Now().UTC(),
	)
	if err != nil {
		return false, mapErr(err, "inbox.TryInsert")
	}
	return tag.RowsAffected() == 0, nil
}

func (r *InboxRepository) Get(ctx context.Context, consumerName, messageID string) (ports.InboxMessage, error) {
	var msg ports.InboxMessage
	err := q(ctx, r.pool).QueryRow(ctx,
		`SELECT consumer_name, message_id, hash FROM inbox_messages WHERE consumer_name = $1 AND message_id = $2`,
		consumerName, messageID,
	).Scan(&msg.ConsumerName, &msg.MessageID, &msg.Hash)
	if err != nil {
		return ports.InboxMessage{}, mapErr(err, "inbox.Get")
	}
	return msg, nil
}

func (r *InboxRepository) MarkCompleted(ctx context.Context, consumerName, messageID string, completedAt time.Time) error {
	tag, err := q(ctx, r.pool).Exec(ctx,
		`UPDATE inbox_messages SET completed_at = $3 WHERE consumer_name = $1 AND message_id = $2`,
		consumerName, messageID, completedAt,
	)
	if err != nil {
		return mapErr(err, "inbox.MarkCompleted")
	}
	if tag.RowsAffected() == 0 {
		return mapErr(pgx.ErrNoRows, "inbox.MarkCompleted")
	}
	return nil
}

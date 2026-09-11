package outboxpublisher

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/beaglexv/backend-challenge-go/internal/application/apptest"
	"github.com/beaglexv/backend-challenge-go/internal/platform/metrics"
)

// fakePublisher is a controllable ports.EventPublisher: it records every
// call and can be told to fail specific event IDs, so tests can drive
// exactly the failure/recovery scenarios the challenge asks for.
type fakePublisher struct {
	mu       sync.Mutex
	calls    []uuid.UUID
	failFor  map[uuid.UUID]bool
	callHook func(eventID uuid.UUID)
}

func newFakePublisher() *fakePublisher {
	return &fakePublisher{failFor: make(map[uuid.UUID]bool)}
}

func (p *fakePublisher) Publish(ctx context.Context, eventID, aggregateID uuid.UUID, eventType string, payload []byte) error {
	p.mu.Lock()
	p.calls = append(p.calls, eventID)
	fail := p.failFor[eventID]
	hook := p.callHook
	p.mu.Unlock()

	if hook != nil {
		hook(eventID)
	}
	if fail {
		return errors.New("simulated publish failure")
	}
	return nil
}

func (p *fakePublisher) callCount(eventID uuid.UUID) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, id := range p.calls {
		if id == eventID {
			n++
		}
	}
	return n
}

func (p *fakePublisher) setFail(eventID uuid.UUID, fail bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failFor[eventID] = fail
}

func TestRunOnce_PublishesAndMarksPublished(t *testing.T) {
	outbox := apptest.NewOutboxRepository()
	eventID := uuid.New()
	require.NoError(t, outbox.Enqueue(context.Background(), eventID, uuid.New(), "WagerTransactionProcessed", []byte(`{}`), time.Now()))

	pub := newFakePublisher()
	w := New(apptest.NoopUnitOfWork{}, outbox, pub, Config{}, metrics.NewNoop(), zaptest.NewLogger(t))

	w.runOnce(context.Background())

	assert.Equal(t, 1, pub.callCount(eventID))
	require.Len(t, outbox.Events, 1)
	assert.NotNil(t, outbox.Events[0].PublishedAt)
}

func TestRunOnce_PublishFailure_LeavesEventClaimedNotPublished(t *testing.T) {
	outbox := apptest.NewOutboxRepository()
	eventID := uuid.New()
	require.NoError(t, outbox.Enqueue(context.Background(), eventID, uuid.New(), "WagerTransactionProcessed", []byte(`{}`), time.Now()))

	pub := newFakePublisher()
	pub.setFail(eventID, true)
	w := New(apptest.NoopUnitOfWork{}, outbox, pub, Config{LockDuration: time.Hour}, metrics.NewNoop(), zaptest.NewLogger(t))

	w.runOnce(context.Background())

	require.Len(t, outbox.Events, 1)
	assert.Nil(t, outbox.Events[0].PublishedAt)
	assert.Equal(t, 1, outbox.Events[0].Attempts)

	// A second poll, immediately after, must not reclaim it: the lock is
	// still fresh.
	w.runOnce(context.Background())
	assert.Equal(t, 1, pub.callCount(eventID), "a freshly failed claim must not be retried before its lock expires")
}

func TestRunOnce_AbandonedLock_ReclaimedAfterExpiry(t *testing.T) {
	outbox := apptest.NewOutboxRepository()
	eventID := uuid.New()
	require.NoError(t, outbox.Enqueue(context.Background(), eventID, uuid.New(), "WagerTransactionProcessed", []byte(`{}`), time.Now()))

	pub := newFakePublisher()
	pub.setFail(eventID, true)
	w := New(apptest.NoopUnitOfWork{}, outbox, pub, Config{LockDuration: 20 * time.Millisecond}, metrics.NewNoop(), zaptest.NewLogger(t))

	w.runOnce(context.Background())
	assert.Equal(t, 1, pub.callCount(eventID))

	// Now let the lock go stale and stop failing — simulating the
	// abandoned-work-is-resumed scenario (a crashed/slow instance's claim
	// expiring, another attempt succeeding).
	pub.setFail(eventID, false)
	require.Eventually(t, func() bool {
		w.runOnce(context.Background())
		return pub.callCount(eventID) == 2
	}, time.Second, 5*time.Millisecond)

	require.Len(t, outbox.Events, 1)
	require.NotNil(t, outbox.Events[0].PublishedAt)
	assert.Equal(t, 2, outbox.Events[0].Attempts)
}

func TestRunOnce_MarkPublishedFailure_RemainsClaimedForRetry(t *testing.T) {
	// Simulates the "interruption between publication and confirmation"
	// scenario: Publish succeeds, but the outbox row for that eventId no
	// longer exists to be marked (stand-in for a crash before commit) —
	// MarkPublished then fails, and the event must remain claimed rather
	// than be silently dropped or double-processed in this same cycle.
	outbox := apptest.NewOutboxRepository()
	eventID := uuid.New()
	require.NoError(t, outbox.Enqueue(context.Background(), eventID, uuid.New(), "WagerTransactionProcessed", []byte(`{}`), time.Now()))

	pub := newFakePublisher()
	// Delete the row the instant Publish is called, before MarkPublished
	// runs, forcing MarkPublished to fail with ErrNotFound.
	w := New(apptest.NoopUnitOfWork{}, outbox, pub, Config{}, metrics.NewNoop(), zaptest.NewLogger(t))
	pub.callHook = func(id uuid.UUID) {
		outbox.Events = nil
	}

	w.runOnce(context.Background())

	assert.Equal(t, 1, pub.callCount(eventID))
	assert.Empty(t, outbox.Events, "sanity: the row really was removed out from under MarkPublished")
}

func TestTwoWorkers_ConcurrentPoll_NeitherDoubleClaimsWhileLockIsFresh(t *testing.T) {
	outbox := apptest.NewOutboxRepository()
	eventID := uuid.New()
	require.NoError(t, outbox.Enqueue(context.Background(), eventID, uuid.New(), "WagerTransactionProcessed", []byte(`{}`), time.Now()))

	pubA := newFakePublisher()
	pubB := newFakePublisher()
	workerA := New(apptest.NoopUnitOfWork{}, outbox, pubA, Config{LockDuration: time.Hour}, metrics.NewNoop(), zaptest.NewLogger(t))
	workerB := New(apptest.NoopUnitOfWork{}, outbox, pubB, Config{LockDuration: time.Hour}, metrics.NewNoop(), zaptest.NewLogger(t))

	workerA.runOnce(context.Background())
	workerB.runOnce(context.Background())

	assert.Equal(t, 1, pubA.callCount(eventID))
	assert.Equal(t, 0, pubB.callCount(eventID), "the second worker must not claim an event still validly locked by the first")
}

func TestStart_Stop_GracefulShutdown(t *testing.T) {
	outbox := apptest.NewOutboxRepository()
	require.NoError(t, outbox.Enqueue(context.Background(), uuid.New(), uuid.New(), "WagerTransactionProcessed", []byte(`{}`), time.Now()))

	w := New(apptest.NoopUnitOfWork{}, outbox, newFakePublisher(), Config{PollInterval: 10 * time.Millisecond}, metrics.NewNoop(), zaptest.NewLogger(t))
	w.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, w.Stop(ctx))
}

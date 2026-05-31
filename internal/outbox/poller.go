package outbox

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/kirtipurohit/sagaforge-ai/internal/kafka"
)

// OutboxEvent mirrors the outbox_events table row.
type OutboxEvent struct {
	ID             string
	AggregateID    string
	AggregateType  string
	EventType      string
	Payload        json.RawMessage
	IdempotencyKey string
}

// Poller reads unpublished rows from outbox_events and delivers them to Kafka.
// It uses SELECT … FOR UPDATE SKIP LOCKED so multiple poller instances don't
// duplicate work.
type Poller struct {
	pool     *pgxpool.Pool
	producer *kafka.Producer
	interval time.Duration
	log      *zap.Logger
}

func NewPoller(pool *pgxpool.Pool, producer *kafka.Producer, interval time.Duration, log *zap.Logger) *Poller {
	return &Poller{pool: pool, producer: producer, interval: interval, log: log}
}

// Run polls until ctx is cancelled.
func (p *Poller) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.poll(ctx); err != nil {
				p.log.Error("outbox poll error", zap.Error(err))
			}
		}
	}
}

func (p *Poller) poll(ctx context.Context) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	rows, err := tx.Query(ctx, `
		SELECT id, aggregate_id, aggregate_type, event_type, payload, idempotency_key
		FROM outbox_events
		WHERE published_at IS NULL
		ORDER BY created_at
		LIMIT 50
		FOR UPDATE SKIP LOCKED
	`)
	if err != nil {
		return err
	}

	var events []OutboxEvent
	for rows.Next() {
		var e OutboxEvent
		if err := rows.Scan(&e.ID, &e.AggregateID, &e.AggregateType, &e.EventType, &e.Payload, &e.IdempotencyKey); err != nil {
			return err
		}
		events = append(events, e)
	}
	rows.Close()

	for _, e := range events {
		topic := topicFor(e.AggregateType)
		if err := p.producer.Publish(ctx, topic, e.AggregateID, e.Payload); err != nil {
			p.log.Error("publish failed", zap.String("event_id", e.ID), zap.Error(err))
			continue // leave unpublished; will retry next poll
		}
		if _, err := tx.Exec(ctx, `
			UPDATE outbox_events SET published_at = NOW() WHERE id = $1
		`, e.ID); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// topicFor maps an aggregate type to a Kafka topic name.
func topicFor(aggregateType string) string {
	switch aggregateType {
	case "order":
		return kafka.TopicOrderEvents
	case "inventory":
		return kafka.TopicInventoryEvents
	case "payment":
		return kafka.TopicPaymentEvents
	case "fulfillment":
		return kafka.TopicFulfillmentEvents
	case "saga":
		return kafka.TopicSagaEvents
	case "ai_insight":
		return kafka.TopicAIInsightEvents
	default:
		return kafka.TopicDLQ
	}
}

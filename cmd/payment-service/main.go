package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/joho/godotenv"
	"go.uber.org/zap"

	"github.com/kirtipurohit/sagaforge-ai/internal/db"
	"github.com/kirtipurohit/sagaforge-ai/internal/kafka"
	"github.com/kirtipurohit/sagaforge-ai/internal/models"
	"github.com/kirtipurohit/sagaforge-ai/internal/outbox"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	_ = godotenv.Load()

	log, _ := zap.NewProduction()
	defer log.Sync() //nolint:errcheck

	pool, err := db.New(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal("db connect", zap.Error(err))
	}
	defer pool.Close()

	kafkaCfg := kafka.Config{
		Brokers:  strings.Split(os.Getenv("KAFKA_BROKERS"), ","),
		Username: os.Getenv("KAFKA_USERNAME"),
		Password: os.Getenv("KAFKA_PASSWORD"),
		TLS:      os.Getenv("KAFKA_TLS") == "true",
	}
	producer := kafka.NewProducer(kafkaCfg, log)
	defer producer.Close()

	pollInterval := 500 * time.Millisecond
	if ms, err := strconv.Atoi(os.Getenv("OUTBOX_POLL_INTERVAL_MS")); err == nil {
		pollInterval = time.Duration(ms) * time.Millisecond
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	poller := outbox.NewPoller(pool, producer, pollInterval, log)
	go poller.Run(ctx)

	consumer := kafka.NewConsumer(kafkaCfg, kafka.TopicInventoryEvents, "payment-service", log)
	defer consumer.Close() //nolint:errcheck

	log.Info("payment-service listening", zap.String("topic", kafka.TopicInventoryEvents))
	if err := consumer.Consume(ctx, makeHandler(pool, log)); err != nil {
		log.Error("consumer error", zap.Error(err))
	}
}

func makeHandler(pool *pgxpool.Pool, log *zap.Logger) kafka.HandlerFunc {
	return func(ctx context.Context, msg kafkago.Message) error {
		event, err := kafka.Unmarshal[models.Event](msg)
		if err != nil {
			return err
		}
		if event.Type != models.EventInventoryReserved {
			return nil // ignore reserve-failed; saga-orchestrator handles compensation
		}
		return processPayment(ctx, pool, log, event)
	}
}

func processPayment(ctx context.Context, pool *pgxpool.Pool, log *zap.Logger, event models.Event) error {
	// Derive total from event payload (set by order-service)
	total, _ := event.Payload["total"].(float64)
	paymentID := uuid.New()
	idempotencyKey := fmt.Sprintf("payment-%s", event.OrderID)

	// --- Idempotency guard ---
	var existing string
	_ = pool.QueryRow(ctx, `SELECT id FROM payments WHERE idempotency_key = $1`, idempotencyKey).Scan(&existing)
	if existing != "" {
		log.Info("payment already processed (idempotent)", zap.String("order_id", event.OrderID.String()))
		return nil
	}

	// Simulate a ~10% payment failure rate for demo purposes
	paymentFailed := rand.Float32() < 0.10
	failReason := "card declined (simulated)"

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if paymentFailed {
		if _, err := tx.Exec(ctx, `
			INSERT INTO payments (id, order_id, amount, status, idempotency_key, failed_at, failure_reason)
			VALUES ($1, $2, $3, 'failed', $4, NOW(), $5)
		`, paymentID, event.OrderID, total, idempotencyKey, failReason); err != nil {
			return err
		}
	} else {
		if _, err := tx.Exec(ctx, `
			INSERT INTO payments (id, order_id, amount, status, idempotency_key, processed_at)
			VALUES ($1, $2, $3, 'processed', $4, NOW())
		`, paymentID, event.OrderID, total, idempotencyKey); err != nil {
			return err
		}
	}

	var (
		eventType      models.EventType
		outboxPayload  models.Event
	)

	if paymentFailed {
		eventType = models.EventPaymentFailed
		outboxPayload = models.Event{
			ID: uuid.New(), SagaID: event.SagaID, OrderID: event.OrderID,
			Type: eventType, OccurredAt: time.Now().UTC(),
			IdempotencyKey: fmt.Sprintf("payment-failed-%s", event.OrderID),
			Payload: map[string]any{
				"payment_id": paymentID,
				"reason":     failReason,
				"total":      total,
			},
		}
	} else {
		eventType = models.EventPaymentProcessed
		outboxPayload = models.Event{
			ID: uuid.New(), SagaID: event.SagaID, OrderID: event.OrderID,
			Type: eventType, OccurredAt: time.Now().UTC(),
			IdempotencyKey: fmt.Sprintf("payment-processed-%s", event.OrderID),
			Payload: map[string]any{
				"payment_id": paymentID,
				"total":      total,
			},
		}
	}

	payloadJSON, _ := json.Marshal(outboxPayload)
	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (aggregate_id, aggregate_type, event_type, payload, idempotency_key)
		VALUES ($1, 'payment', $2, $3, $4)
		ON CONFLICT (idempotency_key) DO NOTHING
	`, event.OrderID.String(), string(eventType), payloadJSON, outboxPayload.IdempotencyKey); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	log.Info("payment processed",
		zap.String("order_id", event.OrderID.String()),
		zap.String("result", string(eventType)),
	)
	return nil
}

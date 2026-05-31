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

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kirtipurohit/sagaforge-ai/internal/db"
	"github.com/kirtipurohit/sagaforge-ai/internal/kafka"
	"github.com/kirtipurohit/sagaforge-ai/internal/models"
	"github.com/kirtipurohit/sagaforge-ai/internal/outbox"
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

	consumer := kafka.NewConsumer(kafkaCfg, kafka.TopicPaymentEvents, "fulfillment-service", log)
	defer consumer.Close() //nolint:errcheck

	log.Info("fulfillment-service listening", zap.String("topic", kafka.TopicPaymentEvents))
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
		if event.Type != models.EventPaymentProcessed {
			return nil // payment.failed is handled by the saga orchestrator
		}
		return startFulfillment(ctx, pool, log, event)
	}
}

func startFulfillment(ctx context.Context, pool *pgxpool.Pool, log *zap.Logger, event models.Event) error {
	fulfillmentID := uuid.New()
	trackingID := fmt.Sprintf("TRACK-%s", strings.ToUpper(fulfillmentID.String()[:8]))

	// Simulate ~5% fulfillment failure
	fulfillmentFailed := rand.Float32() < 0.05
	failReason := "warehouse system unavailable (simulated)"

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if fulfillmentFailed {
		if _, err := tx.Exec(ctx, `
			INSERT INTO fulfillments (id, order_id, status, failed_at)
			VALUES ($1, $2, 'failed', NOW())
			ON CONFLICT (order_id) DO UPDATE SET status = 'failed', failed_at = NOW()
		`, fulfillmentID, event.OrderID); err != nil {
			return err
		}
	} else {
		if _, err := tx.Exec(ctx, `
			INSERT INTO fulfillments (id, order_id, status, tracking_id, started_at, shipped_at)
			VALUES ($1, $2, 'shipped', $3, NOW(), NOW())
			ON CONFLICT (order_id) DO UPDATE
			  SET status = 'shipped', tracking_id = $3, started_at = NOW(), shipped_at = NOW()
		`, fulfillmentID, event.OrderID, trackingID); err != nil {
			return err
		}
	}

	var (
		eventType      models.EventType
		iKey           string
		payload        map[string]any
	)

	if fulfillmentFailed {
		eventType = models.EventFulfillmentFailed
		iKey = fmt.Sprintf("fulfillment-failed-%s", event.OrderID)
		payload = map[string]any{"reason": failReason}
	} else {
		eventType = models.EventFulfillmentShipped
		iKey = fmt.Sprintf("fulfillment-shipped-%s", event.OrderID)
		payload = map[string]any{"tracking_id": trackingID, "fulfillment_id": fulfillmentID}
	}

	outboxEvent := models.Event{
		ID: uuid.New(), SagaID: event.SagaID, OrderID: event.OrderID,
		Type: eventType, OccurredAt: time.Now().UTC(),
		IdempotencyKey: iKey, Payload: payload,
	}
	payloadJSON, _ := json.Marshal(outboxEvent)

	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (aggregate_id, aggregate_type, event_type, payload, idempotency_key)
		VALUES ($1, 'fulfillment', $2, $3, $4)
		ON CONFLICT (idempotency_key) DO NOTHING
	`, event.OrderID.String(), string(eventType), payloadJSON, iKey); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	log.Info("fulfillment processed",
		zap.String("order_id", event.OrderID.String()),
		zap.String("result", string(eventType)),
	)
	return nil
}

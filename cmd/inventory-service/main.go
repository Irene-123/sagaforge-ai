package main

import (
	"context"
	"encoding/json"
	"fmt"
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

	consumer := kafka.NewConsumer(kafkaCfg, kafka.TopicOrderEvents, "inventory-service", log)
	defer consumer.Close() //nolint:errcheck

	log.Info("inventory-service listening", zap.String("topic", kafka.TopicOrderEvents))
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
		if event.Type != models.EventOrderCreated {
			return nil
		}
		return reserveInventory(ctx, pool, log, event)
	}
}

func reserveInventory(ctx context.Context, pool *pgxpool.Pool, log *zap.Logger, event models.Event) error {
	// Decode items from event payload
	itemsRaw, _ := json.Marshal(event.Payload["items"])
	var items []models.OrderItem
	if err := json.Unmarshal(itemsRaw, &items); err != nil {
		return fmt.Errorf("decode items: %w", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var reservationFailed bool
	var failReason string

	for _, item := range items {
		// Atomic decrement – fails if stock insufficient
		tag, err := tx.Exec(ctx, `
			UPDATE inventory_stock
			SET available_qty = available_qty - $1, updated_at = NOW()
			WHERE sku = $2 AND available_qty >= $1
		`, item.Quantity, item.SKU)
		if err != nil {
			return fmt.Errorf("stock update %s: %w", item.SKU, err)
		}
		if tag.RowsAffected() == 0 {
			reservationFailed = true
			failReason = fmt.Sprintf("insufficient stock for SKU %s", item.SKU)
			break
		}
		// Record reservation row
		if _, err := tx.Exec(ctx, `
			INSERT INTO inventory_reservations (id, order_id, sku, quantity)
			VALUES ($1, $2, $3, $4)
		`, uuid.New(), event.OrderID, item.SKU, item.Quantity); err != nil {
			return err
		}
	}

	var (
		eventType      models.EventType
		idempotencyKey string
		outboxPayload  models.Event
	)

	if reservationFailed {
		eventType = models.EventInventoryReserveFailed
		idempotencyKey = fmt.Sprintf("inventory-reserve-failed-%s", event.OrderID)
		outboxPayload = models.Event{
			ID: uuid.New(), SagaID: event.SagaID, OrderID: event.OrderID,
			Type: eventType, OccurredAt: time.Now().UTC(),
			IdempotencyKey: idempotencyKey,
			Payload:        map[string]any{"reason": failReason},
		}
	} else {
		eventType = models.EventInventoryReserved
		idempotencyKey = fmt.Sprintf("inventory-reserved-%s", event.OrderID)
		outboxPayload = models.Event{
			ID: uuid.New(), SagaID: event.SagaID, OrderID: event.OrderID,
			Type: eventType, OccurredAt: time.Now().UTC(),
			IdempotencyKey: idempotencyKey,
			Payload:        map[string]any{"items": items},
		}
	}

	payloadJSON, _ := json.Marshal(outboxPayload)
	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (aggregate_id, aggregate_type, event_type, payload, idempotency_key)
		VALUES ($1, 'inventory', $2, $3, $4)
		ON CONFLICT (idempotency_key) DO NOTHING
	`, event.OrderID.String(), string(eventType), payloadJSON, idempotencyKey); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	log.Info("inventory processed",
		zap.String("order_id", event.OrderID.String()),
		zap.String("result", string(eventType)),
	)
	return nil
}

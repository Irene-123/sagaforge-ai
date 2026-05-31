package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"go.uber.org/zap"

	"github.com/kirtipurohit/sagaforge-ai/internal/db"
	"github.com/kirtipurohit/sagaforge-ai/internal/kafka"
	"github.com/kirtipurohit/sagaforge-ai/internal/models"
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

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	consumer := kafka.NewConsumer(kafkaCfg, kafka.TopicSagaEvents, "notification-service", log)
	defer consumer.Close() //nolint:errcheck

	log.Info("notification-service listening", zap.String("topic", kafka.TopicSagaEvents))
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

		switch event.Type {
		case models.EventOrderCompleted:
			return sendNotification(ctx, pool, log, event, "success",
				fmt.Sprintf("✅ Order %s completed! Your items are on the way.", event.OrderID.String()[:8]))
		case models.EventOrderFailed:
			reason, _ := event.Payload["reason"].(string)
			return sendNotification(ctx, pool, log, event, "failure",
				fmt.Sprintf("❌ Order %s failed: %s. We'll retry or refund automatically.", event.OrderID.String()[:8], reason))
		}
		return nil
	}
}

func sendNotification(ctx context.Context, pool *pgxpool.Pool, log *zap.Logger, event models.Event, ntype, message string) error {
	// Look up customer_id from the order
	var customerID string
	_ = pool.QueryRow(ctx, `SELECT customer_id FROM orders WHERE id = $1`, event.OrderID).Scan(&customerID)

	notif := map[string]any{
		"id":          uuid.New().String(),
		"order_id":    event.OrderID.String(),
		"customer_id": customerID,
		"type":        ntype,
		"channel":     "email", // simulated
		"message":     message,
		"sent_at":     time.Now().UTC().Format(time.RFC3339),
	}

	data, _ := json.MarshalIndent(notif, "", "  ")

	// In production this would call SendGrid / SES / Twilio.
	// For demo we log it visibly so it shows in the console and dashboard.
	log.Info("📧 NOTIFICATION SENT",
		zap.String("type", ntype),
		zap.String("order_id", event.OrderID.String()),
		zap.String("customer_id", customerID),
		zap.String("body", string(data)),
	)

	return nil
}

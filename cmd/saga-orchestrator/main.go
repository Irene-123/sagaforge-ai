package main

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"go.uber.org/zap"

	"github.com/kirtipurohit/sagaforge-ai/internal/db"
	"github.com/kirtipurohit/sagaforge-ai/internal/kafka"
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

	orch := NewOrchestrator(pool, log)

	// Fan-out: one consumer goroutine per topic the orchestrator cares about.
	// Each uses its own consumer group so it gets all messages independently
	// of the business-logic services.
	topics := []string{
		kafka.TopicInventoryEvents,
		kafka.TopicPaymentEvents,
		kafka.TopicFulfillmentEvents,
	}

	errCh := make(chan error, len(topics))
	for _, topic := range topics {
		t := topic
		consumer := kafka.NewConsumer(kafkaCfg, t, "saga-orchestrator", log)
		go func() {
			log.Info("orchestrator listening", zap.String("topic", t))
			errCh <- consumer.Consume(ctx, orch.Handle)
		}()
	}

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil {
			log.Error("consumer error", zap.Error(err))
		}
	}
}

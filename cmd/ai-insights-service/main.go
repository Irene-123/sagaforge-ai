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

	aiCfg := AzureOpenAIConfig{
		Endpoint:       os.Getenv("AZURE_OPENAI_ENDPOINT"),
		APIKey:         os.Getenv("AZURE_OPENAI_API_KEY"),
		DeploymentName: os.Getenv("AZURE_OPENAI_DEPLOYMENT"),
		APIVersion:     os.Getenv("AZURE_OPENAI_API_VERSION"),
	}
	if aiCfg.APIVersion == "" {
		aiCfg.APIVersion = "2024-08-01-preview"
	}

	svc := NewInsightsService(pool, aiCfg, log)

	// Subscribe to saga-events, payment-events, and order-events for AI analysis
	topics := []string{kafka.TopicSagaEvents, kafka.TopicPaymentEvents, kafka.TopicOrderEvents}

	errCh := make(chan error, len(topics))
	for _, topic := range topics {
		t := topic
		consumer := kafka.NewConsumer(kafkaCfg, t, "ai-insights-service", log)
		go func() {
			log.Info("ai-insights-service listening", zap.String("topic", t))
			errCh <- consumer.Consume(ctx, svc.Handle)
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

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

	// Outbox poller runs in the background
	poller := outbox.NewPoller(pool, producer, pollInterval, log)
	go poller.Run(ctx)

	port := os.Getenv("ORDER_SERVICE_PORT")
	if port == "" {
		port = "8081"
	}

	srv := NewServer(pool, log)
	log.Info("order-service starting", zap.String("port", port))
	if err := srv.Start(ctx, ":"+port); err != nil {
		log.Error("server error", zap.Error(err))
	}
}

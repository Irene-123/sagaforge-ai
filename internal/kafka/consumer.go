package kafka

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"go.uber.org/zap"
)

// HandlerFunc processes a raw Kafka message. Return an error to NACK (DLQ routing handled by caller).
type HandlerFunc func(ctx context.Context, msg kafkago.Message) error

type Consumer struct {
	reader *kafkago.Reader
	log    *zap.Logger
}

func NewConsumer(cfg Config, topic, groupID string, log *zap.Logger) *Consumer {
	rc := kafkago.ReaderConfig{
		Brokers:     cfg.Brokers,
		Topic:       topic,
		GroupID:     groupID,
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafkago.FirstOffset,
	}
	if cfg.TLS {
		rc.Dialer = &kafkago.Dialer{
			TLS: &tls.Config{MinVersion: tls.VersionTLS12},
			SASLMechanism: plain.Mechanism{
				Username: cfg.Username,
				Password: cfg.Password,
			},
		}
	}
	return &Consumer{
		reader: kafkago.NewReader(rc),
		log:    log,
	}
}

// Consume reads messages in a loop and dispatches to handler. Blocks until ctx is cancelled.
func (c *Consumer) Consume(ctx context.Context, handler HandlerFunc) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // clean shutdown
			}
			return fmt.Errorf("fetch: %w", err)
		}
		if err := handler(ctx, msg); err != nil {
			c.log.Error("handler error", zap.Error(err), zap.String("topic", msg.Topic))
			// commit anyway to avoid poison-pill loop; caller is responsible for DLQ routing
		}
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.log.Warn("commit failed", zap.Error(err))
		}
	}
}

// Unmarshal is a helper to decode a Kafka message value into v.
func Unmarshal[T any](msg kafkago.Message) (T, error) {
	var v T
	if err := json.Unmarshal(msg.Value, &v); err != nil {
		return v, fmt.Errorf("unmarshal: %w", err)
	}
	return v, nil
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

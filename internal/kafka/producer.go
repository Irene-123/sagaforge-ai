package kafka

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"go.uber.org/zap"
)

type Producer struct {
	writers map[string]*kafkago.Writer
	cfg     Config
	log     *zap.Logger
}

type Config struct {
	Brokers  []string
	Username string
	Password string
	TLS      bool
}

func NewProducer(cfg Config, log *zap.Logger) *Producer {
	return &Producer{
		writers: make(map[string]*kafkago.Writer),
		cfg:     cfg,
		log:     log,
	}
}

func (p *Producer) writerFor(topic string) *kafkago.Writer {
	if w, ok := p.writers[topic]; ok {
		return w
	}
	w := &kafkago.Writer{
		Addr:         kafkago.TCP(p.cfg.Brokers...),
		Topic:        topic,
		Balancer:     &kafkago.Hash{}, // route same order_id to same partition
		RequiredAcks: kafkago.RequireAll,
		Async:        false,
	}
	if p.cfg.TLS {
		w.Transport = &kafkago.Transport{
			TLS: &tls.Config{MinVersion: tls.VersionTLS12},
			SASL: plain.Mechanism{
				Username: p.cfg.Username,
				Password: p.cfg.Password,
			},
		}
	}
	p.writers[topic] = w
	return w
}

// Publish serialises msg as JSON and sends it to topic, keyed by partitionKey.
func (p *Producer) Publish(ctx context.Context, topic, partitionKey string, msg any) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	km := kafkago.Message{
		Key:   []byte(partitionKey),
		Value: data,
		Time:  time.Now(),
	}
	if err := p.writerFor(topic).WriteMessages(ctx, km); err != nil {
		return fmt.Errorf("write to %s: %w", topic, err)
	}
	p.log.Info("published event", zap.String("topic", topic), zap.String("key", partitionKey))
	return nil
}

func (p *Producer) Close() {
	for _, w := range p.writers {
		_ = w.Close()
	}
}

package models

import (
	"time"

	"github.com/google/uuid"
)

// EventType identifies what happened
type EventType string

const (
	// Order lifecycle
	EventOrderCreated   EventType = "order.created"
	EventOrderCompleted EventType = "order.completed"
	EventOrderFailed    EventType = "order.failed"
	EventOrderCancelled EventType = "order.cancelled"

	// Inventory
	EventInventoryReserved       EventType = "inventory.reserved"
	EventInventoryReserveFailed  EventType = "inventory.reserve_failed"
	EventInventoryReleased       EventType = "inventory.released"

	// Payment
	EventPaymentProcessed EventType = "payment.processed"
	EventPaymentFailed    EventType = "payment.failed"
	EventPaymentRefunded  EventType = "payment.refunded"

	// Fulfillment
	EventFulfillmentStarted EventType = "fulfillment.started"
	EventFulfillmentShipped EventType = "fulfillment.shipped"
	EventFulfillmentFailed  EventType = "fulfillment.failed"

	// Notification
	EventNotificationSent EventType = "notification.sent"

	// AI
	EventAIInsightGenerated EventType = "ai.insight_generated"
)

// SagaStatus tracks overall saga progress
type SagaStatus string

const (
	SagaStatusStarted      SagaStatus = "started"
	SagaStatusInProgress   SagaStatus = "in_progress"
	SagaStatusCompleted    SagaStatus = "completed"
	SagaStatusFailed       SagaStatus = "failed"
	SagaStatusCompensating SagaStatus = "compensating"
	SagaStatusCompensated  SagaStatus = "compensated"
)

// Event is the canonical envelope for all Kafka messages
type Event struct {
	ID            uuid.UUID      `json:"id"`
	SagaID        uuid.UUID      `json:"saga_id"`
	OrderID       uuid.UUID      `json:"order_id"`
	Type          EventType      `json:"type"`
	OccurredAt    time.Time      `json:"occurred_at"`
	IdempotencyKey string        `json:"idempotency_key"`
	Payload       map[string]any `json:"payload"`
}

// OrderItem is a line item in an order
type OrderItem struct {
	SKU      string  `json:"sku"`
	Quantity int     `json:"quantity"`
	Price    float64 `json:"price"`
}

// CreateOrderRequest is the HTTP request body
type CreateOrderRequest struct {
	CustomerID string      `json:"customer_id"`
	Items      []OrderItem `json:"items"`
}

// AIInsight is enrichment produced by the AI Insights service
type AIInsight struct {
	SagaID      uuid.UUID `json:"saga_id"`
	OrderID     uuid.UUID `json:"order_id"`
	TriggerEvent EventType `json:"trigger_event"`
	RiskScore   float64   `json:"risk_score"`   // 0.0–1.0
	Explanation string    `json:"explanation"`
	Suggestion  string    `json:"suggestion"`
	GeneratedAt time.Time `json:"generated_at"`
}

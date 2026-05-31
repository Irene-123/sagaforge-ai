package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/kirtipurohit/sagaforge-ai/internal/kafka"
	"github.com/kirtipurohit/sagaforge-ai/internal/models"
)

// Orchestrator reacts to events from all services and drives saga state
// transitions + compensating transactions.
type Orchestrator struct {
	pool *pgxpool.Pool
	log  *zap.Logger
}

func NewOrchestrator(pool *pgxpool.Pool, log *zap.Logger) *Orchestrator {
	return &Orchestrator{pool: pool, log: log}
}

// Handle is the kafka.HandlerFunc dispatched to every consumed message.
func (o *Orchestrator) Handle(ctx context.Context, msg kafkago.Message) error {
	event, err := kafka.Unmarshal[models.Event](msg)
	if err != nil {
		return err
	}

	o.log.Info("orchestrator received event",
		zap.String("type", string(event.Type)),
		zap.String("order_id", event.OrderID.String()),
	)

	switch event.Type {
	case models.EventInventoryReserved:
		return o.advanceStep(ctx, event, "process_payment", models.SagaStatusInProgress)

	case models.EventInventoryReserveFailed:
		return o.failSaga(ctx, event, "reserve_inventory", "inventory reservation failed")

	case models.EventPaymentProcessed:
		return o.advanceStep(ctx, event, "fulfill_order", models.SagaStatusInProgress)

	case models.EventPaymentFailed:
		return o.compensatePaymentFailed(ctx, event)

	case models.EventFulfillmentShipped:
		return o.completeSaga(ctx, event)

	case models.EventFulfillmentFailed:
		return o.compensateFulfillmentFailed(ctx, event)
	}
	return nil
}

// advanceStep updates the saga's current step and status.
func (o *Orchestrator) advanceStep(ctx context.Context, event models.Event, nextStep string, status models.SagaStatus) error {
	_, err := o.pool.Exec(ctx, `
		UPDATE sagas
		SET current_step = $1, status = $2, updated_at = NOW()
		WHERE order_id = $3
	`, nextStep, string(status), event.OrderID)
	return err
}

// failSaga marks the saga as failed and updates the order status.
func (o *Orchestrator) failSaga(ctx context.Context, event models.Event, failedStep, reason string) error {
	tx, err := o.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err := tx.Exec(ctx, `
		UPDATE sagas
		SET status = $1, current_step = $2, failure_reason = $3,
		    completed_at = NOW(), updated_at = NOW()
		WHERE order_id = $4
	`, string(models.SagaStatusFailed), failedStep, reason, event.OrderID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE orders SET status = 'failed', updated_at = NOW() WHERE id = $1
	`, event.OrderID); err != nil {
		return err
	}

	if err := o.appendOutboxInTx(ctx, tx, event, models.EventOrderFailed, map[string]any{
		"reason": reason,
	}); err != nil {
		return err
	}

	o.log.Warn("saga failed", zap.String("order_id", event.OrderID.String()), zap.String("reason", reason))
	return tx.Commit(ctx)
}

// compensatePaymentFailed releases inventory then fails the saga.
func (o *Orchestrator) compensatePaymentFailed(ctx context.Context, event models.Event) error {
	tx, err := o.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Mark saga as compensating
	if _, err := tx.Exec(ctx, `
		UPDATE sagas
		SET status = $1, current_step = 'release_inventory', updated_at = NOW()
		WHERE order_id = $2
	`, string(models.SagaStatusCompensating), event.OrderID); err != nil {
		return err
	}

	// Release stock for every reserved SKU
	rows, err := tx.Query(ctx, `
		SELECT sku, quantity FROM inventory_reservations
		WHERE order_id = $1 AND status = 'reserved'
	`, event.OrderID)
	if err != nil {
		return err
	}
	type reservation struct{ SKU string; Qty int }
	var reservations []reservation
	for rows.Next() {
		var r reservation
		if err := rows.Scan(&r.SKU, &r.Qty); err != nil {
			return err
		}
		reservations = append(reservations, r)
	}
	rows.Close()

	for _, r := range reservations {
		if _, err := tx.Exec(ctx, `
			UPDATE inventory_stock
			SET available_qty = available_qty + $1, updated_at = NOW()
			WHERE sku = $2
		`, r.Qty, r.SKU); err != nil {
			return err
		}
	}

	if _, err := tx.Exec(ctx, `
		UPDATE inventory_reservations SET status = 'released', released_at = NOW()
		WHERE order_id = $1 AND status = 'reserved'
	`, event.OrderID); err != nil {
		return err
	}

	// Mark saga + order as compensated/failed
	if _, err := tx.Exec(ctx, `
		UPDATE sagas
		SET status = $1, completed_at = NOW(), updated_at = NOW(),
		    failure_reason = 'payment failed'
		WHERE order_id = $2
	`, string(models.SagaStatusCompensated), event.OrderID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE orders SET status = 'failed', updated_at = NOW() WHERE id = $1
	`, event.OrderID); err != nil {
		return err
	}

	// Emit inventory.released and order.failed via outbox
	if err := o.appendOutboxInTx(ctx, tx, event, models.EventInventoryReleased, map[string]any{
		"released_skus": reservations,
	}); err != nil {
		return err
	}
	if err := o.appendOutboxInTx(ctx, tx, event, models.EventOrderFailed, map[string]any{
		"reason": "payment failed — inventory released",
	}); err != nil {
		return err
	}

	o.log.Warn("compensated: payment failed, inventory released",
		zap.String("order_id", event.OrderID.String()),
	)
	return tx.Commit(ctx)
}

// compensateFulfillmentFailed refunds payment + releases inventory then fails the saga.
func (o *Orchestrator) compensateFulfillmentFailed(ctx context.Context, event models.Event) error {
	tx, err := o.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err := tx.Exec(ctx, `
		UPDATE sagas
		SET status = $1, current_step = 'refund_payment', updated_at = NOW()
		WHERE order_id = $2
	`, string(models.SagaStatusCompensating), event.OrderID); err != nil {
		return err
	}

	// Refund payment
	if _, err := tx.Exec(ctx, `
		UPDATE payments SET status = 'refunded', refunded_at = NOW() WHERE order_id = $1
	`, event.OrderID); err != nil {
		return err
	}

	// Release inventory
	rows, err := tx.Query(ctx, `
		SELECT sku, quantity FROM inventory_reservations
		WHERE order_id = $1 AND status = 'reserved'
	`, event.OrderID)
	if err != nil {
		return err
	}
	type reservation struct{ SKU string; Qty int }
	var reservations []reservation
	for rows.Next() {
		var r reservation
		if err := rows.Scan(&r.SKU, &r.Qty); err != nil {
			return err
		}
		reservations = append(reservations, r)
	}
	rows.Close()

	for _, r := range reservations {
		if _, err := tx.Exec(ctx, `
			UPDATE inventory_stock
			SET available_qty = available_qty + $1, updated_at = NOW()
			WHERE sku = $2
		`, r.Qty, r.SKU); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE inventory_reservations SET status = 'released', released_at = NOW()
		WHERE order_id = $1 AND status = 'reserved'
	`, event.OrderID); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE sagas
		SET status = $1, completed_at = NOW(), updated_at = NOW(),
		    failure_reason = 'fulfillment failed'
		WHERE order_id = $2
	`, string(models.SagaStatusCompensated), event.OrderID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE orders SET status = 'failed', updated_at = NOW() WHERE id = $1
	`, event.OrderID); err != nil {
		return err
	}

	if err := o.appendOutboxInTx(ctx, tx, event, models.EventPaymentRefunded, map[string]any{
		"reason": "fulfillment failed",
	}); err != nil {
		return err
	}
	if err := o.appendOutboxInTx(ctx, tx, event, models.EventInventoryReleased, map[string]any{
		"released_skus": reservations,
	}); err != nil {
		return err
	}
	if err := o.appendOutboxInTx(ctx, tx, event, models.EventOrderFailed, map[string]any{
		"reason": "fulfillment failed — payment refunded, inventory released",
	}); err != nil {
		return err
	}

	o.log.Warn("compensated: fulfillment failed, payment refunded, inventory released",
		zap.String("order_id", event.OrderID.String()),
	)
	return tx.Commit(ctx)
}

// completeSaga marks the order and saga as successfully completed.
func (o *Orchestrator) completeSaga(ctx context.Context, event models.Event) error {
	tx, err := o.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err := tx.Exec(ctx, `
		UPDATE sagas
		SET status = $1, current_step = 'completed',
		    completed_at = NOW(), updated_at = NOW()
		WHERE order_id = $2
	`, string(models.SagaStatusCompleted), event.OrderID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE orders SET status = 'completed', updated_at = NOW() WHERE id = $1
	`, event.OrderID); err != nil {
		return err
	}

	if err := o.appendOutboxInTx(ctx, tx, event, models.EventOrderCompleted, map[string]any{
		"tracking_id": event.Payload["tracking_id"],
	}); err != nil {
		return err
	}

	o.log.Info("saga completed", zap.String("order_id", event.OrderID.String()))
	return tx.Commit(ctx)
}

// appendOutboxInTx writes an outbox row inside an existing transaction.
func (o *Orchestrator) appendOutboxInTx(ctx context.Context, tx pgx.Tx, event models.Event, eventType models.EventType, payload map[string]any) error {
	outboxEvent := models.Event{
		ID: uuid.New(), SagaID: event.SagaID, OrderID: event.OrderID,
		Type: eventType, OccurredAt: time.Now().UTC(),
		IdempotencyKey: fmt.Sprintf("%s-%s", string(eventType), event.OrderID),
		Payload:        payload,
	}
	data, _ := json.Marshal(outboxEvent)
	_, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (aggregate_id, aggregate_type, event_type, payload, idempotency_key)
		VALUES ($1, 'saga', $2, $3, $4)
		ON CONFLICT (idempotency_key) DO NOTHING
	`, event.OrderID.String(), string(eventType), data, outboxEvent.IdempotencyKey)
	return err
}

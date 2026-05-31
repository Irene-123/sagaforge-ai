package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"go.uber.org/zap"

	"github.com/kirtipurohit/sagaforge-ai/internal/models"
)

type Server struct {
	pool *pgxpool.Pool
	log  *zap.Logger
}

func NewServer(pool *pgxpool.Pool, log *zap.Logger) *Server {
	return &Server{pool: pool, log: log}
}

func (s *Server) Start(ctx context.Context, addr string) error {
	e := echo.New()
	e.HideBanner = true
	e.Use(middleware.RequestLoggerWithConfig(middleware.RequestLoggerConfig{
		LogMethod: true, LogURI: true, LogStatus: true,
		LogValuesFunc: func(c echo.Context, v middleware.RequestLoggerValues) error {
			s.log.Info("request",
				zap.String("method", v.Method),
				zap.String("uri", v.URI),
				zap.Int("status", v.Status),
			)
			return nil
		},
	}))

	e.POST("/orders", s.createOrder)
	e.GET("/orders/:id", s.getOrder)
	e.GET("/health", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	})

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = e.Shutdown(shutCtx)
	}()

	return e.Start(addr)
}

func (s *Server) createOrder(c echo.Context) error {
	var req models.CreateOrderRequest
	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	if len(req.Items) == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "items required")
	}

	orderID := uuid.New()
	sagaID := uuid.New()
	idempotencyKey := fmt.Sprintf("order-created-%s", orderID)

	// Calculate total
	var total float64
	for _, item := range req.Items {
		total += item.Price * float64(item.Quantity)
	}

	itemsJSON, _ := json.Marshal(req.Items)

	// Build the event payload upfront so we can store it in the outbox
	event := models.Event{
		ID:             uuid.New(),
		SagaID:         sagaID,
		OrderID:        orderID,
		Type:           models.EventOrderCreated,
		OccurredAt:     time.Now().UTC(),
		IdempotencyKey: idempotencyKey,
		Payload: map[string]any{
			"order_id":    orderID,
			"saga_id":     sagaID,
			"customer_id": req.CustomerID,
			"items":       req.Items,
			"total":       total,
		},
	}
	eventPayload, _ := json.Marshal(event)

	// --- Single transaction: write order + saga + outbox row ---
	tx, err := s.pool.Begin(c.Request().Context())
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(c.Request().Context()) //nolint:errcheck

	// 1. Insert order
	if _, err := tx.Exec(c.Request().Context(), `
		INSERT INTO orders (id, customer_id, status, total_amount, items)
		VALUES ($1, $2, 'pending', $3, $4)
	`, orderID, req.CustomerID, total, itemsJSON); err != nil {
		return fmt.Errorf("insert order: %w", err)
	}

	// 2. Insert saga
	if _, err := tx.Exec(c.Request().Context(), `
		INSERT INTO sagas (id, order_id, status, current_step)
		VALUES ($1, $2, 'started', 'reserve_inventory')
	`, sagaID, orderID); err != nil {
		return fmt.Errorf("insert saga: %w", err)
	}

	// 3. Insert outbox row (poller will publish to Kafka)
	if _, err := tx.Exec(c.Request().Context(), `
		INSERT INTO outbox_events (aggregate_id, aggregate_type, event_type, payload, idempotency_key)
		VALUES ($1, 'order', $2, $3, $4)
	`, orderID.String(), string(models.EventOrderCreated), eventPayload, idempotencyKey); err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}

	if err := tx.Commit(c.Request().Context()); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	s.log.Info("order created", zap.String("order_id", orderID.String()), zap.String("saga_id", sagaID.String()))

	return c.JSON(http.StatusCreated, map[string]any{
		"order_id": orderID,
		"saga_id":  sagaID,
		"status":   "pending",
		"total":    total,
	})
}

func (s *Server) getOrder(c echo.Context) error {
	orderID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid order id")
	}

	var id, customerID, status string
	var total float64
	var items json.RawMessage
	var createdAt time.Time

	err = s.pool.QueryRow(c.Request().Context(), `
		SELECT id, customer_id, status, total_amount, items, created_at
		FROM orders WHERE id = $1
	`, orderID).Scan(&id, &customerID, &status, &total, &items, &createdAt)
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, "order not found")
	}

	return c.JSON(http.StatusOK, map[string]any{
		"id":          id,
		"customer_id": customerID,
		"status":      status,
		"total":       total,
		"items":       items,
		"created_at":  createdAt,
	})
}

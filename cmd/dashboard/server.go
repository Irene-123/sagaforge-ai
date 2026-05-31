package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	kafkago "github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/kirtipurohit/sagaforge-ai/internal/kafka"
)

// Dashboard serves the HTMX UI and SSE event stream.
type Dashboard struct {
	pool    *pgxpool.Pool
	kafCfg  kafka.Config
	log     *zap.Logger

	// SSE fan-out: all connected browsers receive every new event
	mu      sync.Mutex
	clients map[chan string]struct{}
}

func NewDashboard(pool *pgxpool.Pool, kafCfg kafka.Config, log *zap.Logger) *Dashboard {
	return &Dashboard{
		pool:    pool,
		kafCfg:  kafCfg,
		log:     log,
		clients: make(map[chan string]struct{}),
	}
}

func (d *Dashboard) Start(ctx context.Context, addr string) error {
	e := echo.New()
	e.HideBanner = true
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{AllowOrigins: []string{"*"}}))
	e.Use(middleware.Recover())

	// Health check (used by Railway)
	e.GET("/health", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"status": "ok", "service": "sagaforge-ai"})
	})

	// Pages
	e.GET("/", d.indexPage)

	// API endpoints (consumed by HTMX partials)
	e.GET("/api/sagas", d.listSagas)
	e.GET("/api/sagas/:id", d.sagaDetail)
	e.GET("/api/events/:order_id", d.eventTimeline)
	e.GET("/api/insights/:order_id", d.aiInsights)
	e.GET("/api/stats", d.stats)

	// SSE stream
	e.GET("/api/stream", d.sseStream)

	// Trigger a test order from the dashboard
	e.POST("/api/simulate", d.simulateOrder)

	// Start Kafka consumer for live feed
	go d.consumeLiveFeed(ctx)

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = e.Shutdown(shutCtx)
	}()

	return e.Start(addr)
}

// ──────────────── SSE fan-out ────────────────

func (d *Dashboard) addClient(ch chan string) {
	d.mu.Lock()
	d.clients[ch] = struct{}{}
	d.mu.Unlock()
}

func (d *Dashboard) removeClient(ch chan string) {
	d.mu.Lock()
	delete(d.clients, ch)
	d.mu.Unlock()
}

func (d *Dashboard) broadcast(data string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for ch := range d.clients {
		select {
		case ch <- data:
		default:
			// slow consumer, drop
		}
	}
}

func (d *Dashboard) consumeLiveFeed(ctx context.Context) {
	// Subscribe to all event topics for the live dashboard feed
	topics := []string{
		kafka.TopicOrderEvents,
		kafka.TopicInventoryEvents,
		kafka.TopicPaymentEvents,
		kafka.TopicFulfillmentEvents,
		kafka.TopicSagaEvents,
		kafka.TopicAIInsightEvents,
	}
	for _, topic := range topics {
		t := topic
		consumer := kafka.NewConsumer(d.kafCfg, t, "dashboard-live", d.log)
		go func() {
			_ = consumer.Consume(ctx, func(_ context.Context, msg kafkago.Message) error {
				d.broadcast(string(msg.Value))
				return nil
			})
		}()
	}
}

// ──────────────── SSE endpoint ────────────────

func (d *Dashboard) sseStream(c echo.Context) error {
	c.Response().Header().Set("Content-Type", "text/event-stream")
	c.Response().Header().Set("Cache-Control", "no-cache")
	c.Response().Header().Set("Connection", "keep-alive")
	c.Response().WriteHeader(http.StatusOK)

	ch := make(chan string, 64)
	d.addClient(ch)
	defer d.removeClient(ch)

	flusher, ok := c.Response().Writer.(http.Flusher)
	if !ok {
		return echo.NewHTTPError(http.StatusInternalServerError, "streaming not supported")
	}

	ctx := c.Request().Context()
	for {
		select {
		case <-ctx.Done():
			return nil
		case data := <-ch:
			fmt.Fprintf(c.Response(), "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}

// ──────────────── API handlers ────────────────

func (d *Dashboard) listSagas(c echo.Context) error {
	rows, err := d.pool.Query(c.Request().Context(), `
		SELECT s.id, s.order_id, s.status, s.current_step, s.started_at, s.completed_at, s.failure_reason,
		       o.customer_id, o.total_amount
		FROM sagas s
		LEFT JOIN orders o ON o.id = s.order_id
		ORDER BY s.started_at DESC
		LIMIT 50
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var sagas []map[string]any
	for rows.Next() {
		var (
			id, orderID, status, step string
			startedAt                 time.Time
			completedAt               *time.Time
			failReason                *string
			customerID                *string
			total                     *float64
		)
		if err := rows.Scan(&id, &orderID, &status, &step, &startedAt, &completedAt, &failReason, &customerID, &total); err != nil {
			return err
		}
		s := map[string]any{
			"id": id, "order_id": orderID, "status": status,
			"current_step": step, "started_at": startedAt,
		}
		if completedAt != nil {
			s["completed_at"] = *completedAt
		}
		if failReason != nil {
			s["failure_reason"] = *failReason
		}
		if customerID != nil {
			s["customer_id"] = *customerID
		}
		if total != nil {
			s["total"] = *total
		}
		sagas = append(sagas, s)
	}
	return c.JSON(http.StatusOK, sagas)
}

func (d *Dashboard) sagaDetail(c echo.Context) error {
	sagaID := c.Param("id")
	var id, orderID, status, step string
	var startedAt time.Time
	var completedAt *time.Time
	var failReason *string

	err := d.pool.QueryRow(c.Request().Context(), `
		SELECT id, order_id, status, current_step, started_at, completed_at, failure_reason
		FROM sagas WHERE id = $1
	`, sagaID).Scan(&id, &orderID, &status, &step, &startedAt, &completedAt, &failReason)
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, "saga not found")
	}

	result := map[string]any{
		"id": id, "order_id": orderID, "status": status,
		"current_step": step, "started_at": startedAt,
	}
	if completedAt != nil {
		result["completed_at"] = *completedAt
	}
	if failReason != nil {
		result["failure_reason"] = *failReason
	}
	return c.JSON(http.StatusOK, result)
}

func (d *Dashboard) eventTimeline(c echo.Context) error {
	orderID := c.Param("order_id")
	rows, err := d.pool.Query(c.Request().Context(), `
		SELECT id, event_type, payload, created_at, published_at
		FROM outbox_events
		WHERE aggregate_id = $1
		ORDER BY created_at ASC
	`, orderID)
	if err != nil {
		return err
	}
	defer rows.Close()

	var events []map[string]any
	for rows.Next() {
		var id, etype string
		var payload json.RawMessage
		var createdAt time.Time
		var publishedAt *time.Time
		if err := rows.Scan(&id, &etype, &payload, &createdAt, &publishedAt); err != nil {
			return err
		}
		e := map[string]any{
			"id": id, "event_type": etype, "payload": payload,
			"created_at": createdAt,
		}
		if publishedAt != nil {
			e["published_at"] = *publishedAt
		}
		events = append(events, e)
	}
	return c.JSON(http.StatusOK, events)
}

func (d *Dashboard) aiInsights(c echo.Context) error {
	orderID := c.Param("order_id")
	rows, err := d.pool.Query(c.Request().Context(), `
		SELECT id, trigger_event, risk_score, explanation, suggestion, generated_at
		FROM ai_insights
		WHERE order_id = $1
		ORDER BY generated_at ASC
	`, orderID)
	if err != nil {
		return err
	}
	defer rows.Close()

	var insights []map[string]any
	for rows.Next() {
		var id, trigger, explanation, suggestion string
		var riskScore float64
		var generatedAt time.Time
		if err := rows.Scan(&id, &trigger, &riskScore, &explanation, &suggestion, &generatedAt); err != nil {
			return err
		}
		insights = append(insights, map[string]any{
			"id": id, "trigger_event": trigger, "risk_score": riskScore,
			"explanation": explanation, "suggestion": suggestion,
			"generated_at": generatedAt,
		})
	}
	return c.JSON(http.StatusOK, insights)
}

func (d *Dashboard) stats(c echo.Context) error {
	ctx := c.Request().Context()
	var total, completed, failed, compensated, inProgress int

	_ = d.pool.QueryRow(ctx, `SELECT COUNT(*) FROM sagas`).Scan(&total)
	_ = d.pool.QueryRow(ctx, `SELECT COUNT(*) FROM sagas WHERE status = 'completed'`).Scan(&completed)
	_ = d.pool.QueryRow(ctx, `SELECT COUNT(*) FROM sagas WHERE status = 'failed'`).Scan(&failed)
	_ = d.pool.QueryRow(ctx, `SELECT COUNT(*) FROM sagas WHERE status = 'compensated'`).Scan(&compensated)
	_ = d.pool.QueryRow(ctx, `SELECT COUNT(*) FROM sagas WHERE status IN ('started','in_progress','compensating')`).Scan(&inProgress)

	return c.JSON(http.StatusOK, map[string]any{
		"total": total, "completed": completed, "failed": failed,
		"compensated": compensated, "in_progress": inProgress,
	})
}

func (d *Dashboard) simulateOrder(c echo.Context) error {
	// POST to the order-service to create a test order.
	// In Railway deployment, both services run in the same container.
	orderPort := os.Getenv("ORDER_SERVICE_PORT")
	if orderPort == "" {
		orderPort = "8081"
	}
	orderSvcURL := fmt.Sprintf("http://localhost:%s/orders", orderPort)
	body := `{
		"customer_id": "cust-demo-001",
		"items": [
			{"sku": "SKU-WIDGET-A", "quantity": 2, "price": 29.99},
			{"sku": "SKU-GADGET-X", "quantity": 1, "price": 49.99}
		]
	}`
	resp, err := http.Post(orderSvcURL, "application/json", strings.NewReader(body))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadGateway, fmt.Sprintf("order-service unreachable: %v", err))
	}
	defer resp.Body.Close()

	var result map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&result)
	return c.JSON(resp.StatusCode, result)
}
